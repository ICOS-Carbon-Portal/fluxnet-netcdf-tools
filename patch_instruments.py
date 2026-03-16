#!/usr/bin/env python3
"""
Patch the 'instrument_deployments' variable attribute into METEOSENS variables
in existing ICOS NetCDF files without re-downloading or reprocessing data.

For each station the script fetches column-level instrument deployment metadata
from the ICOS CP METEOSENS data object landing page (one HTTP request per station)
and writes a JSON string attribute to every METEOSENS-derived variable in the
matching .nc file.

Usage:
    python patch_instruments.py 10.18160/R3G6-Z8ZH
    python patch_instruments.py 10.18160/R3G6-Z8ZH --ncdir /data/icos_l2
    python patch_instruments.py 10.18160/R3G6-Z8ZH --pattern "ICOSETC_*.nc"
    python patch_instruments.py 10.18160/R3G6-Z8ZH --overwrite
"""

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path

import netCDF4 as nc

sys.path.insert(0, str(Path(__file__).parent))
from icos_download_restructure import (
    _STATION_RE,
    get_collection_members,
    resolve_collection_url,
)
from fluxnet2nc import fetch_column_instruments

# Matches station ID in NC filenames: ICOSETC_SE-Svb_INTERIM_restructured.nc
_NC_STATION_RE = re.compile(r"ICOSETC_([^_]+-[^_]+)_", re.IGNORECASE)

# METEOSENS triple-index variable pattern: VARBASE_R_H_V (main or ancillary)
_PROFILE_VAR_RE = re.compile(r"^([A-Z][A-Z0-9_]*)_[0-9]+$")


def _iter_profile_vars(ds: nc.Dataset):
    """Yield (variable_name, variable) for METEOSENS-style 4-D variables in root."""
    for name, var in ds.variables.items():
        if len(var.dimensions) == 4 and var.dimensions[0] == "time":
            yield name, var


def patch_nc_file(
    nc_path: Path,
    column_instruments: dict[str, list[dict]],
    overwrite: bool,
) -> tuple[int, int]:
    """Write instrument_deployments to eligible variables in *nc_path*.

    Returns (patched_vars, skipped_vars) counts.
    """
    patched = skipped = 0

    # Build a reverse map: nc_variable_name → list of deployments with r/h/v
    # The NC variable is named after the FLUXNET base (e.g. "TA"), and we need
    # to find which CSV columns (e.g. "TA_1_1_1") map to which r/h/v positions.
    # We do this by scanning column_instruments keys for a matching base prefix.

    with nc.Dataset(nc_path, "a") as ds:
        for nc_name, var in _iter_profile_vars(ds):
            if hasattr(var, "instrument_deployments") and not overwrite:
                skipped += 1
                continue

            # Gather deployments for all columns whose base matches nc_name
            all_deps = []
            for col_label, deps in column_instruments.items():
                # Parse VARBASE_R_H_V from column label
                m = re.match(
                    r"^" + re.escape(nc_name) + r"_([0-9]+)_([0-9]+)_([0-9]+)$",
                    col_label,
                    re.IGNORECASE,
                )
                if not m:
                    continue
                r, h, v = int(m.group(1)), int(m.group(2)), int(m.group(3))
                for dep in deps:
                    all_deps.append({"r": r, "h": h, "v": v, **dep})

            if not all_deps:
                continue

            var.instrument_deployments = json.dumps(all_deps, separators=(",", ":"))
            patched += 1

    return patched, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Patch 'instrument_deployments' variable attribute into "
            "METEOSENS variables in existing ICOS NetCDF files."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "doi",
        help="Collection DOI (e.g. 10.18160/R3G6-Z8ZH)",
    )
    parser.add_argument(
        "--ncdir", default=".", type=Path, metavar="DIR",
        help="Directory to search for .nc files",
    )
    parser.add_argument(
        "--pattern", default="*_restructured.nc", metavar="GLOB",
        help="Filename glob pattern to match within --ncdir",
    )
    parser.add_argument(
        "--station", nargs="+", default=[], metavar="ID",
        help="Limit to specific station IDs (default: all in collection)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite instrument_deployments attribute if already present",
    )
    args = parser.parse_args()

    doi = args.doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    ncdir: Path = args.ncdir.resolve()
    station_filter = {s.upper() for s in args.station}

    # ── Build station-id → nc-file mapping ───────────────────────────────────
    nc_files = [p for p in ncdir.iterdir()
                if p.is_file() and fnmatch.fnmatch(p.name, args.pattern)]
    if not nc_files:
        sys.exit(f"ERROR: no files matching '{args.pattern}' found in {ncdir}")

    station_files: dict[str, Path] = {}
    for p in nc_files:
        m = _NC_STATION_RE.match(p.name)
        if m:
            station_files[m.group(1).upper()] = p

    # ── Resolve DOI → collection ──────────────────────────────────────────────
    print(f"Resolving DOI {doi} …")
    try:
        collection_url = resolve_collection_url(doi)
    except Exception as exc:
        sys.exit(f"ERROR: could not resolve DOI: {exc}")
    print(f"Collection: {collection_url}")

    _archives, meteosens_by_site = get_collection_members(collection_url)
    print(f"Found {len(meteosens_by_site)} METEOSENS object(s) in collection")

    if station_filter:
        meteosens_by_site = {
            k: v for k, v in meteosens_by_site.items()
            if k.upper() in station_filter
        }
        print(f"Filtered to {len(meteosens_by_site)} station(s)")

    # ── Patch each station ────────────────────────────────────────────────────
    total_patched = total_skipped = total_missing = 0

    for site_id, meteosens_res in meteosens_by_site.items():
        nc_path = station_files.get(site_id.upper())
        if nc_path is None:
            print(f"\n{site_id}: no matching .nc file found — skipping")
            total_missing += 1
            continue

        print(f"\n{site_id}: fetching instrument deployment metadata …")
        col_instruments = fetch_column_instruments(meteosens_res)
        if not col_instruments:
            print(f"  WARNING: no instrument deployments found for {meteosens_res}")
            total_missing += 1
            continue

        print(f"  {len(col_instruments)} column(s) with deployment info")
        patched, skipped = patch_nc_file(nc_path, col_instruments, args.overwrite)
        print(f"  {nc_path.name}: {patched} variable(s) patched, "
              f"{skipped} already had attribute")
        total_patched += patched
        total_skipped += skipped

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"Done.  {total_patched} variable(s) patched, "
          f"{total_skipped} skipped (already had attribute), "
          f"{total_missing} station(s) with no match.")


if __name__ == "__main__":
    main()
