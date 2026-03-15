#!/usr/bin/env python3
"""
Patch the 'citation' global attribute into existing ICOS NetCDF files
without re-downloading or reprocessing any data.

For each station archive in the collection the script fetches the
citationString from the ICOS CP data object landing page (one HTTP
request per station) and writes it to every matching .nc file found
in the search directory.

Usage:
    python patch_citation.py 10.18160/R3G6-Z8ZH
    python patch_citation.py 10.18160/R3G6-Z8ZH --ncdir /data/icos_l2
    python patch_citation.py 10.18160/R3G6-Z8ZH --pattern "ICOSETC_*.nc"
    python patch_citation.py 10.18160/R3G6-Z8ZH --overwrite
"""

import argparse
import fnmatch
import re
import sys
from pathlib import Path

import netCDF4 as nc

sys.path.insert(0, str(Path(__file__).parent))
from icos_download_restructure import (
    _STATION_RE,
    get_archive_members,
    resolve_collection_url,
)
from fluxnet2nc import fetch_dobj_citation

# Matches station ID in NC filenames: ICOSETC_SE-Svb_INTERIM_restructured.nc
_NC_STATION_RE = re.compile(r"ICOSETC_([^_]+-[^_]+)_", re.IGNORECASE)


def patch_files(
    nc_files: list[Path],
    citation: str,
    overwrite: bool,
) -> tuple[int, int]:
    """Write ds.citation to each file in *nc_files*.

    Returns (patched, skipped) counts.
    """
    patched = skipped = 0
    for nc_path in nc_files:
        with nc.Dataset(nc_path, "a") as ds:
            if hasattr(ds, "citation") and not overwrite:
                print(f"    SKIP (already has citation): {nc_path.name}")
                skipped += 1
                continue
            ds.citation = citation
            patched += 1
            print(f"    Patched: {nc_path.name}")
    return patched, skipped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Patch 'citation' global attribute into existing ICOS NetCDF files.",
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
        help="Overwrite citation attribute if already present",
    )
    args = parser.parse_args()

    doi = args.doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    ncdir: Path = args.ncdir.resolve()
    station_filter = {s.upper() for s in args.station}

    # ── Build station-id → nc-file mapping from the directory ────────────────
    nc_files = [p for p in ncdir.iterdir()
                if p.is_file() and fnmatch.fnmatch(p.name, args.pattern)]
    if not nc_files:
        sys.exit(f"ERROR: no files matching '{args.pattern}' found in {ncdir}")

    # Map station ID → list of matching files
    station_files: dict[str, list[Path]] = {}
    for p in nc_files:
        m = _NC_STATION_RE.match(p.name)
        if m:
            station_files.setdefault(m.group(1).upper(), []).append(p)

    # ── Resolve DOI → collection → archive list ───────────────────────────────
    print(f"Resolving DOI {doi} …")
    try:
        collection_url = resolve_collection_url(doi)
    except Exception as exc:
        sys.exit(f"ERROR: could not resolve DOI: {exc}")
    print(f"Collection: {collection_url}")

    archives = get_archive_members(collection_url)
    print(f"Found {len(archives)} ARCHIVE file(s) in collection")

    if station_filter:
        archives = [
            a for a in archives
            if (m := _STATION_RE.match(a["name"])) and m.group(1).upper() in station_filter
        ]
        print(f"Filtered to {len(archives)} station(s)")

    # ── Patch each station ────────────────────────────────────────────────────
    total_patched = total_skipped = total_missing = 0

    for arch in archives:
        m = _STATION_RE.match(arch["name"])
        if not m:
            continue
        site_id = m.group(1)

        files_for_station = station_files.get(site_id.upper(), [])
        if not files_for_station:
            print(f"\n{site_id}: no matching .nc files found — skipping")
            total_missing += 1
            continue

        print(f"\n{site_id}: fetching citation …")
        _pid_url, citation = fetch_dobj_citation(arch["res"])
        if not citation:
            print(f"  WARNING: empty citationString for {arch['res']} — skipping")
            total_missing += 1
            continue

        print(f"  Citation: {citation[:80]}{'…' if len(citation) > 80 else ''}")
        patched, skipped = patch_files(files_for_station, citation, args.overwrite)
        total_patched += patched
        total_skipped += skipped

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"Done.  {total_patched} file(s) patched, "
          f"{total_skipped} skipped (already had citation), "
          f"{total_missing} station(s) with no match.")


if __name__ == "__main__":
    main()
