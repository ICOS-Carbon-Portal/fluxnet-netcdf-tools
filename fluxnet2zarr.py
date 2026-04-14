#!/usr/bin/env python3
"""
Populate, update, or manage an ICOS/FLUXNET zarr v2 store.

The ingest pipeline is identical to icos_download_restructure.py — DOI →
download ARCHIVE zip → extract CSVs → process — but writes directly to a
zarr store, creating no permanent intermediate .nc files.

Store layout
------------
icos-fluxnet.zarr/
  SE-Svb/                ← root zarr group: merged HH data
    .zgroup
    .zattrs              ← CF global attrs + _provenance JSON
    time/                ← zarr arrays
    NEE/
    …
    fluxnet_dd/          ← aggregated sub-groups
    fluxnet_mm/
    fluxnet_ww/
    fluxnet_yy/
  DE-Hai/
    …

Usage
-----
    python fluxnet2zarr.py 10.18160/R3G6-Z8ZH
    python fluxnet2zarr.py 10.18160/R3G6-Z8ZH --station SE-Svb DE-Hai
    python fluxnet2zarr.py remove SE-Svb
    python fluxnet2zarr.py list
    python fluxnet2zarr.py info  SE-Svb

Dependencies
------------
    pip install numpy pandas netCDF4 zarr xarray
"""

import argparse
import json
import re
import shutil
import sys
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

try:
    import zarr
except ModuleNotFoundError:
    import subprocess
    print("zarr not found — installing …")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "zarr<3"])
    import zarr  # type: ignore[no-redef]

sys.path.insert(0, str(Path(__file__).parent))
from fluxnet2nc import fetch_column_instruments, fetch_dobj_citation
from fluxnet_restructure import restructure_to_zarr
from icos_download_restructure import (
    _STATION_RE,
    download_zip,
    extract_needed_csvs,
    get_collection_members,
    resolve_collection_url,
)

_NC_STATION_RE = re.compile(r"ICOSETC_([^_]+-[^_]+)_", re.IGNORECASE)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _prov_get(grp: zarr.Group) -> dict:
    raw = grp.attrs.get("_provenance", "{}")
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


def _prov_set(grp: zarr.Group, prov: dict) -> None:
    grp.attrs["_provenance"] = json.dumps(prov)


def _update_provenance(
    store: zarr.Group,
    site_id: str,
    action: str,
    arch_name: str,
    pid_url: str,
    dobj_citation: str,
) -> None:
    """Add a provenance entry to the station zarr group's .zattrs."""
    station_grp = store.require_group(site_id)
    prov = _prov_get(station_grp)
    history = prov.get("history", [])
    history.append({
        "action":      action,
        "timestamp":   _now(),
        "archive":     arch_name,
        "source_doi":  pid_url,
    })
    prov.update({
        "site_id":      site_id,
        "last_updated": _now(),
        "archive":      arch_name,
        "source_doi":   pid_url,
        "citation":     dobj_citation,
        "history":      history,
    })
    if "created" not in prov:
        prov["created"] = history[0]["timestamp"]
    _prov_set(station_grp, prov)


# ── Sub-command implementations ───────────────────────────────────────────────

def cmd_populate(args: argparse.Namespace) -> None:
    """Resolve DOI → download → extract → write to zarr for each station."""
    doi = args.doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")

    print(f"Resolving DOI {doi} …")
    collection_url = resolve_collection_url(doi)
    print(f"  Collection URL: {collection_url}")

    archives, meteosens_by_site = get_collection_members(collection_url)
    print(f"  Found {len(archives)} station archive(s), "
          f"{len(meteosens_by_site)} METEOSENS object(s)")

    filter_set = {s.upper() for s in args.station} if args.station else set()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    store_path = str(Path(args.store).resolve())

    store = zarr.open_group(store_path, mode="a")

    processed = skipped = 0
    for arch in archives:
        m = _STATION_RE.search(arch["name"])
        if not m:
            print(f"  WARNING: cannot parse site ID from {arch['name']!r} — skipping",
                  file=sys.stderr)
            continue
        site_id = m.group(1)

        if filter_set and site_id.upper() not in filter_set:
            continue

        action = "update" if site_id in store else "create"
        print(f"\n{'─'*60}")
        print(f"{site_id}  [{action}]")

        # ── Fetch per-archive citation ────────────────────────────────────────
        pid_url, dobj_citation = fetch_dobj_citation(arch["res"])

        # ── Download zip ──────────────────────────────────────────────────────
        zip_path = outdir / arch["name"].replace(" ", "_")
        if not zip_path.suffix:
            zip_path = zip_path.with_suffix(".zip")
        if zip_path.exists() and zip_path.stat().st_size > 0:
            print(f"  Using cached {zip_path.name}")
        else:
            download_zip(arch["hash_id"], zip_path, label=site_id)

        # ── Extract CSVs ──────────────────────────────────────────────────────
        csv_paths = extract_needed_csvs(zip_path, outdir)
        if not csv_paths:
            print(f"  WARNING: no matching CSV files in {zip_path.name} — skipping",
                  file=sys.stderr)
            if not args.keep_zip:
                zip_path.unlink(missing_ok=True)
            skipped += 1
            continue

        # ── Fetch instrument metadata ─────────────────────────────────────────
        meteosens_res   = meteosens_by_site.get(site_id, "")
        col_instruments: dict = {}
        if meteosens_res:
            print(f"  Fetching instrument deployment metadata …")
            col_instruments = fetch_column_instruments(meteosens_res)
            print(f"  {len(col_instruments)} column(s) with instrument info")

        # ── Delete existing station group (clean slate for update) ────────────
        if site_id in store:
            store_dir = Path(store_path) / site_id
            if store_dir.is_dir():
                shutil.rmtree(store_dir)
            # Re-open after rmtree so zarr's in-memory state is consistent
            store = zarr.open_group(store_path, mode="a")

        # ── Restructure directly to zarr ──────────────────────────────────────
        restructure_args = Namespace(
            site_id            = site_id,
            comment            = args.comment,
            doi                = "",
            doi_url            = pid_url,
            doi_citation       = "",
            dobj_citation      = dobj_citation,
            column_instruments = col_instruments,
        )
        restructure_to_zarr(csv_paths, store_path, site_id, restructure_args)

        # ── Record provenance ─────────────────────────────────────────────────
        store = zarr.open_group(store_path, mode="a")   # refresh after writes
        _update_provenance(store, site_id, action, arch["name"], pid_url, dobj_citation)
        zarr.consolidate_metadata(store_path)            # keep .zmetadata in sync

        # ── Cleanup ───────────────────────────────────────────────────────────
        if not args.keep_csv:
            for p in csv_paths:
                p.unlink(missing_ok=True)
        if not args.keep_zip:
            zip_path.unlink(missing_ok=True)

        processed += 1

    print(f"\n{'═'*60}")
    print(f"Done.  {processed} station(s) written, {skipped} skipped.")
    print(f"Store: {store_path}")


def cmd_remove(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    site_id    = args.site_id
    store      = zarr.open_group(store_path, mode="a")

    if site_id not in store:
        sys.exit(f"ERROR: station {site_id!r} not found in {store_path}")

    store_dir = Path(store_path) / site_id
    if store_dir.is_dir():
        shutil.rmtree(store_dir)
        print(f"Removed {site_id} from {store_path}")
        zarr.consolidate_metadata(store_path)
    else:
        sys.exit(f"ERROR: expected directory {store_dir} not found")


def cmd_list(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    if not Path(store_path).exists():
        print(f"Store {store_path} does not exist.")
        return

    store    = zarr.open_group(store_path, mode="r")
    stations = sorted(store.group_keys())
    if not stations:
        print("Store is empty.")
        return

    print(f"\n{'Station':<12}  {'Last updated':<22}  {'Archive'}")
    print("─" * 72)
    for sid in stations:
        grp  = store[sid]
        prov = _prov_get(grp)
        print(f"{sid:<12}  {prov.get('last_updated', '?'):<22}  {prov.get('archive', '?')}")
    print(f"\n{len(stations)} station(s) in {store_path}")


def cmd_info(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    site_id    = args.site_id
    store      = zarr.open_group(store_path, mode="r")

    if site_id not in store:
        sys.exit(f"ERROR: station {site_id!r} not found in {store_path}")

    grp  = store[site_id]
    prov = _prov_get(grp)

    citation = prov.get("citation", "")
    cit_short = citation[:100] + "…" if len(citation) > 100 else citation

    print(f"\nStation:      {site_id}")
    print(f"Created:      {prov.get('created', '?')}")
    print(f"Last updated: {prov.get('last_updated', '?')}")
    print(f"Archive:      {prov.get('archive', '?')}")
    print(f"Source DOI:   {prov.get('source_doi', '?')}")
    print(f"Citation:     {cit_short}")
    print(f"\nGroups:       {', '.join(sorted(grp.group_keys())) or '(none)'}")
    print(f"Root arrays:  {len(list(grp.array_keys()))}")
    print(f"\nHistory:")
    for h in prov.get("history", []):
        print(f"  {h.get('timestamp','?')}  {h.get('action','?'):8s}  "
              f"{h.get('archive','')}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Populate or manage an ICOS/FLUXNET zarr v2 store. "
            "Pass a DOI as the first argument to download and ingest stations, "
            "or use a sub-command (remove / list / info) to manage the store."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--store", default="icos-fluxnet.zarr", metavar="DIR",
        help="Zarr store directory",
    )

    sub = parser.add_subparsers(dest="command")

    # ── populate (default when first arg looks like a DOI) ────────────────────
    p_pop = sub.add_parser(
        "populate",
        help="Download collection from DOI and ingest stations into the store",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_pop.add_argument("doi", help="ICOS collection DOI (e.g. 10.18160/R3G6-Z8ZH)")
    p_pop.add_argument("--station", nargs="+", default=[], metavar="ID",
                       help="Process only these station IDs; default: all")
    p_pop.add_argument("--outdir", default=".", type=Path, metavar="DIR",
                       help="Directory for temporary downloads and CSVs")
    p_pop.add_argument("--keep-zip", action="store_true",
                       help="Keep downloaded zip after extraction")
    p_pop.add_argument("--keep-csv", action="store_true",
                       help="Keep extracted CSV files after ingestion")
    p_pop.add_argument("--comment", default="",
                       help="Free-text comment added as a global attribute")

    # ── remove ────────────────────────────────────────────────────────────────
    p_rem = sub.add_parser("remove", help="Remove a station from the store")
    p_rem.add_argument("site_id", help="Station ID, e.g. SE-Svb")

    # ── list ──────────────────────────────────────────────────────────────────
    sub.add_parser("list", help="List all stations in the store",
                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # ── info ──────────────────────────────────────────────────────────────────
    p_inf = sub.add_parser("info", help="Show provenance for a station")
    p_inf.add_argument("site_id", help="Station ID, e.g. SE-Svb")

    # ── Smart default: treat first arg as DOI if no sub-command given ─────────
    argv = sys.argv[1:]
    if argv and argv[0] not in ("populate", "remove", "list", "info", "--help", "-h"):
        argv = ["populate"] + argv

    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "populate": cmd_populate,
        "remove":   cmd_remove,
        "list":     cmd_list,
        "info":     cmd_info,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
