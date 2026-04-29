"""
One-shot fix: re-attach station metadata (geospatial_lat/_lon, station_elevation,
country, ecosystem, climate_zone, …) to every station group + sub-group in
icos-fluxnet.zarr.

Bug background: xr.Dataset.to_zarr(mode="a") wipes the group .zattrs on every
call, so multi-flush writes in restructure_to_zarr lost everything except
_provenance (which was set later by a separate grp.attrs[...] = ... call).

This script does NOT touch any data — only the .zattrs JSON files at each
group level. Runtime: ~1 HTTP request per station, ~30s total for 35 stations.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import zarr

from fluxnet2nc import fetch_icos_station_meta


SUBGROUPS = ("fluxnet_dd", "fluxnet_mm", "fluxnet_ww", "fluxnet_yy", "meteosens")


def build_global_attrs(site_id: str, station_meta: dict, prov: dict) -> dict:
    """Re-build the same dict restructure_to_zarr would have written."""
    attrs: dict = {
        "Conventions": "CF-1.12",
        "title":       f"ICOS ETC L2 restructured data — site {site_id}",
        "institution": "ICOS Carbon Portal / FLUXNET",
        "site_id":     site_id,
        "featureType": "timeSeries",
        "references": (
            "Pastorello et al. (2020) The FLUXNET2015 dataset and the ONEFlux "
            "processing pipeline for eddy covariance data. "
            "Scientific Data 7:225. https://doi.org/10.1038/s41597-020-0534-3"
        ),
    }
    attrs.update({k: str(v) for k, v in station_meta.items()})

    # Pull DOI / citation back from the provenance entry (not the original
    # CLI args, which we don't have here).
    if prov.get("source_doi"):
        attrs["source_doi"] = prov["source_doi"]
    if prov.get("citation"):
        attrs["citation"] = prov["citation"]
    return attrs


def patch_station(store: zarr.Group, site_id: str, *, dry: bool) -> bool:
    """Patch one station's group + sub-groups. Returns True on success."""
    station = store[site_id]
    prov = json.loads(station.attrs.get("_provenance", "{}"))

    print(f"  [{site_id}] fetching ICOS station metadata …", end=" ", flush=True)
    try:
        station_meta = fetch_icos_station_meta(site_id)
    except Exception as exc:
        print(f"FAILED: {exc}")
        return False
    print(f"{len(station_meta)} fields")

    new_attrs = build_global_attrs(site_id, station_meta, prov)
    new_attrs["history"] = (
        f"{prov.get('history',[{}])[0].get('timestamp','')}. "
        f"Patched .zattrs on {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
        "by patch_fluxnet_zarr_attrs.py"
    )

    # Apply to root station group + every existing sub-group
    targets = [station]
    for sub in SUBGROUPS:
        if sub in station:
            targets.append(station[sub])

    for grp in targets:
        for k, v in new_attrs.items():
            if dry:
                continue
            grp.attrs[k] = v

    print(f"     wrote {len(new_attrs)} attrs to {len(targets)} group(s)"
          f"{' (DRY)' if dry else ''}")
    return True


def main() -> None:
    p = argparse.ArgumentParser(
        description="Re-attach station metadata attrs to fluxnet zarr store"
    )
    p.add_argument("--store",   default="icos-fluxnet.zarr",
                   help="Zarr store path")
    p.add_argument("--station", nargs="+", default=[],
                   help="Limit to specific station IDs (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch metadata and report, but don't write")
    args = p.parse_args()

    store_path = Path(args.store).resolve()
    if not store_path.exists():
        sys.exit(f"ERROR: store {store_path} not found")

    store = zarr.open_group(str(store_path), mode="a")
    sids = sorted(store.group_keys())
    if args.station:
        sids = [s for s in sids if s in set(args.station)]
        if not sids:
            sys.exit(f"ERROR: none of {args.station} present in store")

    print(f"Patching {len(sids)} station(s) in {store_path}")
    print()

    ok = 0
    for sid in sids:
        if patch_station(store, sid, dry=args.dry_run):
            ok += 1

    print()
    if not args.dry_run and ok:
        print("Re-consolidating metadata (this is the slow step) …", flush=True)
        for i, sid in enumerate(sids, 1):
            print(f"  [{i}/{len(sids)}] {sid}", flush=True)
            zarr.consolidate_metadata(str(store_path / sid))
            for sub in SUBGROUPS:
                sub_path = store_path / sid / sub
                if sub_path.is_dir() and (sub_path / ".zgroup").exists():
                    zarr.consolidate_metadata(str(sub_path))
        print("  store-root", flush=True)
        zarr.consolidate_metadata(str(store_path))

    print(f"Done. {ok}/{len(sids)} stations patched"
          f"{' (DRY-RUN — no files written)' if args.dry_run else ''}.")


if __name__ == "__main__":
    main()
