"""
obspack2zarr — populate a zarr v2 store with ICOS Obspack atmospheric
greenhouse-gas data (CO2, CH4, N2O, CO).

Subcommands
-----------
populate  DOI [--station …] [--gas …] [--store …] [--keep-nc]
list      [--store …]
info      STATION_ID [--store …]
remove    STATION_ID [--store …]

Each station group is named ``{TRIGRAM}{HEIGHT_MAGL}`` (e.g. ``HTM150``,
``CMN0``).  All four gases at the same station + height live in the same
group, with per-gas time dimensions (``time_co2``, ``time_ch4``, …) and
per-gas variable namespacing.
"""
from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import shutil
import stat
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import xarray as xr
import zarr

from icos_download_restructure import resolve_collection_url
from obspack_ingest import (
    GAS_SCALE,
    ObspackFileInfo,
    build_dataset,
    merge_attrs,
    parse_filename,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rmtree(path: Path) -> None:
    def _onexc(func, p, _):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass
    shutil.rmtree(path, onexc=_onexc)


def _prov_get(grp: zarr.Group) -> dict:
    raw = grp.attrs.get("_provenance", "{}")
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


def _prov_set(grp: zarr.Group, prov: dict) -> None:
    grp.attrs["_provenance"] = json.dumps(prov)


def _update_provenance(
    grp: zarr.Group,
    action: str,
    gas: str,
    file_name: str,
    pid_url: str,
) -> None:
    """Append an entry to the station group's _provenance history."""
    prov = _prov_get(grp)
    history = prov.get("history", [])
    history.append({
        "action":     action,
        "gas":        gas,
        "timestamp":  _now(),
        "file":       file_name,
        "source_doi": pid_url,
    })
    prov.update({
        "last_updated": _now(),
        "history":      history,
    })
    if "created" not in prov:
        prov["created"] = history[0]["timestamp"]
    _prov_set(grp, prov)


# ── Collection / download ─────────────────────────────────────────────────────

def _get_obspack_members(collection_url: str) -> list[dict]:
    """Return all .nc members of the collection — flat list."""
    req = urllib.request.Request(collection_url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    out: list[dict] = []
    for m in data.get("members", []):
        name = m.get("name", "")
        if not name.endswith(".nc"):
            continue
        out.append({
            "name":    name,
            "res":     m["res"],
            "hash_id": m["res"].rsplit("/", 1)[-1],
        })
    return out


def fetch_atc_station_metadata(trigram: str) -> dict:
    """
    Fetch ICOS atmosphere-station landing-page metadata for a 3-letter trigram.

    Returns a dict of fields suitable for inclusion in the zarr group .zattrs.
    Returns an empty dict on any error so the ingest can continue.
    """
    url = f"https://meta.icos-cp.eu/resources/stations/AS_{trigram.upper()}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            d = json.load(resp)
    except Exception:
        return {}

    out: dict = {}
    if (loc := d.get("location") or {}):
        out["station_lat"] = loc.get("lat")
        out["station_lon"] = loc.get("lon")
        out["station_alt"] = loc.get("alt")
    if (org := d.get("org") or {}):
        out["station_full_name"] = org.get("name", "")
    if (resp_org := d.get("responsibleOrganization") or {}):
        out["responsible_organization"] = resp_org.get("name", "")
        out["responsible_organization_uri"] = ((resp_org.get("self") or {}).get("uri") or "")
    out["country_code"] = d.get("countryCode", "")
    si = d.get("specificInfo") or {}
    out["station_class"]      = si.get("stationClass", "")
    out["station_labeling_date"] = si.get("labelingDate", "")
    out["wigos_id"]           = si.get("wigosId", "")
    out["time_zone_offset"]   = si.get("timeZoneOffset")
    out["station_landing_page"] = url

    # Current PI (most recent staff with role label "Principal Investigator")
    pis = []
    for s in d.get("staff", []):
        role = (s.get("role") or {}).get("role") or {}
        if role.get("label") == "Principal Investigator":
            person = s.get("person") or {}
            full = f"{person.get('firstName','')} {person.get('lastName','')}".strip()
            if full:
                pis.append(full)
    if pis:
        out["current_pi"] = "; ".join(pis)

    # Drop empty values for cleanliness
    return {k: v for k, v in out.items() if v not in (None, "", [])}


def _fetch_dobj_citation(res_url: str) -> tuple[str, str]:
    """Return (pid_url, citation) for a single Obspack data object."""
    req = urllib.request.Request(res_url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            d = json.load(resp)
        pid       = d.get("pid", "")
        pid_url   = f"https://hdl.handle.net/{pid}" if pid else ""
        citation  = (d.get("references") or {}).get("citationString", "")
        return pid_url, citation
    except Exception:
        return "", ""


def _download_nc(hash_id: str, dest: Path, label: str = "") -> None:
    """Stream-download an Obspack .nc via the licence-accept endpoint."""
    accept_url = f"https://data.icos-cp.eu/licence_accept?ids=%5B%22{hash_id}%22%5D"
    jar    = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    tag = f"[{label}] " if label else ""
    print(f"  {tag}Downloading …", end="", flush=True)
    with opener.open(urllib.request.Request(accept_url), timeout=600) as resp:
        with open(dest, "wb") as fh:
            shutil.copyfileobj(resp, fh, length=1 << 20)
    print(f"\r  {tag}Downloaded {dest.stat().st_size/1e6:.1f} MB → {dest.name}")


# ── Populate ──────────────────────────────────────────────────────────────────

def cmd_populate(args: argparse.Namespace) -> None:
    store_path = Path(args.store).resolve()
    outdir     = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Resolving DOI {args.doi} …")
    collection_url = resolve_collection_url(args.doi)
    print(f"Collection URL: {collection_url}")

    members = _get_obspack_members(collection_url)
    print(f"Collection has {len(members)} netCDF members")

    # Group members by station_id
    by_station: dict[str, list[dict]] = defaultdict(list)
    skipped_unparsed = 0
    for m in members:
        try:
            info = parse_filename(m["name"])
        except ValueError:
            skipped_unparsed += 1
            continue
        if args.gas and info.gas not in args.gas:
            continue
        sid = info.station_id
        if args.station and sid not in args.station:
            continue
        by_station[sid].append({**m, "info": info})

    if skipped_unparsed:
        print(f"  ({skipped_unparsed} files skipped — unparseable filename)")

    print(f"Will populate {len(by_station)} station group(s): {sorted(by_station)}")
    print()

    # Open / create store
    store_path.parent.mkdir(parents=True, exist_ok=True)
    g_root = zarr.open_group(str(store_path), mode="a")

    # In-memory accumulator for group attrs (worked around to_zarr mode='a' wiping)
    station_attrs: dict[str, dict] = {}
    for sid in by_station:
        if sid in g_root:
            station_attrs[sid] = dict(g_root[sid].attrs)

    # Fetch ATC landing-page metadata once per trigram (cached across heights)
    atc_cache: dict[str, dict] = {}
    for sid, files in by_station.items():
        trigram = files[0]["info"].trigram
        if trigram not in atc_cache:
            atc_cache[trigram] = fetch_atc_station_metadata(trigram)
            tag = "✓" if atc_cache[trigram] else "✗"
            print(f"  ATC metadata for {trigram.upper()}: {tag} ({len(atc_cache[trigram])} fields)")

    processed = 0
    failed    = 0

    for sid, files in sorted(by_station.items()):
        print(f"━━━ {sid} ({len(files)} file{'s' if len(files)!=1 else ''}) ━━━")
        for entry in files:
            info: ObspackFileInfo = entry["info"]
            label = f"{sid}/{info.gas}"
            nc_path = outdir / entry["name"]
            try:
                if not nc_path.exists():
                    _download_nc(entry["hash_id"], nc_path, label=label)
                else:
                    print(f"  [{label}] using cached {nc_path.name}")

                pid_url, citation = _fetch_dobj_citation(entry["res"])

                ds, _, attrs = build_dataset(nc_path)
                # Add per-gas DOI / citation to group attrs
                attrs[f"{info.gas}_source_doi"]      = pid_url
                attrs[f"{info.gas}_dobj_citation"]   = citation
                # Merge ATC landing-page enrichment (station-level, gas-agnostic)
                for k, v in atc_cache.get(info.trigram, {}).items():
                    attrs.setdefault(k, v)

                # Decide write mode
                station_existed_before = sid in station_attrs and station_attrs[sid]
                # Check whether the gas's variables already exist in the group
                if station_existed_before and f"time_{info.gas}" in g_root[sid]:
                    # Re-ingest of same gas — drop the old per-gas vars first
                    print(f"  [{label}] removing existing vars (re-ingest)")
                    for v in list(g_root[sid].array_keys()):
                        if v == info.gas or v.startswith(f"{info.gas}_") or v == f"time_{info.gas}":
                            del g_root[sid][v]

                mode = "w" if not station_existed_before else "a"
                ds.to_zarr(str(store_path), group=sid, mode=mode)

                # Re-apply accumulated attrs (to_zarr mode='a' wipes them)
                station_attrs[sid] = merge_attrs(station_attrs.get(sid, {}), attrs)
                grp = g_root[sid]
                for k, v in station_attrs[sid].items():
                    grp.attrs[k] = v

                action = "create" if mode == "w" else "update"
                _update_provenance(grp, action, info.gas, entry["name"], pid_url)
                # Refresh accumulator with provenance
                station_attrs[sid]["_provenance"] = grp.attrs["_provenance"]

                if not args.keep_nc:
                    nc_path.unlink(missing_ok=True)

                processed += 1
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"  [{label}] FAILED: {exc}")
                failed += 1

        # Per-group consolidation (mirrors fluxnet2zarr pattern)
        try:
            zarr.consolidate_metadata(str(store_path / sid))
        except Exception as exc:
            print(f"  [{sid}] consolidate failed: {exc}")

    # Store-root consolidation
    try:
        zarr.consolidate_metadata(str(store_path))
    except Exception as exc:
        print(f"store-root consolidate failed: {exc}")

    print()
    print(f"{'═'*60}")
    print(f"Done.  {processed} file(s) ingested, {failed} failed.")
    print(f"Store: {store_path}")


# ── List / info / remove ─────────────────────────────────────────────────────

def cmd_list(args: argparse.Namespace) -> None:
    store_path = Path(args.store).resolve()
    if not store_path.exists():
        sys.exit(f"ERROR: store {store_path} not found")
    g = zarr.open_group(str(store_path), mode="r")
    rows = []
    for sid in sorted(g.group_keys()):
        prov = _prov_get(g[sid])
        gases = sorted({h["gas"] for h in prov.get("history", []) if "gas" in h})
        rows.append((sid, prov.get("last_updated", "—"), ",".join(gases) or "—"))
    if not rows:
        print(f"(empty store: {store_path})")
        return
    w0 = max(len(r[0]) for r in rows)
    w1 = max(len(r[1]) for r in rows)
    print(f"{'station':<{w0}}  {'last_updated':<{w1}}  gases")
    print(f"{'-'*w0}  {'-'*w1}  -----")
    for sid, lu, g_str in rows:
        print(f"{sid:<{w0}}  {lu:<{w1}}  {g_str}")


def cmd_info(args: argparse.Namespace) -> None:
    store_path = Path(args.store).resolve()
    g = zarr.open_group(str(store_path), mode="r")
    if args.station_id not in g:
        sys.exit(f"ERROR: station {args.station_id!r} not found in {store_path}")
    grp = g[args.station_id]
    prov = _prov_get(grp)

    print(f"Station: {args.station_id}")
    print(f"Path   : {store_path / args.station_id}")
    print()
    print("── Site metadata ─────────────────────────────────────────────")
    for k in ("site_code", "site_name", "station_full_name", "site_country",
              "country_code", "site_latitude", "site_longitude",
              "site_elevation", "intake_height", "altitude",
              "station_class", "station_labeling_date", "wigos_id",
              "time_zone_offset", "responsible_organization", "current_pi",
              "site_url", "station_landing_page"):
        if k in grp.attrs:
            v = grp.attrs[k]
            if isinstance(v, str) and len(v) > 70:
                v = v[:67] + "..."
            print(f"  {k:<25s} {v}")
    print()
    print("── Gases ─────────────────────────────────────────────────────")
    for gas in GAS_SCALE:
        if gas not in grp:
            continue
        print(f"  {gas.upper()}:")
        for k in (f"{gas}_calibration_scale", f"{gas}_dataset_name",
                  f"{gas}_data_frequency", f"{gas}_start_date",
                  f"{gas}_stop_date", f"{gas}_source_doi"):
            if k in grp.attrs:
                print(f"     {k:<35s} {grp.attrs[k]}")
        print(f"     samples                              "
              f"{grp[f'time_{gas}'].shape[0]}")
    print()
    print("── History ───────────────────────────────────────────────────")
    for h in prov.get("history", []):
        print(f"  {h.get('timestamp','?')}  {h.get('action','?'):<6s}  "
              f"{h.get('gas','?'):<4s}  {h.get('file','?')}")


def cmd_remove(args: argparse.Namespace) -> None:
    store_path = Path(args.store).resolve()
    g = zarr.open_group(str(store_path), mode="a")
    if args.station_id not in g:
        sys.exit(f"ERROR: station {args.station_id!r} not found")
    target = store_path / args.station_id
    if target.is_dir():
        _rmtree(target)
        print(f"Removed {args.station_id}")
        try:
            zarr.consolidate_metadata(str(store_path))
        except Exception as exc:
            print(f"consolidate failed: {exc}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        prog="obspack2zarr",
        description="Populate a zarr v2 store with ICOS Obspack data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = p.add_subparsers(dest="command")

    pop = sub.add_parser("populate", help="Download an Obspack collection and write to zarr",
                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    pop.add_argument("doi", help="Collection DOI (e.g. 10.18160/1PZ9-SDJ2)")
    pop.add_argument("--store",   default="icos-obspack.zarr",
                     help="Zarr store directory")
    pop.add_argument("--outdir",  default=".obspack_cache",
                     help="Directory for downloaded .nc files")
    pop.add_argument("--station", nargs="+", default=[],
                     help="Limit to specific station IDs (e.g. HTM150 CBW207)")
    pop.add_argument("--gas",     nargs="+", choices=list(GAS_SCALE),
                     default=[], help="Limit to specific gases")
    pop.add_argument("--keep-nc", action="store_true",
                     help="Keep downloaded .nc files after ingest")
    pop.set_defaults(func=cmd_populate)

    lst = sub.add_parser("list", help="List stations in the store",
                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    lst.add_argument("--store", default="icos-obspack.zarr")
    lst.set_defaults(func=cmd_list)

    inf = sub.add_parser("info", help="Show info for one station",
                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    inf.add_argument("station_id")
    inf.add_argument("--store", default="icos-obspack.zarr")
    inf.set_defaults(func=cmd_info)

    rm = sub.add_parser("remove", help="Remove one station group",
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    rm.add_argument("station_id")
    rm.add_argument("--store", default="icos-obspack.zarr")
    rm.set_defaults(func=cmd_remove)

    args = p.parse_args()
    if not args.command:
        p.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
