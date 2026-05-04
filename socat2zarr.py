#!/usr/bin/env python3
"""
Populate, update, or manage an ICOS Ocean (SOCAT) zarr v2 store.

Pulls the 116 CSV products from a SOCAT-style ICOS collection (DOI
10.18160/FS17-JBFB at time of writing), one CSV per deployment/cruise leg,
and writes them as one zarr group per file into ``icos-socat.zarr``.

Each CSV's filename stem is used as the cruise/deployment ID — that ID is
also the zarr group name.  All numeric columns are ingested with their
original ICOS labels, units, and quantity-kind preserved as variable
attrs.  QC flag columns are kept alongside the data.

Store layout
------------
icos-socat.zarr/
  119920250401/                    ← Thornton Buoy (fixed)
    .zgroup
    .zattrs   {station_id, platform_name, fixed:true, lat, lon, interval,
               source_doi, citation, history:[{action,timestamp,…}], …}
    time/  Temp/  P_sal/  pCO2/  fCO2/  xCO2/  Temp_QC/  …
  11SS20240501/                    ← BE-SOOP-Simon Stevin leg
    .zgroup
    .zattrs   {…, fixed:false, …}
    time/  lon/  lat/  Temp/  P_sal/  pCO2/  fCO2/  xCO2/  pCO2_QC/  …

Usage
-----
    python socat2zarr.py 10.18160/FS17-JBFB
    python socat2zarr.py 10.18160/FS17-JBFB --platform 11SS
    python socat2zarr.py 10.18160/FS17-JBFB --member 11SS20240501.csv
    python socat2zarr.py list
    python socat2zarr.py info 11SS20240501
    python socat2zarr.py remove 11SS20240501

Dependencies
------------
    pip install numpy pandas zarr<3
"""

import argparse
import http.cookiejar
import io
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import zarr
except ModuleNotFoundError:
    import subprocess
    print("zarr not found — installing …")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "zarr<3"])
    import zarr  # type: ignore[no-redef]

sys.path.insert(0, str(Path(__file__).parent))
from icos_download_restructure import resolve_collection_url


# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_DOI   = "10.18160/FS17-JBFB"
DEFAULT_STORE = "icos-socat.zarr"

# Map ICOS column labels → short, lowercase names used as zarr array names.
# Variables not in this map are kept under their ICOS label sanitised to a
# valid Python identifier.  QC flag columns are auto-renamed to "<base>_QC".
_KNOWN_COLUMN_NAMES = {
    "TIMESTAMP":                          "time",
    "Longitude":                          "lon",
    "Latitude":                           "lat",
    "Temp [degC]":                        "Temp",
    "P_sal [psu]":                        "P_sal",
    "xCO2 in atmosphere [umol mol-1]":    "xCO2_atm",
    "Atmospheric Pressure [hPa]":         "atm_pressure",
    "pCO2 [uatm]":                        "pCO2",
    "fCO2 [uatm]":                        "fCO2",
    "pCO2 in atmosphere [uatm]":          "pCO2_atm",
    "fCO2 in atmosphere [uatm]":          "fCO2_atm",
}

_QC_SUFFIX_RE = re.compile(r"\s*QC Flag$", re.IGNORECASE)
_UNIT_BRACKET_RE = re.compile(r"\[([^\]]+)\]")


def _unit_from_label(label: str) -> str | None:
    """Extract a unit from a column label like 'Temp [degC]' or 'pCO2 [uatm]'."""
    m = _UNIT_BRACKET_RE.search(label)
    return m.group(1).strip() if m else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sanitise_name(label: str) -> str:
    """Convert an arbitrary column label to a safe zarr array name."""
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", label).strip("_")
    return s or "var"


def _column_to_zarr_name(label: str, used: set[str]) -> tuple[str, bool]:
    """
    Map an ICOS column label to a zarr array name.

    Returns (name, is_qc_flag).  QC flag columns are renamed to "<base>_QC"
    where base is the zarr name of the column they qualify (the column
    immediately preceding them, by the SOCAT product convention).
    """
    qc_match = _QC_SUFFIX_RE.search(label)
    if qc_match:
        base_label = label[: qc_match.start()].strip()
        base_name = _KNOWN_COLUMN_NAMES.get(base_label) or _sanitise_name(base_label)
        # If two QC columns refer to the same base (e.g. duplicate xCO2 in some
        # files), disambiguate the second occurrence.
        candidate = f"{base_name}_QC"
        if candidate in used:
            i = 2
            while f"{candidate}_{i}" in used:
                i += 1
            candidate = f"{candidate}_{i}"
        return candidate, True

    if label in _KNOWN_COLUMN_NAMES:
        return _KNOWN_COLUMN_NAMES[label], False

    # Unknown column — sanitise the label and disambiguate against `used`.
    name = _sanitise_name(label)
    candidate = name
    i = 2
    while candidate in used:
        candidate = f"{name}_{i}"
        i += 1
    return candidate, False


# ── ICOS CP API ───────────────────────────────────────────────────────────────

def get_collection_csv_members(collection_url: str) -> list[dict[str, Any]]:
    """Return all CSV members of the collection in collection order."""
    req = urllib.request.Request(
        collection_url, headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    out = []
    for m in data.get("members", []):
        name = m.get("name", "")
        if name.lower().endswith(".csv"):
            out.append({
                "name":    name,
                "res":     m["res"],
                "hash_id": m["res"].rsplit("/", 1)[-1],
            })
    return out


def fetch_object_metadata(res_url: str) -> dict[str, Any]:
    """Full JSON metadata for an ICOS object."""
    req = urllib.request.Request(res_url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def extract_citation(object_meta: dict[str, Any]) -> str:
    """Pull the formatted citation string from object JSON, if present."""
    refs = object_meta.get("references") or {}
    return (refs.get("citationString") or "").strip()


# ── Download ──────────────────────────────────────────────────────────────────

def download_csv_bytes(hash_id: str, label: str = "") -> bytes:
    """
    Download a single ICOS CSV via the licence_accept endpoint, returning the
    raw bytes.  This sets the per-object cookie and follows the redirect to
    the actual data URL transparently.
    """
    ids_param = urllib.parse.quote('["' + hash_id + '"]')
    accept_url = f"https://data.icos-cp.eu/licence_accept?ids={ids_param}"
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    tag = f"[{label}] " if label else ""
    print(f"  {tag}Downloading …", end="", flush=True)
    with opener.open(urllib.request.Request(accept_url), timeout=300) as resp:
        body = resp.read()
    print(f" {len(body)/1e3:.1f} kB", flush=True)
    return body


# ── CSV → zarr ────────────────────────────────────────────────────────────────

def write_csv_to_zarr(
    store_path: str,
    cruise_id: str,
    csv_bytes: bytes,
    object_meta: dict[str, Any],
    citation: str,
) -> dict[str, Any]:
    """
    Parse one ICOS SOCAT CSV and write it as a zarr group at
    ``<store_path>/<cruise_id>``.  Returns a small summary dict
    (n_rows, columns, fixed, station_id) for logging.
    """
    si       = object_meta.get("specificInfo", {})
    columns  = si.get("columns", [])
    acq      = si.get("acquisition", {})
    station  = acq.get("station") or {}
    interval = acq.get("interval") or {}
    geo      = object_meta.get("coverageGeo") or {}

    # Build label → spec map for unit / quantity-kind lookups (only the
    # subset of variables that ICOS catalogues; the file may carry more)
    label_spec: dict[str, dict[str, Any]] = {c.get("label", ""): c for c in columns}

    # Parse CSV into a DataFrame.
    df = _parse_icos_csv(csv_bytes)

    # The ICOS catalogue calls the time column "TIMESTAMP" but the actual file
    # uses "Date/Time".  Normalise either to "TIMESTAMP".
    time_col = next(
        (c for c in df.columns if c.lower() in ("timestamp", "date/time")), None
    )
    if time_col is None:
        raise ValueError(f"{cruise_id}: no time column in CSV (looked for TIMESTAMP / Date/Time)")
    if time_col != "TIMESTAMP":
        df = df.rename(columns={time_col: "TIMESTAMP"})

    # Drop the per-row "QC Comment" and "Type" columns — useful inline metadata
    # but blow up the variable count and aren't directly plottable.  Per-cell QC
    # comments are still recoverable from the source CSV; we keep the QC Flag.
    drop_cols = [c for c in df.columns
                 if c.lower().endswith(" qc comment") or c.lower().endswith(" type")]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Map column labels → zarr array names (handles duplicates, QC suffixes)
    used_names: set[str] = set()
    label_to_name: dict[str, str] = {}
    is_qc: dict[str, bool] = {}
    for label in df.columns:
        name, qc = _column_to_zarr_name(label, used_names)
        used_names.add(name)
        label_to_name[label] = name
        is_qc[name] = qc

    # Time index
    times = pd.to_datetime(df["TIMESTAMP"], utc=True, errors="coerce")
    if times.isna().any():
        bad = int(times.isna().sum())
        print(f"    WARNING: {bad} unparseable timestamp rows dropped")
        keep = ~times.isna()
        df = df.loc[keep].reset_index(drop=True)
        times = times.loc[keep].reset_index(drop=True)
    time_arr = times.dt.tz_convert(None).to_numpy(dtype="datetime64[ns]")

    n_rows = len(df)

    # Open store and (re)create the cruise group
    store = zarr.open_group(store_path, mode="a")
    if cruise_id in store:
        del store[cruise_id]
    grp = store.require_group(cruise_id)

    grp.create_dataset(
        "time", data=time_arr.astype("datetime64[ns]"),
        chunks=(min(n_rows, 65536),), overwrite=True,
    )
    grp["time"].attrs.update({
        "_ARRAY_DIMENSIONS": ["time"],
        "long_name":         "time",
        "standard_name":     "time",
    })

    has_lon = "lon" in label_to_name.values()
    has_lat = "lat" in label_to_name.values()

    for label in df.columns:
        if label == "TIMESTAMP":
            continue
        name = label_to_name[label]
        spec = label_spec.get(label, {})
        # valueType / unit / self entries may be dict or str depending on
        # whether ICOS catalogues this column.  Be defensive.
        vt = spec.get("valueType") if isinstance(spec, dict) else None
        vt = vt if isinstance(vt, dict) else {}
        unit_obj = vt.get("unit") if isinstance(vt.get("unit"), dict) else {}
        self_obj = vt.get("self") if isinstance(vt.get("self"), dict) else {}
        unit = unit_obj.get("label") or _unit_from_label(label)
        qty  = self_obj.get("label")

        col = df[label]
        if is_qc[name]:
            arr = pd.to_numeric(col, errors="coerce").fillna(-1).astype("int8").to_numpy()
            dtype = "int8"
        else:
            arr = pd.to_numeric(col, errors="coerce").to_numpy(dtype="float64")
            dtype = "float64"

        grp.create_dataset(
            name, data=arr,
            chunks=(min(n_rows, 65536),),
            dtype=dtype,
            overwrite=True,
        )
        attrs: dict[str, Any] = {
            "_ARRAY_DIMENSIONS": ["time"],
            "icos_label":        label,
        }
        if unit:
            attrs["units"] = unit
        if qty:
            attrs["long_name"]     = qty
            std = _quantity_to_standard_name(qty, label)
            if std:
                attrs["standard_name"] = std
        grp[name].attrs.update(attrs)

    # Fixed vs moving — decided by station coverage geometry type when present,
    # otherwise by whether lon/lat columns vary across the file.
    geo_type = (geo.get("type") or "").lower()
    if geo_type in ("point",):
        fixed = True
    elif geo_type in ("linestring", "multilinestring", "polygon"):
        fixed = False
    elif has_lon and has_lat:
        try:
            lon_var = float(np.nanstd(grp["lon"][:]))
            lat_var = float(np.nanstd(grp["lat"][:]))
        except Exception:
            lon_var = lat_var = 0.0
        fixed = (lon_var < 0.001) and (lat_var < 0.001)
    else:
        fixed = True

    # Static lat/lon — used by the API for fixed-deployment bbox.  Prefer
    # the catalogue station location; fall back to the median of the in-file
    # values (handles fixed buoys whose CSV still ships lon/lat columns).
    static_lat = (station.get("location") or {}).get("lat") if station else None
    static_lon = (station.get("location") or {}).get("lon") if station else None
    if fixed and (static_lat is None or static_lon is None) and has_lon and has_lat:
        try:
            static_lon = float(np.nanmedian(grp["lon"][:]))
            static_lat = float(np.nanmedian(grp["lat"][:]))
        except Exception:
            pass

    # Group attrs (overwrite — we recreated the group)
    group_attrs = {
        "cruise_id":      cruise_id,
        "filename":       f"{cruise_id}.csv",
        "station_id":     station.get("id"),
        "platform_name":  (station.get("org") or {}).get("name"),
        "country_code":   station.get("countryCode"),
        "fixed":          fixed,
        "lat":            static_lat,
        "lon":            static_lon,
        "n_rows":         n_rows,
        "time_start":     interval.get("start"),
        "time_end":       interval.get("stop"),
        "geo_type":       geo.get("type"),
        "object_pid":     object_meta.get("pid"),
        "source_url":     object_meta.get("accessUrl"),
        "citation":       citation,
    }
    grp.attrs.update({k: v for k, v in group_attrs.items() if v is not None})

    # Provenance
    prov = {
        "created":      _now(),
        "last_updated": _now(),
        "source_doi":   object_meta.get("pid") or "",
        "citation":     citation,
        "history":      [{"action": "create", "timestamp": _now()}],
    }
    grp.attrs["_provenance"] = json.dumps(prov)

    # Consolidate metadata for this group + the whole store
    zarr.consolidate_metadata(str(Path(store_path) / cruise_id))
    zarr.consolidate_metadata(store_path)

    return {
        "n_rows":     n_rows,
        "columns":    list(label_to_name.values()),
        "fixed":      fixed,
        "station_id": station.get("id"),
    }


def _parse_icos_csv(csv_bytes: bytes) -> pd.DataFrame:
    """
    ICOS SOCAT product CSVs occasionally carry a metadata preamble before
    the actual column header; pandas handles both via the ``comment`` and
    ``skip_blank_lines`` options.  As of release 2025 the files start
    directly with the header row, but the parser is forgiving either way.
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    # Find the header line (first one starting with TIMESTAMP)
    lines = text.splitlines()
    header_idx = next(
        (i for i, ln in enumerate(lines) if ln.startswith("TIMESTAMP")), 0
    )
    body = "\n".join(lines[header_idx:])
    return pd.read_csv(io.StringIO(body))


def _quantity_to_standard_name(qty_label: str, raw_label: str) -> str:
    """Best-effort CF-ish standard_name from the ICOS quantity-kind label."""
    q = (qty_label or "").lower()
    r = (raw_label or "").lower()
    if "water temperature" in q:
        return "sea_water_temperature"
    if "practical salinity" in q:
        return "sea_water_practical_salinity"
    if "co2 (dry air mole fraction)" in q:
        return "mole_fraction_of_carbon_dioxide_in_air"
    if "co2 partial pressure (water)" in q:
        return "surface_partial_pressure_of_carbon_dioxide_in_sea_water"
    if "co2 partial pressure (air)" in q:
        return "surface_partial_pressure_of_carbon_dioxide_in_air"
    if "co2 fugacity (water)" in q:
        return "fugacity_of_carbon_dioxide_in_sea_water"
    if "co2 fugacity (air)" in q:
        return "fugacity_of_carbon_dioxide_in_air"
    if "atmospheric pressure" in q or "atmospheric pressure" in r:
        return "air_pressure"
    if "longitude" in q:
        return "longitude"
    if "latitude" in q:
        return "latitude"
    return ""


# ── Sub-command: populate ─────────────────────────────────────────────────────

def cmd_populate(args: argparse.Namespace) -> None:
    doi = args.doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    print(f"Resolving DOI {doi} …")
    collection_url = resolve_collection_url(doi)
    print(f"  Collection URL: {collection_url}")

    members = get_collection_csv_members(collection_url)
    print(f"  Found {len(members)} CSV member(s)")

    # Filters
    name_filter = set(args.member or [])
    platform_filter = set(p.upper() for p in (args.platform or []))

    store_path = str(Path(args.store).resolve())
    Path(store_path).mkdir(parents=True, exist_ok=True)
    # Initialise/open the store
    zarr.open_group(store_path, mode="a")

    processed = skipped = failed = 0
    for m in members:
        cruise_id = Path(m["name"]).stem
        platform_prefix = re.match(r"^([A-Za-z0-9]+?)\d{8}", cruise_id)
        platform_code = platform_prefix.group(1).upper() if platform_prefix else ""

        if name_filter and m["name"] not in name_filter:
            continue
        if platform_filter and platform_code not in platform_filter:
            continue

        print(f"\n{'─'*60}")
        print(f"{cruise_id}  ({m['name']})")
        try:
            object_meta = fetch_object_metadata(m["res"])
            citation    = extract_citation(object_meta)
            csv_bytes   = download_csv_bytes(m["hash_id"], label=cruise_id)
            summary = write_csv_to_zarr(
                store_path, cruise_id, csv_bytes, object_meta, citation,
            )
        except Exception as exc:
            print(f"  ERROR: {exc}", file=sys.stderr)
            failed += 1
            continue

        kind = "fixed buoy" if summary["fixed"] else "moving platform"
        print(
            f"  Wrote {summary['n_rows']} rows · {len(summary['columns'])} vars "
            f"· {kind} · station={summary['station_id']!s}"
        )
        processed += 1

    print(f"\n{'═'*60}")
    print(f"Done. {processed} written, {skipped} skipped, {failed} failed.")
    print(f"Store: {store_path}")


# ── Sub-command: list / info / remove ─────────────────────────────────────────

def cmd_list(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    if not Path(store_path).exists():
        print(f"Store does not exist: {store_path}")
        return
    store = zarr.open_group(store_path, mode="r")
    keys = sorted(store.group_keys())
    print(f"{len(keys)} cruise/deployment group(s) in {store_path}")
    for k in keys:
        attrs = store[k].attrs
        kind = "buoy" if attrs.get("fixed") else "ship"
        print(f"  {k:25s}  {kind:5s}  station={attrs.get('station_id', '?')}  "
              f"rows={attrs.get('n_rows', '?')}  "
              f"{attrs.get('time_start', '?')[:10]}–{attrs.get('time_end', '?')[:10]}")


def cmd_info(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    store = zarr.open_group(store_path, mode="r")
    if args.cruise_id not in store:
        print(f"No such cruise: {args.cruise_id}")
        sys.exit(1)
    grp = store[args.cruise_id]
    print(f"Group: {args.cruise_id}")
    print("\nAttributes:")
    for k, v in grp.attrs.items():
        s = json.dumps(v) if not isinstance(v, str) else v
        print(f"  {k:20s} = {s[:200]}")
    print("\nArrays:")
    for n in grp.array_keys():
        a = grp[n]
        print(f"  {n:20s}  shape={a.shape}  dtype={a.dtype}  "
              f"unit={a.attrs.get('units', '-')}")


def cmd_remove(args: argparse.Namespace) -> None:
    store_path = str(Path(args.store).resolve())
    store = zarr.open_group(store_path, mode="a")
    if args.cruise_id not in store:
        print(f"No such cruise: {args.cruise_id}")
        sys.exit(1)
    del store[args.cruise_id]
    zarr.consolidate_metadata(store_path)
    print(f"Removed {args.cruise_id}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build / maintain an ICOS Ocean (SOCAT) zarr store.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--store", default=DEFAULT_STORE, help="Zarr store path")
    sub = p.add_subparsers(dest="cmd")

    p_pop = sub.add_parser("populate", help="Download collection and write to store")
    p_pop.add_argument("doi", nargs="?", default=DEFAULT_DOI,
                       help="DOI of the ICOS ocean collection")
    p_pop.add_argument("--platform", action="append",
                       help="Platform-code prefix filter (e.g. 11SS); repeatable")
    p_pop.add_argument("--member", action="append",
                       help="Specific member filename to fetch (repeatable)")

    sub.add_parser("list", help="List cruise/deployment groups")

    p_info = sub.add_parser("info", help="Show details for one cruise group")
    p_info.add_argument("cruise_id")

    p_rm = sub.add_parser("remove", help="Remove a cruise group from the store")
    p_rm.add_argument("cruise_id")

    return p


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Default action when invoked as "python socat2zarr.py <doi>" without an
    # explicit sub-command — alias to "populate".
    if args.cmd is None:
        # Re-parse with "populate" injected
        argv2 = ["populate"] + (sys.argv[1:] if argv is None else argv)
        # Strip a leading --store ARGS pair if present (already consumed)
        args = parser.parse_args(argv2)

    if args.cmd == "populate":
        cmd_populate(args)
    elif args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "info":
        cmd_info(args)
    elif args.cmd == "remove":
        cmd_remove(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
