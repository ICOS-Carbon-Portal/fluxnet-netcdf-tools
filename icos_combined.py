#!/usr/bin/env python3
"""
Combine all ICOS ETC L2 CSV files for a site into a single CF-1.12 NetCDF4.

All half-hourly products (FLUXNET_HH, FLUXES, METEO, METEOSENS) are merged
into a single /half_hourly group.  Variables that appear in more than one
half-hourly product are removed entirely to avoid ambiguity.

Aggregated products each get their own group with their own time axis:
  /half_hourly  merged from FLUXNET_HH, FLUXES, METEO, METEOSENS
  /fluxnet_dd   from ICOSETC_CC-###_FLUXNET_DD_INTERIM_L2.csv
  /fluxnet_ww   …WW…
  /fluxnet_mm   …MM…
  /fluxnet_yy   …YY…

Usage:
    python icos_combined.py ICOSETC_SE-Svb_*.csv
    python icos_combined.py ICOSETC_SE-Svb_*.csv -o SE-Svb_all.nc

Dependencies:
    pip install numpy pandas netCDF4
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd

# Re-use helpers from fluxnet2nc (must be in the same directory or on sys.path)
from fluxnet2nc import (
    FILL_VALUE_IN,
    FILL_VALUE_OUT,
    _FREQ_ISO,
    _FREQ_OFFSET,
    _GLOBAL_ATTRS,
    _build_long_name,
    _choose_int_dtype,
    _detect_freq_code,
    _get_standard_name,
    _get_units,
    _group_name,
    _infer_freq_code,
    _parse_timestamps,
    _product_priority,
    _read_csv,
    _to_cf_time,
    fetch_doi_citation,
    fetch_icos_station_meta,
    parse_filename,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _qc_norm(col: str) -> str:
    """Normalize QC/FLAG suffix so VAR_QC and VAR_FLAG map to the same key."""
    if col.endswith("_QC"):
        return col[:-3]
    if col.endswith("_FLAG"):
        return col[:-5]
    return col


def _write_group(
    root_ds:     nc.Dataset,
    grp_name:    str,
    df:          pd.DataFrame,
    ts_start:    pd.DatetimeIndex,
    ts_end:      pd.DatetimeIndex | None,
    freq_code:   str,
    source_file: str,
    skip_vars:   set[str],
    grp:         nc.Dataset | None = None,
) -> set[str]:
    """Write one group to *root_ds*; return the set of variable names written.

    If *grp* is provided it is used directly (writing into the root dataset or
    a pre-created group) instead of creating a new child group.
    Child groups inherit the root-level global attributes so that every group
    is self-describing when opened in isolation.
    """
    if grp is None:
        grp = root_ds.createGroup(grp_name)
        # Propagate root global attributes to the child group
        for attr in _GLOBAL_ATTRS:
            val = getattr(root_ds, attr, None)
            if val is not None:
                setattr(grp, attr, val)
    grp.source              = source_file
    grp.temporal_resolution = _FREQ_ISO.get(freq_code, "unknown")
    grp.featureType         = "timeSeries"

    _TS_COLS = {"TIMESTAMP", "TIMESTAMP_START", "TIMESTAMP_END"}
    data_cols = [c for c in df.columns if c not in _TS_COLS and c not in skip_vars]

    # ── Time coordinate ───────────────────────────────────────────────────────
    grp.createDimension("time", len(ts_start))
    epoch      = ts_start[0].replace(month=1, day=1, hour=0, minute=0,
                                      second=0, microsecond=0)
    time_units = f"minutes since {epoch.strftime('%Y-%m-%d %H:%M:%S')}"
    time_vals  = _to_cf_time(ts_start, epoch)

    tvar = grp.createVariable("time", "f8", ("time",))
    tvar.standard_name = "time"
    tvar.long_name     = "time at start of averaging period"
    tvar.units         = time_units
    tvar.calendar      = "standard"
    tvar.axis          = "T"
    tvar[:]            = time_vals

    if ts_end is not None:
        grp.createDimension("nv", 2)
        tvar.bounds = "time_bounds"
        end_vals    = _to_cf_time(ts_end, epoch)
        tbvar = grp.createVariable("time_bounds", "f8", ("time", "nv"))
        tbvar.units    = time_units
        tbvar.calendar = "standard"
        tbvar[:, 0]    = time_vals
        tbvar[:, 1]    = end_vals

    # ── Data variables ────────────────────────────────────────────────────────
    written: set[str] = set()
    skipped: list[str] = []

    for col in data_cols:
        is_qc = col.endswith("_QC") or col.endswith("_FLAG")
        raw   = df[col].values
        mask  = np.asarray(pd.isna(df[col]))

        if is_qc:
            # QC flags: integer storage for HH (0–3 or 12-cipher METEOSENS codes);
            # float32 when values are fractional (DD/WW/MM/YY mean-QC).
            safe = np.where(mask, 0, np.asarray(raw, dtype=np.float64))
            if np.any(safe != np.floor(safe)):
                dtype: str       = "f4"
                fv:   object     = FILL_VALUE_OUT
                arr:  np.ndarray = np.where(mask, fv, safe.astype(np.float32))
            else:
                vmax = int(safe.max()) if safe.size else 0
                if vmax <= 254:
                    dtype, fv = "u1", np.uint8(255)
                elif vmax <= 2_147_483_647:
                    dtype, fv = "i4", np.int32(-1)
                else:
                    dtype, fv = "i8", np.int64(-1)
                arr = np.where(mask, fv, safe.astype(np.dtype(dtype)))
        else:
            try:
                f64 = np.asarray(raw, dtype=np.float64)
            except (ValueError, TypeError):
                skipped.append(col)
                continue
            safe  = np.where(mask, 0.0, f64)
            int_t = _choose_int_dtype(safe[~mask])
            if int_t:
                dtype, fv = int_t
                arr = np.where(mask, fv, safe.astype(np.dtype(dtype)))
            else:
                dtype, fv = "f4", FILL_VALUE_OUT
                arr = np.where(mask, fv, f64.astype(np.float32))

        var = grp.createVariable(
            col, dtype, ("time",), fill_value=fv, zlib=True, complevel=4,
        )
        var.long_name     = _build_long_name(col, is_qc=is_qc)
        var.missing_value = fv

        if is_qc:
            var.units = "1"
            if dtype == "u1":
                var.flag_values   = np.array([0, 1, 2, 3], dtype=np.uint8)
                var.flag_meanings = (
                    "measured good_quality_gap_fill "
                    "medium_quality_gap_fill poor_quality_gap_fill"
                )
            elif dtype in ("i4", "i8"):
                var.comment = (
                    "12-digit composite quality flag (METEOSENS). "
                    "Each cipher encodes a specific check; first cipher is always 8."
                )
        else:
            var.units = _get_units(col)
            sn = _get_standard_name(col)
            if sn:
                var.standard_name = sn
            if dtype == "f4":
                var.fluxnet_missing_value = np.int32(FILL_VALUE_IN)

        var[:] = arr
        written.add(col)

    if skipped:
        print(f"    Skipped {len(skipped)} non-numeric column(s): "
              f"{', '.join(skipped[:5])}{'…' if len(skipped) > 5 else ''}")
    return written


# ── Main combination ──────────────────────────────────────────────────────────

def combine(csv_paths: list[Path], nc_path: Path, args: argparse.Namespace) -> None:
    # (priority, group_name, csv_path, df, freq_code)
    SourceEntry = tuple[int, str, Path, pd.DataFrame, str]
    sources: list[SourceEntry] = []
    site_id = args.site_id

    for csv_path in csv_paths:
        print(f"Reading  {csv_path.name}")
        df    = _read_csv(csv_path)
        finfo = parse_filename(csv_path)
        if not site_id:
            site_id = finfo.get("site_id", "unknown")

        # Detect timestamp column
        if "TIMESTAMP_START" in df.columns:
            ts_col = "TIMESTAMP_START"
        elif "TIMESTAMP" in df.columns:
            ts_col = "TIMESTAMP"
        else:
            print(f"  WARNING: no timestamp column — skipping", file=sys.stderr)
            continue

        ts_start  = _parse_timestamps(df[ts_col])
        freq_code = _detect_freq_code(csv_path) or _infer_freq_code(ts_start)
        sources.append((
            _product_priority(csv_path),
            _group_name(csv_path),
            csv_path, df, freq_code,
        ))

    if not sources:
        sys.exit("ERROR: no valid CSV files to combine.")

    # Within each resolution, process highest-priority products first
    sources.sort(key=lambda x: (x[4], x[0]))  # (freq_code, priority)

    _TS_COLS = {"TIMESTAMP", "TIMESTAMP_START", "TIMESTAMP_END"}
    HH_CODES = {"HH", "HR"}

    hh_sources  = [(p, g, path, df, fc) for p, g, path, df, fc in sources if fc in HH_CODES]
    agg_sources = [(p, g, path, df, fc) for p, g, path, df, fc in sources if fc not in HH_CODES]

    print(f"Fetching ICOS station metadata for {site_id} …")
    station_meta = fetch_icos_station_meta(site_id)

    # Pre-fetched per-archive citation takes priority (set by the download
    # pipeline via args.doi_url / args.doi_citation); fall back to fetching
    # from args.doi for standalone CLI use.
    doi_url      = getattr(args, "doi_url", "") or ""
    doi_citation = getattr(args, "doi_citation", "") or ""
    if not doi_url and getattr(args, "doi", None):
        print(f"Fetching APA citation for DOI {args.doi} …")
        doi_url, doi_citation = fetch_doi_citation(args.doi)

    print(f"\nWriting  {nc_path}")
    with nc.Dataset(nc_path, "w", format="NETCDF4") as root_ds:
        root_ds.Conventions = "CF-1.12"
        root_ds.title       = f"ICOS ETC L2 combined data — site {site_id}"
        root_ds.institution = "ICOS Carbon Portal / FLUXNET"
        root_ds.site_id     = site_id
        root_ds.featureType = "timeSeries"
        root_ds.history     = (
            f"Created {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by icos_combined.py"
        )
        if args.comment:
            root_ds.comment = args.comment
        for attr_key, attr_val in station_meta.items():
            setattr(root_ds, attr_key, attr_val)
        if doi_url:
            root_ds.source_doi    = doi_url
        if doi_citation:
            root_ds.PartOfDataset = doi_citation
        dobj_citation = getattr(args, "dobj_citation", "") or ""
        if dobj_citation:
            root_ds.citation = dobj_citation

        written_by_res: dict[str, set[str]] = {}

        # ── Half-hourly: merge all products into one group ────────────────────
        if hh_sources:
            # Count in how many products each variable appears.
            # _QC and _FLAG suffixes are treated as equivalent (same base variable),
            # so VAR_QC from one product and VAR_FLAG from another are considered
            # duplicates and both removed.
            norm_count: dict[str, int] = {}   # normalized_name -> product count
            col_to_norm: dict[str, str]  = {}  # col -> normalized_name
            for _, _, _, df, _ in hh_sources:
                seen: set[str] = set()
                for col in df.columns:
                    if col not in _TS_COLS:
                        norm = _qc_norm(col)
                        col_to_norm[col] = norm
                        if norm not in seen:
                            norm_count[norm] = norm_count.get(norm, 0) + 1
                            seen.add(norm)

            hh_dupes = {col for col, norm in col_to_norm.items()
                        if norm_count.get(norm, 0) > 1}

            # Parse each source's timestamps and build a union time index
            hh_freq_code = hh_sources[0][4]
            parsed: list[tuple[Path, pd.DataFrame, pd.DatetimeIndex]] = []
            for _, _, path, df, _ in hh_sources:
                src_ts = "TIMESTAMP_START" if "TIMESTAMP_START" in df.columns else "TIMESTAMP"
                parsed.append((path, df, _parse_timestamps(df[src_ts])))

            source_names = [p.name for p, _, _ in parsed]
            all_ts: pd.DatetimeIndex = parsed[0][2]
            for _, _, ts in parsed[1:]:
                all_ts = all_ts.union(ts)

            # Align each source on the union index (outer join on timestamp)
            data_parts: list[pd.DataFrame] = []
            for _, df, ts_idx in parsed:
                unique_cols = [c for c in df.columns if c not in _TS_COLS and c not in hh_dupes]
                if unique_cols:
                    part = df[unique_cols].copy()
                    part.index = ts_idx
                    data_parts.append(part.reindex(all_ts))
            merged_data = pd.concat(data_parts, axis=1) if data_parts else pd.DataFrame(index=all_ts)

            ts_start = all_ts
            ts_end: pd.DatetimeIndex | None = (
                pd.DatetimeIndex([t + _FREQ_OFFSET[hh_freq_code] for t in ts_start])
                if hh_freq_code in _FREQ_OFFSET else None
            )

            # Write directly into the root dataset so xr.open_dataset() works
            written = _write_group(
                root_ds, "half_hourly", merged_data, ts_start, ts_end,
                hh_freq_code, ", ".join(source_names), set(),
                grp=root_ds,
            )
            written_by_res[hh_freq_code] = written

            print(
                f"  /  (root)               {len(written):4d} vars  [{hh_freq_code}]"
                f"  ({len(hh_dupes)} duplicate variable(s) removed)"
            )

        # ── Aggregated: one group per product/resolution ──────────────────────
        for priority, grp_name, csv_path, df, freq_code in agg_sources:
            if "TIMESTAMP_START" in df.columns:
                ts_start = _parse_timestamps(df["TIMESTAMP_START"])
                ts_end = (
                    _parse_timestamps(df["TIMESTAMP_END"])
                    if "TIMESTAMP_END" in df.columns else None
                )
            else:
                ts_start = _parse_timestamps(df["TIMESTAMP"])
                ts_end   = None

            if ts_end is None and freq_code in _FREQ_OFFSET:
                ts_end = pd.DatetimeIndex(
                    [t + _FREQ_OFFSET[freq_code] for t in ts_start]
                )

            skip_vars = written_by_res.get(freq_code, set())
            written   = _write_group(
                root_ds, grp_name, df, ts_start, ts_end,
                freq_code, csv_path.name, skip_vars,
            )
            written_by_res.setdefault(freq_code, set()).update(written)

            n_dup = sum(
                1 for c in df.columns
                if c not in _TS_COLS and c in skip_vars
            )
            dup_note = f"  ({n_dup} duplicate(s) skipped)" if n_dup else ""
            print(f"  /{grp_name:22s}  {len(written):4d} vars"
                  f"  [{freq_code}]{dup_note}")

    print(f"Done.    {nc_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Combine ICOS ETC L2 CSV files for one site into a single "
            "CF-1.12 NetCDF4, one group per product/resolution."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "csv", nargs="+",
        help="Input CSV files; glob patterns supported (e.g. ICOSETC_SE-Svb_*.csv)",
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=None, metavar="NC",
        help=(
            "Output NetCDF file "
            "(default: ICOSETC_<site_id>_combined.nc in the same directory as the inputs)"
        ),
    )
    parser.add_argument(
        "--site-id", default="",
        help="Override site ID (auto-detected from ICOS filename convention)",
    )
    parser.add_argument(
        "--comment", default="",
        help="Free-text comment added as a global attribute",
    )
    parser.add_argument(
        "--doi", default="",
        metavar="DOI",
        help=(
            "DOI of the source dataset collection (e.g. 10.18160/R3G6-Z8ZH). "
            "When given, the canonical DOI URL is stored as source_doi and the "
            "APA citation fetched from doi.org is stored as PartOfDataset."
        ),
    )
    args = parser.parse_args()

    # Expand glob patterns (required on Windows where the shell does not expand them)
    csv_paths: list[Path] = []
    for pattern in args.csv:
        matches = (
            sorted(Path().glob(pattern))
            if any(c in pattern for c in "*?[")
            else [Path(pattern)]
        )
        csv_paths.extend(matches)

    csv_paths = [
        p.resolve() for p in csv_paths
        if p.suffix.lower() == ".csv" and p.exists()
    ]
    if not csv_paths:
        sys.exit("ERROR: no CSV files found.")

    if args.output:
        nc_path = args.output.resolve()
    else:
        finfo   = parse_filename(csv_paths[0])
        site    = args.site_id or finfo.get("site_id", "combined")
        interim = "_INTERIM" if any("INTERIM" in p.stem.upper() for p in csv_paths) else ""
        nc_path = csv_paths[0].parent / f"ICOSETC_{site}{interim}_combined.nc"

    combine(csv_paths, nc_path, args)


if __name__ == "__main__":
    main()
