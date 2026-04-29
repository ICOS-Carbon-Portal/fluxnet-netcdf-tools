"""
combine_to_dim — build a sibling "combined" zarr group alongside per-station
groups, with a single `station` dimension carrying lat/lon/intake_height as
1-D coordinates. Enables direct xarray spatial filtering:

    ds = xr.open_zarr("icos-obspack.zarr", group="co2")
    nl = ds.where((ds.lat.between(50.7, 53.6)) & (ds.lon.between(3.3, 7.3)),
                  drop=True).sel(time_co2=slice("2024-01-01", "2024-12-31"))

Usage
-----
    python combine_to_dim.py obspack --gas co2
    python combine_to_dim.py obspack --gas co2 ch4 n2o co
    python combine_to_dim.py fluxnet --freq fluxnet_dd fluxnet_mm fluxnet_ww fluxnet_yy hh
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import zarr


# ── Obspack ──────────────────────────────────────────────────────────────────

# Variables to drop from the combined view (large per-sample, low-value)
_OBSPACK_DROP = {"obspack_id"}


def _coord_or_attr(ds: xr.Dataset, *keys, cast=float):
    """Return the first key present in attrs (or coords), cast to *cast*."""
    for k in keys:
        if k in ds.attrs:
            try:
                return cast(ds.attrs[k])
            except (TypeError, ValueError):
                continue
        if k in ds.coords:
            try:
                return cast(ds.coords[k].values.item())
            except Exception:
                continue
    return np.nan if cast is float else ""


def combine_obspack_gas(store_path: Path, gas: str) -> None:
    """Build {store}/{gas} as a combined-station zarr group for one gas."""
    z = zarr.open_group(str(store_path), mode="r")
    sids = sorted(s for s in z.group_keys()
                  if gas in z[s] and re.match(r"^[A-Z]{3}\d+$", s))
    if not sids:
        print(f"  [{gas}] no stations have this gas; skipping")
        return

    print(f"  [{gas}] {len(sids)} stations")

    # 1. Open each per-station dataset, collect:
    #    - per-gas variable list (intersection — we keep what every station has)
    #    - per-station time axis
    print(f"    scanning per-station time axes …", flush=True)
    per_station_ds: dict[str, xr.Dataset] = {}
    common_vars: set[str] | None = None
    for sid in sids:
        ds = xr.open_zarr(str(store_path), group=sid, consolidated=True)
        per_station_ds[sid] = ds
        gas_vars = {v for v in ds.data_vars
                    if (v == gas or v.startswith(f"{gas}_"))
                    and v.removeprefix(f"{gas}_") not in _OBSPACK_DROP
                    and v != f"{gas}_obspack_id"}
        common_vars = gas_vars if common_vars is None else common_vars & gas_vars
    common_vars = sorted(common_vars)
    print(f"    {len(common_vars)} variables common across stations")

    # 2. Build the union time axis
    print(f"    building union time axis …", flush=True)
    time_dim = f"time_{gas}"
    all_times: set = set()
    for sid in sids:
        all_times.update(per_station_ds[sid][time_dim].values)
    time_union = np.array(sorted(all_times))
    print(f"    union: {len(time_union)} timestamps "
          f"({str(time_union[0])[:10]} → {str(time_union[-1])[:10]})")

    # 3. Allocate target arrays
    n_st, n_t = len(sids), len(time_union)
    print(f"    target shape: ({n_st}, {n_t}) per variable")

    # Per-variable: dtype + extra dims
    var_specs: dict[str, dict] = {}
    sample_ds = per_station_ds[sids[0]]
    for v in common_vars:
        da = sample_ds[v]
        extra_dims = tuple(d for d in da.dims if d != time_dim)
        extra_shape = tuple(da.sizes[d] for d in extra_dims)
        # Use float32 fill for floats, 0 for ints, "" for object/strings
        if da.dtype.kind == "f":
            fill = np.float32(np.nan) if da.dtype == np.float32 else np.float64(np.nan)
        elif da.dtype.kind in "iu":
            fill = da.dtype.type(0)
        elif da.dtype.kind == "O":
            fill = ""
        elif da.dtype.kind in "SU":
            fill = da.dtype.type("")
        else:
            fill = 0
        var_specs[v] = {"dims": ("station", time_dim) + extra_dims,
                        "shape": (n_st, n_t) + extra_shape,
                        "dtype": da.dtype,
                        "fill":  fill,
                        "extra_dims": extra_dims,
                        "extra_shape": extra_shape,
                        "attrs": dict(da.attrs)}

    # Inherit extra-dim coord values from the first station (assumed shared
    # across all stations within a gas, which is the case for `dim_concerns`).
    extra_coords = {}
    for v in common_vars:
        for d in var_specs[v]["extra_dims"]:
            if d in sample_ds.coords:
                extra_coords[d] = sample_ds.coords[d].values

    # 4. Allocate arrays + scatter from each station
    print(f"    allocating + scattering …", flush=True)
    arrays: dict[str, np.ndarray] = {}
    for v in common_vars:
        sp = var_specs[v]
        arr = np.full(sp["shape"], sp["fill"], dtype=sp["dtype"])
        arrays[v] = arr

    def _coerce_for_zarr(vals: np.ndarray) -> np.ndarray:
        """Object arrays containing bytes are valid CF but break zarr's VLenUTF8.
        Decode bytes → str so the array is uniformly-typed for zarr."""
        if vals.dtype == object and vals.size and isinstance(vals.flat[0], (bytes, bytearray)):
            return np.array([v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else v
                             for v in vals.ravel()], dtype=object).reshape(vals.shape)
        return vals

    # Map each station's time axis into the union by sorted-side searchsorted
    for i, sid in enumerate(sids):
        ds   = per_station_ds[sid]
        t_st = ds[time_dim].values
        idx  = np.searchsorted(time_union, t_st)
        for v in common_vars:
            sp = var_specs[v]
            vals = _coerce_for_zarr(ds[v].values)
            if sp["extra_dims"]:
                arrays[v][i, idx, ...] = vals
            else:
                arrays[v][i, idx]      = vals
        if (i + 1) % 10 == 0 or (i + 1) == n_st:
            print(f"      {i+1}/{n_st} stations scattered", flush=True)

    # 5. Build per-station coord arrays
    lat   = np.array([_coord_or_attr(per_station_ds[s], "site_latitude", "station_lat")  for s in sids], dtype="float64")
    lon   = np.array([_coord_or_attr(per_station_ds[s], "site_longitude", "station_lon") for s in sids], dtype="float64")
    alt   = np.array([_coord_or_attr(per_station_ds[s], "altitude", "site_elevation")    for s in sids], dtype="float64")
    intk  = np.array([_coord_or_attr(per_station_ds[s], "intake_height")                 for s in sids], dtype="float64")
    name  = np.array([str(per_station_ds[s].attrs.get("site_name", ""))                  for s in sids], dtype=object)
    cc    = np.array([str(per_station_ds[s].attrs.get("country_code", "")
                          or per_station_ds[s].attrs.get("site_country", ""))            for s in sids], dtype=object)
    src   = np.array([str(per_station_ds[s].attrs.get(f"{gas}_source_doi", ""))          for s in sids], dtype=object)
    cal   = np.array([str(per_station_ds[s].attrs.get(f"{gas}_calibration_scale", ""))   for s in sids], dtype=object)

    # 6. Assemble the xr.Dataset
    print(f"    assembling xr.Dataset …", flush=True)
    coords = {
        "station":          ("station", np.array(sids, dtype=object)),
        "lat":              ("station", lat),
        "lon":              ("station", lon),
        "altitude":         ("station", alt),
        "intake_height":    ("station", intk),
        "site_name":        ("station", name),
        "country":          ("station", cc),
        "source_doi":       ("station", src),
        "calibration_scale":("station", cal),
        time_dim:           (time_dim, time_union),
    }
    coords.update({d: (d, v) for d, v in extra_coords.items()})

    data_vars = {}
    for v in common_vars:
        sp = var_specs[v]
        data_vars[v] = (sp["dims"], arrays[v], sp["attrs"])

    out = xr.Dataset(data_vars=data_vars, coords=coords)
    out.attrs.update({
        "gas":          gas,
        "n_stations":   n_st,
        "time_min":     str(time_union[0])[:19] + "Z",
        "time_max":     str(time_union[-1])[:19] + "Z",
        "source":       "icos-obspack.zarr (per-station groups)",
        "build_tool":   "combine_to_dim.py",
        "Conventions":  "CF-1.7",
    })

    # Default chunking: full station axis × ~year-of-hours along time.
    chunks = {"station": n_st, time_dim: min(n_t, 8760)}
    for v in common_vars:
        sp = var_specs[v]
        out[v].encoding = {
            "chunks": tuple(chunks.get(d, 1) for d in sp["dims"][:2])
                      + tuple(sp["extra_shape"]),
        }

    # 7. Write
    out_group = gas
    print(f"    writing → {store_path}/{out_group} …", flush=True)
    out.to_zarr(str(store_path), group=out_group, mode="w", consolidated=False)
    zarr.consolidate_metadata(str(store_path / out_group))
    print(f"    done ({sum(arrays[v].nbytes for v in common_vars) / 1e6:.1f} MB in-memory before zarr compression)")


# ── Fluxnet ──────────────────────────────────────────────────────────────────

# Skip variables whose dim set contains any of these — they're station-specific.
_FLUXNET_SKIP_DIMS = {"soil_layer", "r", "h", "v"}


def combine_fluxnet_freq(store_path: Path, freq: str) -> None:
    """Build {store}/_combined/{freq} for a fluxnet frequency sub-group."""
    z = zarr.open_group(str(store_path), mode="r")
    # Stations = top-level groups that are not the combined sibling.
    sids = sorted(s for s in z.group_keys() if not s.startswith("_"))
    sids = [s for s in sids if freq in z[s] or freq == "hh"]
    # For "hh" the data is the station's root group, not a sub-group
    if freq == "hh":
        group_path_for = lambda sid: sid
    else:
        group_path_for = lambda sid: f"{sid}/{freq}"
    sids = [s for s in sids if group_path_for(s) in z or freq in z.get(s, {})]
    if not sids:
        print(f"  [{freq}] no stations have this group; skipping")
        return

    print(f"  [{freq}] {len(sids)} stations")

    # 1. Open per-station, find variables whose dims avoid the station-specific
    #    set. Take the intersection across stations of eligible variable names.
    print(f"    scanning per-station variables …", flush=True)
    per_station_ds: dict[str, xr.Dataset] = {}
    common_vars: set[str] | None = None
    for sid in sids:
        try:
            ds = xr.open_zarr(str(store_path), group=group_path_for(sid),
                              consolidated=False)
        except Exception as exc:
            print(f"    [{sid}] open failed: {exc}; skipping")
            continue
        per_station_ds[sid] = ds
        eligible = {v for v in ds.data_vars
                    if not (set(ds[v].dims) & _FLUXNET_SKIP_DIMS)
                    and "time" in ds[v].dims}
        common_vars = eligible if common_vars is None else common_vars & eligible
    sids = list(per_station_ds.keys())
    common_vars = sorted(common_vars or [])
    if not common_vars:
        print(f"    no common combinable variables; skipping {freq}")
        return
    print(f"    {len(common_vars)} variables common across stations")

    # 2. Union time axis
    print(f"    building union time axis …", flush=True)
    all_times: set = set()
    for sid in sids:
        all_times.update(per_station_ds[sid]["time"].values)
    time_union = np.array(sorted(all_times))
    print(f"    union: {len(time_union)} timestamps "
          f"({str(time_union[0])[:10]} → {str(time_union[-1])[:10]})")

    # 3. Per-variable specs (use the first station with the var as the
    #    template for dtype + extra-dim sizes; verify others match).
    n_st, n_t = len(sids), len(time_union)
    sample_ds = per_station_ds[sids[0]]
    var_specs: dict[str, dict] = {}
    for v in common_vars:
        da = sample_ds[v]
        extra_dims  = tuple(d for d in da.dims if d != "time")
        extra_shape = tuple(da.sizes[d] for d in extra_dims)
        if da.dtype.kind == "f":
            fill = np.float32(np.nan) if da.dtype == np.float32 else np.float64(np.nan)
        elif da.dtype.kind in "iu":
            fill = da.dtype.type(0)
        elif da.dtype.kind == "O":
            fill = ""
        elif da.dtype.kind in "SU":
            fill = da.dtype.type("")
        else:
            fill = 0
        var_specs[v] = {"dims": ("station", "time") + extra_dims,
                        "shape": (n_st, n_t) + extra_shape,
                        "dtype": da.dtype,
                        "fill":  fill,
                        "extra_dims": extra_dims,
                        "extra_shape": extra_shape,
                        "attrs": dict(da.attrs)}

    # Inherit extra-dim coord values from the first station
    extra_coords = {}
    for v in common_vars:
        for d in var_specs[v]["extra_dims"]:
            if d in sample_ds.coords:
                extra_coords[d] = sample_ds.coords[d].values

    # 4. Allocate + scatter
    print(f"    allocating + scattering …", flush=True)
    arrays: dict[str, np.ndarray] = {}
    for v in common_vars:
        sp = var_specs[v]
        arrays[v] = np.full(sp["shape"], sp["fill"], dtype=sp["dtype"])

    def _coerce(vals):
        if vals.dtype == object and vals.size and isinstance(vals.flat[0], (bytes, bytearray)):
            return np.array([x.decode("utf-8", errors="replace") if isinstance(x, (bytes, bytearray)) else x
                             for x in vals.ravel()], dtype=object).reshape(vals.shape)
        return vals

    for i, sid in enumerate(sids):
        ds   = per_station_ds[sid]
        t_st = ds["time"].values
        idx  = np.searchsorted(time_union, t_st)
        for v in common_vars:
            sp = var_specs[v]
            try:
                vals = _coerce(ds[v].values)
            except Exception:
                continue
            if sp["extra_dims"]:
                arrays[v][i, idx, ...] = vals
            else:
                arrays[v][i, idx]      = vals
        if (i + 1) % 5 == 0 or (i + 1) == n_st:
            print(f"      {i+1}/{n_st} stations scattered", flush=True)

    # 5. Per-station coords from group .zattrs (lat/lon/etc. were patched in)
    def _attr_float(ds, *keys):
        for k in keys:
            v = ds.attrs.get(k)
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
        return np.nan

    lat   = np.array([_attr_float(per_station_ds[s], "geospatial_lat", "site_latitude")  for s in sids], dtype="float64")
    lon   = np.array([_attr_float(per_station_ds[s], "geospatial_lon", "site_longitude") for s in sids], dtype="float64")
    elev  = np.array([_attr_float(per_station_ds[s], "station_elevation", "altitude")    for s in sids], dtype="float64")
    name  = np.array([str(per_station_ds[s].attrs.get("station_name",
                          per_station_ds[s].attrs.get("site_name", "")))                 for s in sids], dtype=object)
    cc    = np.array([str(per_station_ds[s].attrs.get("country", ""))                    for s in sids], dtype=object)
    eco   = np.array([str(per_station_ds[s].attrs.get("ecosystem", ""))                  for s in sids], dtype=object)
    src   = np.array([str(per_station_ds[s].attrs.get("source_doi", ""))                 for s in sids], dtype=object)

    # 6. Assemble dataset
    print(f"    assembling xr.Dataset …", flush=True)
    coords = {
        "station":          ("station", np.array(sids, dtype=object)),
        "lat":              ("station", lat),
        "lon":              ("station", lon),
        "station_elevation":("station", elev),
        "station_name":     ("station", name),
        "country":          ("station", cc),
        "ecosystem":        ("station", eco),
        "source_doi":       ("station", src),
        "time":             ("time", time_union),
    }
    coords.update({d: (d, v) for d, v in extra_coords.items()})

    data_vars = {v: (var_specs[v]["dims"], arrays[v], var_specs[v]["attrs"])
                 for v in common_vars}

    out = xr.Dataset(data_vars=data_vars, coords=coords)
    out.attrs.update({
        "freq":         freq,
        "n_stations":   n_st,
        "time_min":     str(time_union[0])[:19] + "Z",
        "time_max":     str(time_union[-1])[:19] + "Z",
        "source":       "icos-fluxnet.zarr (per-station groups)",
        "build_tool":   "combine_to_dim.py",
        "Conventions":  "CF-1.12",
    })

    # Default chunking
    chunks = {"station": n_st, "time": min(n_t, 1024)}
    for v in common_vars:
        sp = var_specs[v]
        out[v].encoding = {
            "chunks": tuple(chunks.get(d, 1) for d in sp["dims"][:2])
                      + tuple(sp["extra_shape"]),
        }

    out_group = f"_combined/{freq}"
    print(f"    writing → {store_path}/{out_group} …", flush=True)
    out.to_zarr(str(store_path), group=out_group, mode="w", consolidated=False)
    zarr.consolidate_metadata(str(store_path / "_combined" / freq))
    raw_size = sum(arrays[v].nbytes for v in common_vars)
    print(f"    done ({raw_size / 1e6:.1f} MB in-memory before zarr compression)")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(prog="combine_to_dim",
                                description="Build station-dim combined zarr groups")
    sub = p.add_subparsers(dest="command")

    op = sub.add_parser("obspack")
    op.add_argument("--store", default="icos-obspack.zarr",
                    help="Path to obspack zarr store")
    op.add_argument("--gas",   nargs="+", default=["co2", "ch4", "n2o", "co"],
                    choices=["co2", "ch4", "n2o", "co"],
                    help="Gases to combine")

    fp = sub.add_parser("fluxnet")
    fp.add_argument("--store", default="icos-fluxnet.zarr",
                    help="Path to fluxnet zarr store")
    fp.add_argument("--freq",  nargs="+",
                    default=["fluxnet_dd", "fluxnet_mm", "fluxnet_ww", "fluxnet_yy"],
                    help="Frequency sub-groups to combine")

    args = p.parse_args()
    if not args.command:
        p.print_help()
        sys.exit(1)

    store = Path(args.store).resolve()
    if not store.exists():
        sys.exit(f"ERROR: store {store} not found")

    if args.command == "obspack":
        for gas in args.gas:
            print(f"\n━━━ obspack / {gas} ━━━")
            combine_obspack_gas(store, gas)
        print("\nReconsolidating store-root metadata …", flush=True)
        zarr.consolidate_metadata(str(store))
    elif args.command == "fluxnet":
        for freq in args.freq:
            print(f"\n━━━ fluxnet / {freq} ━━━")
            combine_fluxnet_freq(store, freq)
        print("\nReconsolidating store-root metadata …", flush=True)
        zarr.consolidate_metadata(str(store))

    print("Done.")


if __name__ == "__main__":
    main()
