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

    # New for Part B: per-station citation, station landing-page URL, and
    # the per-station obspack file name (for DataObject entities).
    cite  = np.array([str(per_station_ds[s].attrs.get(f"{gas}_dobj_citation",
                          per_station_ds[s].attrs.get(f"{gas}_obspack_citation", "")))
                      for s in sids], dtype=object)
    surl  = np.array([str(per_station_ds[s].attrs.get("station_landing_page",
                          per_station_ds[s].attrs.get("site_url", "")))
                      for s in sids], dtype=object)
    fname = np.array([str(per_station_ds[s].attrs.get(f"{gas}_dataset_name", ""))
                      for s in sids], dtype=object)

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
        "citation":         ("station", cite),
        "station_url":      ("station", surl),
        "dataset_name":     ("station", fname),
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
                    and "time" in ds[v].dims
                    and "nv" not in ds[v].dims     # skip time_bounds (sparse, breaks HTTP decode)
                    and v != "time_bounds"}
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
    # New for Part B
    cite  = np.array([str(per_station_ds[s].attrs.get("citation",
                          per_station_ds[s].attrs.get("PartOfDataset", "")))              for s in sids], dtype=object)
    surl  = np.array([str(per_station_ds[s].attrs.get("icos_landing_page",
                          per_station_ds[s].attrs.get("site_url", "")))                  for s in sids], dtype=object)

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
        "citation":         ("station", cite),
        "station_url":      ("station", surl),
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


# ── SOCAT ────────────────────────────────────────────────────────────────────

# SOCAT zarr stores have one group per cruise leg / buoy deployment (named after
# the source CSV stem, e.g. "11SS20240501").  This combines them into a single
# `(deployment, time)` view.  Differences from the fluxnet/obspack combiners:
#
#  • lat/lon are *per row*, not per station — so they're 2-D
#    coords on (deployment, time), padded with NaN where a deployment
#    has no sample at the union timestamp.
#  • WOCE QC masking is applied per-variable at scatter time (flag <= 2),
#    matching what the ENVRI app does on the per-cruise path.
#  • Static metadata (station_id, platform_name, fixed, source_doi, citation)
#    is exposed as 1-D coords along `deployment`.

# ENVRI ocean variables of interest + the matching QC array name.  We only
# combine these; the fuller per-row set (Equilibrator Pressure, etc.) is
# still queryable per-cruise.
_SOCAT_VARS: list[tuple[str, str]] = [
    ("Temp",     "Temp_QC"),
    ("P_sal",    "P_sal_QC"),
    ("xCO2_atm", "xCO2_atm_QC"),
    ("pCO2",     "pCO2_QC"),
    ("fCO2",     "fCO2_QC"),
]


def combine_socat(store_path: Path) -> None:
    """Build {store}/_combined as a (deployment, time) view of all cruises."""
    z = zarr.open_group(str(store_path), mode="r")
    cruises = sorted(s for s in z.group_keys() if not s.startswith("_"))
    if not cruises:
        print("  no cruise groups; skipping")
        return
    print(f"  {len(cruises)} cruise/deployment group(s)")

    # 1. Open every cruise (lazy) and find ones that actually carry the vars
    print("    scanning per-cruise time axes …", flush=True)
    per_ds: dict[str, xr.Dataset] = {}
    for cid in cruises:
        try:
            ds = xr.open_zarr(str(store_path), group=cid, consolidated=True)
        except Exception as exc:
            print(f"    [{cid}] open failed: {exc}; skipping")
            continue
        if "time" not in ds:
            continue
        per_ds[cid] = ds
    cruises = list(per_ds.keys())

    # 2. Build the union time axis
    print("    building union time axis …", flush=True)
    all_times: set = set()
    for cid in cruises:
        all_times.update(per_ds[cid]["time"].values)
    time_union = np.array(sorted(all_times), dtype="datetime64[ns]")
    n_dep, n_t = len(cruises), len(time_union)
    print(f"    union: {n_t} timestamps "
          f"({str(time_union[0])[:10]} → {str(time_union[-1])[:10]})  "
          f"× {n_dep} deployments")

    # 3. Allocate target arrays.  Float32 NaN-padded; QC-masked at fill.
    arrays: dict[str, np.ndarray] = {}
    var_attrs: dict[str, dict] = {}
    for v, _qc in _SOCAT_VARS:
        # Use the first cruise that has this var as the attr template
        attrs = {}
        for cid in cruises:
            if v in per_ds[cid]:
                attrs = dict(per_ds[cid][v].attrs)
                break
        var_attrs[v] = attrs
        arrays[v] = np.full((n_dep, n_t), np.nan, dtype="float32")

    # 2-D lon/lat — NaN-padded (fixed buoys broadcast their static value
    # across their deployment's time slots).
    lon2d = np.full((n_dep, n_t), np.nan, dtype="float32")
    lat2d = np.full((n_dep, n_t), np.nan, dtype="float32")

    # 4. Scatter per cruise
    print("    allocating + scattering …", flush=True)
    for i, cid in enumerate(cruises):
        ds   = per_ds[cid]
        t_st = ds["time"].values
        idx  = np.searchsorted(time_union, t_st)

        # Lon/lat: prefer the per-row arrays when present; otherwise broadcast
        # the static lat/lon from group attrs across the deployment's time.
        if "lon" in ds and "lat" in ds:
            lo = ds["lon"].values.astype("float32")
            la = ds["lat"].values.astype("float32")
            # Position QC, when present, masks both lon and lat
            pos_ok = np.ones(t_st.size, dtype=bool)
            for q in ("lon_QC", "lat_QC"):
                if q in ds:
                    qv = ds[q].values
                    pos_ok &= (qv >= 0) & (qv <= 2)
            lo = np.where(pos_ok, lo, np.nan)
            la = np.where(pos_ok, la, np.nan)
            lon2d[i, idx] = lo
            lat2d[i, idx] = la
        else:
            sa = ds.attrs
            slat = sa.get("lat", np.nan)
            slon = sa.get("lon", np.nan)
            try:
                slat_f = float(slat); slon_f = float(slon)
                lon2d[i, idx] = slon_f
                lat2d[i, idx] = slat_f
            except (TypeError, ValueError):
                pass

        # Science variables: WOCE-mask, then scatter
        for v, qc_name in _SOCAT_VARS:
            if v not in ds:
                continue
            vals = ds[v].values.astype("float32")
            ok = np.isfinite(vals)
            if qc_name in ds:
                qv = ds[qc_name].values
                ok &= (qv >= 0) & (qv <= 2)
            vals = np.where(ok, vals, np.nan)
            arrays[v][i, idx] = vals

        if (i + 1) % 10 == 0 or (i + 1) == n_dep:
            print(f"      {i+1}/{n_dep} deployments scattered", flush=True)

    # 5. Per-deployment static coords from each cruise's .zattrs
    def _attr_str(ds: xr.Dataset, *keys) -> str:
        for k in keys:
            v = ds.attrs.get(k)
            if v not in (None, ""):
                return str(v)
        return ""

    def _attr_bool(ds: xr.Dataset, key: str) -> bool:
        v = ds.attrs.get(key)
        return bool(v)

    station_id = np.array([_attr_str(per_ds[c], "station_id")    for c in cruises], dtype=object)
    platform   = np.array([_attr_str(per_ds[c], "platform_name") for c in cruises], dtype=object)
    fixed      = np.array([_attr_bool(per_ds[c], "fixed")        for c in cruises], dtype=bool)
    source_doi = np.array([_attr_str(per_ds[c], "source_doi", "object_pid") for c in cruises], dtype=object)
    citation   = np.array([_attr_str(per_ds[c], "citation")      for c in cruises], dtype=object)
    cc         = np.array([_attr_str(per_ds[c], "country_code")  for c in cruises], dtype=object)

    # Re-prefix bare PIDs (e.g. "11676/abc") with the Handle resolver
    source_doi = np.array(
        [s if s.startswith("http") or s == "" else f"https://hdl.handle.net/{s}"
         for s in source_doi], dtype=object,
    )

    # 6. Assemble dataset
    print("    assembling xr.Dataset …", flush=True)
    coords = {
        "deployment":     ("deployment", np.array(cruises, dtype=object)),
        "station_id":     ("deployment", station_id),
        "platform_name":  ("deployment", platform),
        "fixed":          ("deployment", fixed),
        "country_code":   ("deployment", cc),
        "source_doi":     ("deployment", source_doi),
        "citation":       ("deployment", citation),
        "lon":            (("deployment", "time"), lon2d),
        "lat":            (("deployment", "time"), lat2d),
        "time":           ("time", time_union),
    }
    data_vars = {
        v: (("deployment", "time"), arrays[v], var_attrs[v])
        for v, _ in _SOCAT_VARS
    }

    out = xr.Dataset(data_vars=data_vars, coords=coords)
    out.attrs.update({
        "n_deployments": n_dep,
        "time_min":      str(time_union[0])[:19] + "Z",
        "time_max":      str(time_union[-1])[:19] + "Z",
        "qc_filter":     "WOCE flag <= 2 on the variable's QC and on lon_QC/lat_QC",
        "source":        "icos-socat.zarr (per-cruise groups)",
        "build_tool":    "combine_to_dim.py socat",
        "Conventions":   "CF-1.7",
    })

    # Chunking: full deployment axis × ~year-of-minutes along time.  SOOP cadence
    # is ~1 min; n_t for an annual subset is ~525k, so 65 536 keeps any single
    # bbox query inside ~8 chunks.
    chunks = {"deployment": n_dep, "time": min(n_t, 65_536)}
    for v, _ in _SOCAT_VARS:
        out[v].encoding = {"chunks": (chunks["deployment"], chunks["time"])}
    for c in ("lon", "lat"):
        out[c].encoding = {"chunks": (chunks["deployment"], chunks["time"])}

    out_group = "_combined"
    print(f"    writing → {store_path}/{out_group} …", flush=True)
    out.to_zarr(str(store_path), group=out_group, mode="w", consolidated=False)
    zarr.consolidate_metadata(str(store_path / "_combined"))
    raw_size = sum(arrays[v].nbytes for v in arrays) + lon2d.nbytes + lat2d.nbytes
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

    sp = sub.add_parser("socat")
    sp.add_argument("--store", default="icos-socat.zarr",
                    help="Path to SOCAT zarr store")

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
    elif args.command == "socat":
        print(f"\n━━━ socat / _combined ━━━")
        combine_socat(store)
        print("\nReconsolidating store-root metadata …", flush=True)
        zarr.consolidate_metadata(str(store))

    print("Done.")


if __name__ == "__main__":
    main()
