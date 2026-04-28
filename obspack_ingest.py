"""
obspack_ingest — read one ICOS Obspack netCDF file and write its data into a
zarr group. The group is named by station ID (trigram + rounded intake height,
e.g. ``HTM150``); the measurement variable is renamed to the gas name and
rescaled to community units (CO2 → ppm, CH4/N2O/CO → ppb).

Static columns (constant across the time axis) are promoted to group ``.zattrs``
to avoid duplicating non-time-varying data.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import xarray as xr


# ── Constants ────────────────────────────────────────────────────────────────

GAS_SCALE = {
    "co2": (1e6, "ppm"),
    "ch4": (1e9, "ppb"),
    "n2o": (1e9, "ppb"),
    "co":  (1e9, "ppb"),
}

# Time-varying columns we DROP — redundant with the `time` coordinate or
# trivially recomputable.
_DROP_VARS = {
    "start_time", "datetime", "time_decimal",
    "time_components", "solartime_components",
    "obs_num",
}

# Candidate static columns — if all values equal the first, promote to .zattrs.
_STATIC_CANDIDATES = (
    "latitude", "longitude", "altitude", "intake_height",
    "instrument", "icos_datalevel",
)

# Variables in mole-fraction units that need the same rescale as `value`.
_MOLE_FRACTION_VARS = ("value_std_dev", "icos_LTR", "icos_SMR", "icos_STTB")

_FILENAME_RE = re.compile(
    r"^(?P<gas>co2|ch4|n2o|co)"
    r"_(?P<trigram>[a-z]{3})"
    r"_(?P<kind>[a-z\-]+)"
    r"_(?P<dataset_num>\d+)"
    r"_(?P<sel>[a-z0-9\-]+?)"
    r"(?:-(?P<height>\d+)magl)?"
    r"\.nc$"
)


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ObspackFileInfo:
    gas: str
    trigram: str          # 3-letter, lower-case
    kind: str             # "tower-insitu" / "surface-insitu"
    dataset_num: int
    height_magl: int | None  # None for surface stations

    @property
    def station_id(self) -> str:
        """Trigram (upper-case) + height-magl int.  Surface → height 0."""
        h = self.height_magl if self.height_magl is not None else 0
        return f"{self.trigram.upper()}{h}"


# ── Filename parser ───────────────────────────────────────────────────────────

def parse_filename(name: str) -> ObspackFileInfo:
    """Parse an Obspack filename like ``ch4_arn_tower-insitu_478_allvalid-10magl.nc``."""
    m = _FILENAME_RE.match(name)
    if not m:
        raise ValueError(f"unrecognised Obspack filename: {name}")
    height = int(m.group("height")) if m.group("height") else None
    return ObspackFileInfo(
        gas         = m.group("gas"),
        trigram     = m.group("trigram"),
        kind        = m.group("kind"),
        dataset_num = int(m.group("dataset_num")),
        height_magl = height,
    )


# ── Static-column promotion ──────────────────────────────────────────────────

def _is_static(arr: np.ndarray) -> bool:
    """Return True if all values along arr equal the first one (NaN-aware)."""
    if arr.size == 0:
        return True
    if arr.dtype.kind == "f":
        first = arr[0]
        if np.isnan(first):
            return bool(np.all(np.isnan(arr)))
        return bool(np.all(arr == first))
    return bool(np.all(arr == arr[0]))


def _scalar_from_array(arr: np.ndarray):
    """Return a JSON-serialisable scalar from arr[0]."""
    v = arr[0]
    if isinstance(v, (bytes, np.bytes_)):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, np.generic):
        return v.item()
    return v


# ── Ingest ────────────────────────────────────────────────────────────────────

def build_dataset(nc_path: Path) -> tuple[xr.Dataset, ObspackFileInfo, dict]:
    """
    Read one Obspack netCDF file and return (ds, info, group_attrs):

      ds            — xr.Dataset with `time` coord and time-varying vars only
      info          — parsed filename
      group_attrs   — dict suitable for the zarr group .zattrs (static
                      promotions, global attrs, _provenance contribution)
    """
    info = parse_filename(nc_path.name)

    # load() pulls all data into memory and closes the underlying file handle,
    # which lets the caller delete the .nc file on Windows after build_dataset
    # returns.
    with xr.open_dataset(nc_path) as _src:
        src = _src.load()

    # ── 1. Drop redundant time encodings ──────────────────────────────────
    drop = [v for v in _DROP_VARS if v in src.variables]
    src  = src.drop_vars(drop)

    # ── 2. Rename + rescale the measurement variable ──────────────────────
    scale, units = GAS_SCALE[info.gas]
    if "value" in src:
        src = src.rename({"value": info.gas})
        src[info.gas] = src[info.gas] * scale
        src[info.gas].attrs.update({
            "units":               units,
            "long_name":           f"{info.gas.upper()} dry-air mole fraction",
            "calibration_scale":   src.attrs.get("dataset_calibration_scale", ""),
        })

    # Rescale & rename other mole-fraction variables.
    for v in _MOLE_FRACTION_VARS:
        if v not in src:
            continue
        if src[v].attrs.get("units", "") != "mol mol-1":
            continue
        new_name = v.replace("value", info.gas)
        if new_name != v:
            src = src.rename({v: new_name})
        src[new_name] = src[new_name] * scale
        src[new_name].attrs["units"] = units

    # ── 2b. Namespace dim + all per-sample vars with the gas prefix ───────
    # Different gases at the same station have different time axes, so we
    # use per-gas dimensions (`time_co2`, `time_ch4`, …) and prefix every
    # data variable to avoid name collisions on merge.
    dim_new = f"time_{info.gas}"
    src = src.swap_dims({"time": "time"}).rename({"time": dim_new})

    rename_map: dict[str, str] = {}
    for v in list(src.data_vars):
        if v == info.gas or v.startswith(f"{info.gas}_"):
            continue
        rename_map[v] = f"{info.gas}_{v}"
    if rename_map:
        src = src.rename(rename_map)

    # ── 3. Promote static columns to attrs ────────────────────────────────
    # Candidates were renamed in step 2b — look for the prefixed names too.
    static_attrs: dict[str, object] = {}
    for cand in _STATIC_CANDIDATES:
        for v in (cand, f"{info.gas}_{cand}"):
            if v not in src:
                continue
            arr = src[v].values
            if _is_static(arr):
                # Strip the gas prefix when promoting to attrs — lat/lon/etc.
                # are station-level constants, not gas-specific.
                key = cand
                static_attrs[key] = _scalar_from_array(arr)
                src = src.drop_vars(v)
            break

    # ── 4. Collect + clean global attrs into group_attrs ──────────────────
    global_attrs = dict(src.attrs)
    src.attrs    = {}      # don't propagate to xarray default attrs

    group_attrs = {
        "site_code":         global_attrs.get("site_code", info.trigram.upper()),
        "site_name":         global_attrs.get("site_name", ""),
        "site_country":      global_attrs.get("site_country", ""),
        "site_url":          global_attrs.get("site_url", ""),
        "site_latitude":     global_attrs.get("site_latitude"),
        "site_longitude":    global_attrs.get("site_longitude"),
        "site_elevation":    global_attrs.get("site_elevation"),
        "Conventions":       global_attrs.get("Conventions", "CF-1.7"),
        # static promotions take precedence
        **static_attrs,
        # gas-specific block
        f"{info.gas}_dataset_num":          info.dataset_num,
        f"{info.gas}_dataset_name":         global_attrs.get("dataset_name", ""),
        f"{info.gas}_dataset_kind":         info.kind,
        f"{info.gas}_calibration_scale":    global_attrs.get("dataset_calibration_scale", ""),
        f"{info.gas}_data_frequency":       global_attrs.get("dataset_data_frequency"),
        f"{info.gas}_data_frequency_unit":  global_attrs.get("dataset_data_frequency_unit", ""),
        f"{info.gas}_provider_citations":   _collect_citations(global_attrs),
        f"{info.gas}_obspack_name":         global_attrs.get("obspack_name", ""),
        f"{info.gas}_obspack_citation":     global_attrs.get("obspack_citation", ""),
        f"{info.gas}_dataset_intake_ht":    global_attrs.get("dataset_intake_ht"),
        f"{info.gas}_start_date":           global_attrs.get("dataset_start_date", ""),
        f"{info.gas}_stop_date":            global_attrs.get("dataset_stop_date", ""),
    }
    # drop None values for cleanliness
    group_attrs = {k: v for k, v in group_attrs.items() if v is not None and v != ""}

    return src, info, group_attrs


def _collect_citations(attrs: dict) -> list[str]:
    """Pull dataset_provider_citation_1..N into a list."""
    out: list[str] = []
    for i in range(1, 50):
        key = f"dataset_provider_citation_{i}"
        if key in attrs:
            out.append(attrs[key])
        else:
            break
    return out


# ── Convenience ──────────────────────────────────────────────────────────────

def merge_attrs(existing: dict, new: dict) -> dict:
    """
    Merge new ingest attrs into existing group .zattrs.

    Station-level keys (lat/lon/elevation/intake_height, site_*) are written
    only if absent. Gas-specific keys (prefixed with the gas name) overwrite.
    """
    merged = dict(existing)
    for k, v in new.items():
        # Gas-specific keys: overwrite (e.g. re-ingest of co2 file)
        if any(k.startswith(g + "_") for g in GAS_SCALE):
            merged[k] = v
        elif k not in merged:
            merged[k] = v
    return merged


def summarise(ds: xr.Dataset, info: ObspackFileInfo, group_attrs: dict) -> str:
    """Format a one-screen summary of a parsed file (for smoke tests / logs)."""
    lines: list[str] = []
    lines.append(f"  station_id   : {info.station_id}")
    lines.append(f"  gas          : {info.gas}")
    lines.append(f"  kind         : {info.kind}")
    lines.append(f"  dataset_num  : {info.dataset_num}")
    lines.append(f"  height_magl  : {info.height_magl}")
    lines.append(f"  time samples : {ds.sizes.get('time', 0)}")
    lines.append(f"  time range   : {str(ds['time'].values[0])[:19]} → {str(ds['time'].values[-1])[:19]}")
    lines.append(f"  data vars    : {len(ds.data_vars)}")
    for v in ds.data_vars:
        da = ds[v]
        u  = da.attrs.get("units", "")
        lines.append(f"     {v:<25s} dims={da.dims} {da.shape} units={u!r}")
    lines.append(f"  group attrs  : {len(group_attrs)} keys")
    for k in ("site_code", "site_name", "site_latitude", "site_longitude",
              "intake_height", f"{info.gas}_calibration_scale",
              f"{info.gas}_provider_citations"):
        if k in group_attrs:
            v = group_attrs[k]
            if isinstance(v, list):
                v = f"[{len(v)} items]"
            elif isinstance(v, str) and len(v) > 80:
                v = v[:77] + "..."
            lines.append(f"     {k:<35s} {v}")
    return "\n".join(lines)
