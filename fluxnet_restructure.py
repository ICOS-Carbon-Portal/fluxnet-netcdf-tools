#!/usr/bin/env python3
"""
Restructure FLUXNET/ICOS CSV files into a CF-1.12 NetCDF4 with
multi-dimensional variables that reflect the ONEFlux processing logic
(Pastorello et al. 2020, Scientific Data 7:225).

Instead of one flat 1-D variable per FLUXNET column, logically related
columns are collapsed into N-D arrays along labelled coordinate dimensions:

  NEE   (time, ustar_threshold, nee_variant)
  NEE_QC / NEE_RANDUNC / NEE_JOINTUNC   – same dims
  GPP   (time, partition_method, ustar_threshold, nee_variant)
  GPP_QC  – same dims
  RECO  (time, partition_method, ustar_threshold, nee_variant)
  RECO_QC – same dims
  TS    (time, soil_layer)    – MDS gap-filled soil temperature
  TS_QC / SWC / SWC_QC       – same soil_layer dim
  LE_CORR (time, corr_pct)   – EBC-corrected latent heat (p25 / p50 / p75)
  H_CORR  (time, corr_pct)   – EBC-corrected sensible heat

All other columns (met variables, LE_F_MDS, RANDUNC scalars, etc.) are
written as ordinary 1-D time-series variables.

Temporal groups mirror icos_combined.py:
  /              merged half-hourly products (root, xarray-compatible)
  /fluxnet_dd    daily FLUXNET aggregation
  /fluxnet_ww    weekly
  /fluxnet_mm    monthly
  /fluxnet_yy    yearly

Usage:
    python fluxnet_restructure.py ICOSETC_SE-Svb_*.csv
    python fluxnet_restructure.py ICOSETC_SE-Svb_*.csv -o SE-Svb_restructured.nc

Dependencies:
    pip install numpy pandas netCDF4
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd

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
    fetch_column_instruments,
    fetch_doi_citation,
    fetch_icos_station_meta,
    parse_filename,
)

# ─────────────────────────────────────────────────────────────────────────────
# Dimension labels for the new coordinate axes
# ─────────────────────────────────────────────────────────────────────────────

USTAR_LABELS = ["CUT", "VUT"]
"""
USTAR threshold method applied before gap-filling:
  CUT – Constant USTAR Threshold (same value across all years)
  VUT – Variable USTAR Threshold (year-specific, influenced by neighbouring years)
See Pastorello et al. (2020) §USTAR threshold estimation.
"""

PARTITION_LABELS = ["NT", "DT"]
"""
CO2 flux partitioning method:
  NT – nighttime method (Reichstein et al. 2005)
  DT – daytime method   (Lasslop et al. 2010)
Use both; their difference is informative about methodological uncertainty.
"""

NEE_VARIANT_LABELS = [
    "REF",     # most representative instance (highest Nash-Sutcliffe sum)
    "USTAR50", # instance from median USTAR threshold
    "MEAN",    # arithmetic mean of the 40 USTAR-threshold instances
    "SE",      # standard error of the 40 instances
    "p05", "p16", "p25", "p50", "p75", "p84", "p95",  # percentiles
]
"""
Ensemble variants arising from the 40-USTAR-threshold bootstrapping.
Percentiles characterise the uncertainty due to USTAR threshold selection.
"""

CORR_PCT_LABELS = ["p25", "p50", "p75"]
"""
Percentiles of the energy-balance closure correction factor distribution:
  p25 – lower uncertainty bound  (source: LE_CORR_25 / H_CORR_25)
  p50 – median / recommended     (source: LE_CORR    / H_CORR)
  p75 – upper uncertainty bound  (source: LE_CORR_75 / H_CORR_75)
"""

# Fast index look-ups
_USTAR_IDX   = {s: i for i, s in enumerate(USTAR_LABELS)}
_PART_IDX    = {s: i for i, s in enumerate(PARTITION_LABELS)}
_VARIANT_IDX = {s: i for i, s in enumerate(NEE_VARIANT_LABELS)}
# bare percentile strings "05" … "95" also resolve to the p-prefixed entries
_VARIANT_IDX.update(
    {p: _VARIANT_IDX[f"p{p}"] for p in ["05", "16", "25", "50", "75", "84", "95"]}
)
_CORRPCT_IDX = {"CORR_25": 0, "CORR": 1, "CORR_75": 2}

# ── Column-classification regexes ─────────────────────────────────────────────

# NEE_{USTAR}_{VARIANT}[_{STAT}]
# DAY_RANDUNC / NIGHT_RANDUNC appear in aggregated products (DD/WW/MM/YY)
# and represent the random uncertainty split by daytime / nighttime periods.
_NEE_RE = re.compile(
    r"^NEE_(CUT|VUT)"
    r"_(REF|USTAR50|MEAN|SE|05|16|25|50|75|84|95)"
    r"(?:_(QC|RANDUNC|JOINTUNC|RANDUNC_METHOD|RANDUNC_N"
    r"|DAY_RANDUNC|NIGHT_RANDUNC))?$"
)

# {GPP|RECO}_{PART}_{USTAR}_{VARIANT}[_{STAT}]
_GPPECO_RE = re.compile(
    r"^(GPP|RECO)_(NT|DT)_(CUT|VUT)"
    r"_(REF|USTAR50|MEAN|SE|05|16|25|50|75|84|95)"
    r"(?:_(QC))?$"
)

# {TS|SWC}_F_MDS_{LAYER}[_{STAT}]
_SOIL_RE = re.compile(r"^(TS|SWC)_F_MDS_(\d+)(?:_(QC))?$")

# {LE|H}_{CORR | CORR_25 | CORR_75}   – only the three percentile columns
_ENERGY_CORR_RE = re.compile(r"^(LE|H)_(CORR(?:_25|_75)?)$")

# FLUXNET BADM triple-index: VARBASE_R_H_V[_N|_SE|_QC]
# R, H, V are integers (one or more digits; H can be 10, 11, … on tall towers)
_PROFILE_RE = re.compile(
    r"^(.+?)_(\d+)_(\d+)_(\d+)(?:_(N|SE|QC))?$"
)

# Single-integer-suffix variables: VARBASE_IDX[_N|_SE|_SD|_QC]
# Used for concentration gradients (CO2_DRY_7, H2O_8), fetch percentiles
# (FETCH_50), and METEO replicate measurements (G_1, SWC_2).
# Applied only after _PROFILE_RE columns are consumed, so triple-index
# columns can never reach this pattern.
_SINGLE_IDX_RE = re.compile(
    r"^(.+?)_(\d+)(?:_(N|SE|SD|QC))?$"
)

_TS_COLS = {"TIMESTAMP", "TIMESTAMP_START", "TIMESTAMP_END"}

# ── Fill-value constants ───────────────────────────────────────────────────────
_FV_F32 = np.float32(FILL_VALUE_OUT)
_FV_U8  = np.uint8(255)
_FV_I1  = np.int8(-1)
_FV_I2  = np.int16(-9999)
_FV_I4  = np.int32(-9999)

def _promote_count_arr(
    cnt: np.ndarray, fv_in: np.floating
) -> tuple[str, np.integer, np.ndarray]:
    """Promote a float32 sample-count array to the smallest integer dtype.

    Sample counts are always non-negative integers; we choose:
      i1 (fill -1)    when max ≤ 127
      u1 (fill 255)   when max ≤ 254
      i2 (fill -9999) when max ≤ 32 767
      i4 (fill -9999) otherwise
    """
    mask  = cnt == fv_in
    valid = cnt[~mask]
    vmax  = int(valid.max()) if valid.size else 0
    if vmax <= 127:
        dtype, fv = "i1", _FV_I1
    elif vmax <= 254:
        dtype, fv = "u1", _FV_U8
    elif vmax <= 32_767:
        dtype, fv = "i2", _FV_I2
    else:
        dtype, fv = "i4", _FV_I4
    safe = np.where(mask, 0, cnt)   # zero fill-slots before cast to avoid overflow
    out  = np.where(mask, fv, safe.astype(np.dtype(dtype)))
    return dtype, fv, out


_QC_FLAG_ATTRS: dict = dict(
    flag_values   = np.array([0, 1, 2, 3], dtype=np.uint8),
    flag_meanings = (
        "measured good_quality_fill medium_quality_fill poor_quality_fill"
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
# Low-level NetCDF4 helpers
# ─────────────────────────────────────────────────────────────────────────────

def _free_nc_name(grp: nc.Dataset, base: str) -> str:
    """Return *base* if unused in *grp*, else *base*_obs, _obs2, … ."""
    if base not in grp.variables:
        return base
    for suffix in ("_obs", "_obs2", "_obs3", "_raw"):
        name = base + suffix
        if name not in grp.variables:
            return name
    raise RuntimeError(f"Cannot find a free NC variable name for {base!r}")


def _ensure_dim(grp: nc.Dataset, name: str, size: int,
                labels: list[str] | None = None) -> None:
    """Create *name* dimension and optional string coordinate variable if absent."""
    if name in grp.dimensions:
        return
    grp.createDimension(name, size)
    if labels is not None:
        v = grp.createVariable(name, str, (name,))
        v.long_name = name.replace("_", " ")
        v[:] = np.array(labels, dtype=object)


def _col_to_f32(series: pd.Series) -> np.ndarray:
    """Convert a DataFrame column → float32, replacing FILL_VALUE_IN with _FV_F32."""
    arr  = series.to_numpy(dtype=np.float64, na_value=np.nan)
    mask = np.isnan(arr) | (arr == float(FILL_VALUE_IN))
    return np.where(mask, _FV_F32, arr.astype(np.float32))


def _col_to_qc_u8(series: pd.Series) -> np.ndarray:
    """Convert a QC column → uint8 (0–3); 255 = missing/not-applicable."""
    arr  = series.to_numpy(dtype=np.float64, na_value=np.nan)
    mask = np.isnan(arr) | (arr == float(FILL_VALUE_IN))
    # At coarser resolutions QC is a mean and may be fractional; round it.
    safe = np.where(mask, 0.0, arr)
    return np.where(mask, _FV_U8,
                    np.clip(np.round(safe), 0, 3).astype(np.uint8))


def _nc_var(grp: nc.Dataset, name: str, dtype: str,
            dims: tuple[str, ...], fv,
            long_name: str, units: str,
            **extra) -> nc.Variable:
    v = grp.createVariable(name, dtype, dims,
                           fill_value=fv, zlib=True, complevel=4)
    v.long_name     = long_name
    v.missing_value = fv
    v.units         = units
    for k, val in extra.items():
        setattr(v, k, val)
    return v


# ─────────────────────────────────────────────────────────────────────────────
# Multi-dimensional variable writers
# ─────────────────────────────────────────────────────────────────────────────

def _write_nee(grp: nc.Dataset, df: pd.DataFrame, cols: list[str]) -> None:
    """
    NEE(time, ustar_threshold, nee_variant)  and associated stat arrays.

    Source columns consumed (example):
      NEE_CUT_REF, NEE_CUT_REF_QC, NEE_CUT_REF_RANDUNC, NEE_CUT_REF_JOINTUNC,
      NEE_VUT_USTAR50, NEE_VUT_05, NEE_VUT_16, …  (all matched by _NEE_RE,
      except RANDUNC_METHOD / RANDUNC_N which stay as 1-D scalars)
    """
    _ensure_dim(grp, "ustar_threshold", len(USTAR_LABELS),      USTAR_LABELS)
    _ensure_dim(grp, "nee_variant",     len(NEE_VARIANT_LABELS), NEE_VARIANT_LABELS)

    n_t   = len(df)
    dims  = ("time", "ustar_threshold", "nee_variant")
    shape = (n_t, len(USTAR_LABELS), len(NEE_VARIANT_LABELS))

    val      = np.full(shape, _FV_F32, dtype=np.float32)
    qc       = np.full(shape, _FV_U8,  dtype=np.uint8)
    runc     = np.full(shape, _FV_F32, dtype=np.float32)
    junc     = np.full(shape, _FV_F32, dtype=np.float32)
    day_runc = np.full(shape, _FV_F32, dtype=np.float32)
    ngt_runc = np.full(shape, _FV_F32, dtype=np.float32)
    has_day_runc = has_ngt_runc = False

    for col in cols:
        m = _NEE_RE.match(col)
        if not m:
            continue
        ustar, variant, stat = m.groups()
        ui = _USTAR_IDX[ustar]
        vi = _VARIANT_IDX[variant]
        if   stat is None:            val[:, ui, vi]      = _col_to_f32(df[col])
        elif stat == "QC":            qc[:, ui, vi]       = _col_to_qc_u8(df[col])
        elif stat == "RANDUNC":       runc[:, ui, vi]     = _col_to_f32(df[col])
        elif stat == "JOINTUNC":      junc[:, ui, vi]     = _col_to_f32(df[col])
        elif stat == "DAY_RANDUNC":   day_runc[:, ui, vi] = _col_to_f32(df[col]); has_day_runc = True
        elif stat == "NIGHT_RANDUNC": ngt_runc[:, ui, vi] = _col_to_f32(df[col]); has_ngt_runc = True
        # RANDUNC_METHOD / RANDUNC_N are left for the 1-D fallback

    u = _get_units("NEE") or "umolCO2 m-2 s-1"
    coords = "ustar_threshold nee_variant"

    _nc_var(grp, "NEE", "f4", dims, _FV_F32,
            "Net Ecosystem Exchange", u,
            coordinates=coords,
            comment=(
                "ustar_threshold: CUT = constant, VUT = variable USTAR threshold. "
                "nee_variant: ensemble member — REF is most representative, "
                "USTAR50 uses median threshold, pXX are distribution percentiles."
            ))[:] = val

    _nc_var(grp, "NEE_QC", "u1", dims, _FV_U8,
            "NEE gap-fill quality flag", "1",
            coordinates=coords,
            comment="255 = missing or not applicable for this variant",
            **_QC_FLAG_ATTRS)[:] = qc

    _nc_var(grp, "NEE_RANDUNC", "f4", dims, _FV_F32,
            "NEE random uncertainty", u,
            coordinates=coords,
            comment=(
                "Estimated via Hollinger & Richardson (2005). "
                "_FillValue where not applicable (MEAN, SE, percentile variants)."
            ))[:] = runc

    _nc_var(grp, "NEE_JOINTUNC", "f4", dims, _FV_F32,
            "NEE joint uncertainty (USTAR ensemble + random)", u,
            coordinates=coords,
            comment=(
                "Quadratic combination of USTAR-threshold spread and random uncertainty. "
                "_FillValue where not applicable."
            ))[:] = junc

    if has_day_runc:
        _nc_var(grp, "NEE_DAY_RANDUNC", "f4", dims, _FV_F32,
                "NEE daytime random uncertainty", u,
                coordinates=coords,
                comment=(
                    "Random uncertainty for daytime half-hours only "
                    "(Hollinger & Richardson 2005). "
                    "Only REF and USTAR50 variants populated in aggregated products."
                ))[:] = day_runc

    if has_ngt_runc:
        _nc_var(grp, "NEE_NIGHT_RANDUNC", "f4", dims, _FV_F32,
                "NEE nighttime random uncertainty", u,
                coordinates=coords,
                comment=(
                    "Random uncertainty for nighttime half-hours only "
                    "(Hollinger & Richardson 2005). "
                    "Only REF and USTAR50 variants populated in aggregated products."
                ))[:] = ngt_runc


def _write_gppeco(grp: nc.Dataset, df: pd.DataFrame,
                  cols: list[str], base: str) -> None:
    """
    GPP or RECO(time, partition_method, ustar_threshold, nee_variant).

    partition_method: NT (nighttime, Reichstein 2005) or DT (daytime, Lasslop 2010).
    """
    _ensure_dim(grp, "partition_method", len(PARTITION_LABELS),  PARTITION_LABELS)
    _ensure_dim(grp, "ustar_threshold",  len(USTAR_LABELS),      USTAR_LABELS)
    _ensure_dim(grp, "nee_variant",      len(NEE_VARIANT_LABELS), NEE_VARIANT_LABELS)

    n_t   = len(df)
    dims  = ("time", "partition_method", "ustar_threshold", "nee_variant")
    shape = (n_t, len(PARTITION_LABELS), len(USTAR_LABELS), len(NEE_VARIANT_LABELS))

    val = np.full(shape, _FV_F32, dtype=np.float32)
    qc  = np.full(shape, _FV_U8,  dtype=np.uint8)

    for col in cols:
        m = _GPPECO_RE.match(col)
        if not m:
            continue
        _, part, ustar, variant, stat = m.groups()
        pi = _PART_IDX[part]
        ui = _USTAR_IDX[ustar]
        vi = _VARIANT_IDX[variant]
        if   stat is None:  val[:, pi, ui, vi] = _col_to_f32(df[col])
        elif stat == "QC":  qc[:, pi, ui, vi]  = _col_to_qc_u8(df[col])

    desc = {"GPP": "Gross Primary Production", "RECO": "Ecosystem Respiration"}
    u    = _get_units(base) or "umolCO2 m-2 s-1"

    _nc_var(grp, base, "f4", dims, _FV_F32, desc[base], u,
            coordinates="partition_method ustar_threshold nee_variant",
            comment=(
                "partition_method NT: nighttime method (Reichstein et al. 2005); "
                "DT: daytime method (Lasslop et al. 2010). "
                "Consider their difference as methodological uncertainty."
            ))[:] = val

    _nc_var(grp, f"{base}_QC", "u1", dims, _FV_U8,
            f"{desc[base]} gap-fill quality flag", "1",
            comment="255 = missing or not applicable",
            **_QC_FLAG_ATTRS)[:] = qc


def _write_soil(grp: nc.Dataset, df: pd.DataFrame,
                ts_cols: list[str], swc_cols: list[str]) -> None:
    """
    TS(time, soil_layer) and SWC(time, soil_layer).

    Both variables share the same soil_layer dimension, sized to the maximum
    layer index found across TS and SWC columns.  Positions with no data
    (e.g. SWC only has 5 layers while TS has 6) are filled with _FillValue.
    """
    all_cols = ts_cols + swc_cols
    if not all_cols:
        return

    # Determine the maximum layer index across both variables
    layer_indices = sorted({
        int(m.group(2))
        for c in all_cols
        if (m := _SOIL_RE.match(c))
    })
    n_layers = max(layer_indices)

    # Create shared dimension and integer coordinate (1-based)
    _ensure_dim(grp, "soil_layer", n_layers)
    if "soil_layer" not in grp.variables:
        lv = grp.createVariable("soil_layer", "i2", ("soil_layer",))
        lv.long_name = "soil layer index (1 = shallowest)"
        lv.units     = "1"
        lv.comment   = (
            "Layer depths are reported in the site BADM metadata "
            "(VAR_INFO_HEIGHT for TS and SWC)."
        )
        lv[:] = np.arange(1, n_layers + 1, dtype=np.int16)

    n_t   = len(df)
    dims  = ("time", "soil_layer")
    shape = (n_t, n_layers)

    desc  = {"TS": "Soil temperature (MDS gap-filled)",
             "SWC": "Soil water content (MDS gap-filled)"}
    units = {"TS": "degC", "SWC": "%"}

    for base, cols in (("TS", ts_cols), ("SWC", swc_cols)):
        if not cols:
            continue

        val = np.full(shape, _FV_F32, dtype=np.float32)
        qc  = np.full(shape, _FV_U8,  dtype=np.uint8)

        for col in cols:
            m = _SOIL_RE.match(col)
            if not m:
                continue
            _, layer_str, stat = m.groups()
            li = int(layer_str) - 1       # convert 1-based label to 0-based index
            if   stat is None:  val[:, li] = _col_to_f32(df[col])
            elif stat == "QC":  qc[:, li]  = _col_to_qc_u8(df[col])

        _nc_var(grp, base, "f4", dims, _FV_F32,
                desc[base], units[base],
                coordinates="soil_layer")[:] = val

        _nc_var(grp, f"{base}_QC", "u1", dims, _FV_U8,
                f"{desc[base]} gap-fill quality flag", "1",
                comment="255 = missing",
                **_QC_FLAG_ATTRS)[:] = qc


def _write_energy_corr(grp: nc.Dataset, df: pd.DataFrame,
                       cols: list[str], base: str) -> None:
    """
    LE_CORR or H_CORR(time, corr_pct) – energy-balance-corrected flux.

    The three percentiles capture uncertainty in the EBC correction factor
    (Foken 2008, Stoy et al. 2013):
      corr_pct = p25 → LE_CORR_25 (25th percentile, lower bound)
      corr_pct = p50 → LE_CORR    (median, recommended for analysis)
      corr_pct = p75 → LE_CORR_75 (75th percentile, upper bound)

    Other LE/H variables (LE_F_MDS, LE_RANDUNC, LE_CORR_JOINTUNC, …)
    are written as ordinary 1-D variables by the fallback path.
    """
    _ensure_dim(grp, "corr_pct", len(CORR_PCT_LABELS), CORR_PCT_LABELS)

    n_t   = len(df)
    dims  = ("time", "corr_pct")
    shape = (n_t, len(CORR_PCT_LABELS))
    arr   = np.full(shape, _FV_F32, dtype=np.float32)

    for col in cols:
        m = _ENERGY_CORR_RE.match(col)
        if not m:
            continue
        _, variant = m.groups()           # "CORR_25", "CORR", or "CORR_75"
        arr[:, _CORRPCT_IDX[variant]] = _col_to_f32(df[col])

    desc = {"LE": "Latent heat flux", "H": "Sensible heat flux"}
    _nc_var(grp, f"{base}_CORR", "f4", dims, _FV_F32,
            f"{desc[base]} (energy balance corrected)", "W m-2",
            coordinates="corr_pct",
            comment=(
                "Corrected using the Bowen-ratio assumption (Foken 2008). "
                "corr_pct = p50 (median) is the recommended value; "
                "p25 / p75 bound the uncertainty from the correction factor."
            ))[:] = arr


# ─────────────────────────────────────────────────────────────────────────────
# FLUXNET BADM triple-index profile writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_profile_vars(grp: nc.Dataset, df: pd.DataFrame,
                        cols: list[str],
                        column_instruments: dict | None = None) -> None:
    """
    Collapse FLUXNET BADM triple-index variables VARBASE_R_H_V[_N|_SE|_QC]
    into VARBASE(time, pos, height, vrep).

    R = horizontal position index  (sensor replica across space)
    H = height / depth index       (vertical position in canopy or soil)
    V = vertical replicate index   (repeated measurement at same height)

    Coordinate variables {nc_name}_r / _h / _v hold 1-based integer indices.
    If the base NC variable name is already taken (e.g. TS, SWC from the
    MDS gap-fill path), the variable is written as VARBASE_obs.
    """
    # ── Group columns by base name ──────────────────────────────────────────
    # entry = (col, R, H, V, stat_or_None)
    groups: dict[str, list[tuple[str, int, int, int, str | None]]] = {}
    for col in cols:
        m = _PROFILE_RE.match(col)
        if not m:
            continue
        base = m.group(1)
        r, h, v = int(m.group(2)), int(m.group(3)), int(m.group(4))
        stat  = m.group(5)          # None | "N" | "SE" | "QC"
        groups.setdefault(base, []).append((col, r, h, v, stat))

    for base, entries in groups.items():
        max_r = max(e[1] for e in entries)
        max_h = max(e[2] for e in entries)
        max_v = max(e[3] for e in entries)

        # Resolve NC variable name (avoid collision with existing var)
        nc_name = _free_nc_name(grp, base)

        # Variable-specific dimension names
        dim_r = f"{nc_name}_r"
        dim_h = f"{nc_name}_h"
        dim_v = f"{nc_name}_v"

        for dim, size, desc in (
            (dim_r, max_r, "horizontal position"),
            (dim_h, max_h, "height / depth"),
            (dim_v, max_v, "vertical replicate"),
        ):
            _ensure_dim(grp, dim, size)
            if dim not in grp.variables:
                cv = grp.createVariable(dim, "i2", (dim,))
                cv.long_name = f"{base} {desc} index (1 = first / shallowest)"
                cv.units     = "1"
                cv[:]        = np.arange(1, size + 1, dtype=np.int16)

        n_t   = len(df)
        dims  = ("time", dim_r, dim_h, dim_v)
        shape = (n_t, max_r, max_h, max_v)

        val = np.full(shape, _FV_F32, dtype=np.float32)
        cnt = np.full(shape, _FV_F32, dtype=np.float32)   # _N
        se  = np.full(shape, _FV_F32, dtype=np.float32)   # _SE
        qc  = np.full(shape, _FV_U8,  dtype=np.uint8)     # _QC
        has_n = has_se = has_qc = False

        for col, r, h, v, stat in entries:
            ri, hi, vi = r - 1, h - 1, v - 1
            if   stat is None:  val[:, ri, hi, vi] = _col_to_f32(df[col])
            elif stat == "N":   cnt[:, ri, hi, vi] = _col_to_f32(df[col]); has_n  = True
            elif stat == "SE":  se[:, ri, hi, vi]  = _col_to_f32(df[col]); has_se = True
            elif stat == "QC":  qc[:, ri, hi, vi]  = _col_to_qc_u8(df[col]); has_qc = True

        coords = f"{dim_r} {dim_h} {dim_v}"
        u  = _get_units(base) or "1"
        ln = _build_long_name(base)
        if nc_name != base:
            ln += " (observed multi-replicate)"

        _nc_var(grp, nc_name, "f4", dims, _FV_F32, ln, u,
                coordinates=coords)[:] = val

        if column_instruments:
            all_deps = []
            for col, r, h, v, stat in entries:
                if stat is not None:
                    continue
                for dep in column_instruments.get(col, []):
                    all_deps.append({"r": r, "h": h, "v": v, **dep})
            if all_deps:
                grp[nc_name].instrument_deployments = json.dumps(
                    all_deps, separators=(",", ":")
                )

        if has_n:
            cnt_dtype, cnt_fv, cnt_arr = _promote_count_arr(cnt, _FV_F32)
            _nc_var(grp, f"{nc_name}_N", cnt_dtype, dims, cnt_fv,
                    f"{ln} sample count", "1",
                    coordinates=coords)[:] = cnt_arr
        if has_se:
            _nc_var(grp, f"{nc_name}_SE", "f4", dims, _FV_F32,
                    f"{ln} standard error", u,
                    coordinates=coords)[:] = se
        if has_qc:
            _nc_var(grp, f"{nc_name}_QC", "u1", dims, _FV_U8,
                    f"{ln} quality flag", "1",
                    coordinates=coords,
                    **_QC_FLAG_ATTRS)[:] = qc


# ─────────────────────────────────────────────────────────────────────────────
# Single-index profile / gradient writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_single_idx_vars(grp: nc.Dataset, df: pd.DataFrame,
                           cols: list[str]) -> list[str]:
    """
    Collapse single-integer-suffix variables VARBASE_IDX[_N|_SE|_SD|_QC]
    into VARBASE(time, idx).

    The coordinate variable {nc_name}_level holds the actual integer index
    values found in the data (not necessarily 0-based or consecutive):
      CO2_DRY_7 … CO2_DRY_14  →  CO2_DRY(time, 8),  CO2_DRY_level = [7..14]
      FETCH_50, FETCH_70 …    →  FETCH(time, 4),     FETCH_level   = [50,70,80,90]
      G_1, G_2               →  G(time, 2),          G_level       = [1, 2]

    Only applied after _PROFILE_RE columns are consumed, so triple-index
    columns (which also match _SINGLE_IDX_RE) are never processed here.
    Groups with only a single column are kept as multi-dim (size-1 dim)
    only when accompanied by a stat variant (_N/_SE/_SD/_QC); otherwise
    they fall through to the 1-D fallback.
    """
    # ── Group by base name ───────────────────────────────────────────────────
    groups: dict[str, list[tuple[str, int, str | None]]] = {}
    for col in cols:
        m = _SINGLE_IDX_RE.match(col)
        if not m:
            continue
        base = m.group(1)
        idx  = int(m.group(2))
        stat = m.group(3)           # None | "N" | "SE" | "SD" | "QC"
        groups.setdefault(base, []).append((col, idx, stat))

    skipped: list[str] = []

    for base, entries in groups.items():
        # Require at least 2 distinct index values OR a value+stat pair at
        # any level to qualify as a true profile (avoids collapsing a lone
        # variable that happens to end in a digit).
        levels_with_val  = {idx for _, idx, st in entries if st is None}
        levels_with_stat = {idx for _, idx, st in entries if st is not None}
        distinct_levels  = levels_with_val | levels_with_stat

        if len(distinct_levels) < 2 and not (levels_with_val & levels_with_stat):
            # Only a single index with no companion stat → keep as 1-D
            skipped.extend(col for col, _, _ in entries)
            continue

        sorted_levels = sorted(distinct_levels)
        level_to_i    = {lvl: i for i, lvl in enumerate(sorted_levels)}
        n_levels      = len(sorted_levels)

        nc_name  = _free_nc_name(grp, base)
        dim_name = f"{nc_name}_level"
        _ensure_dim(grp, dim_name, n_levels)
        if dim_name not in grp.variables:
            lv = grp.createVariable(dim_name, "i4", (dim_name,))
            lv.long_name = f"{base} level index"
            lv.units     = "1"
            lv[:]        = np.array(sorted_levels, dtype=np.int32)

        n_t   = len(df)
        dims  = ("time", dim_name)
        shape = (n_t, n_levels)

        val = np.full(shape, _FV_F32, dtype=np.float32)
        cnt = np.full(shape, _FV_F32, dtype=np.float32)   # _N
        se  = np.full(shape, _FV_F32, dtype=np.float32)   # _SE / _SD
        qc  = np.full(shape, _FV_U8,  dtype=np.uint8)     # _QC
        has_n = has_se = has_qc = False
        se_label = "SE"

        for col, idx, stat in entries:
            li = level_to_i[idx]
            if   stat is None:         val[:, li] = _col_to_f32(df[col])
            elif stat == "N":          cnt[:, li] = _col_to_f32(df[col]); has_n  = True
            elif stat in ("SE", "SD"): se[:, li]  = _col_to_f32(df[col]); has_se = True; se_label = stat
            elif stat == "QC":         qc[:, li]  = _col_to_qc_u8(df[col]); has_qc = True

        u  = _get_units(base) or "1"
        ln = _build_long_name(base)
        if nc_name != base:
            ln += " (observed)"

        _nc_var(grp, nc_name, "f4", dims, _FV_F32, ln, u,
                coordinates=dim_name)[:] = val

        if has_n:
            cnt_dtype, cnt_fv, cnt_arr = _promote_count_arr(cnt, _FV_F32)
            _nc_var(grp, f"{nc_name}_N", cnt_dtype, dims, cnt_fv,
                    f"{ln} sample count", "1",
                    coordinates=dim_name)[:] = cnt_arr
        if has_se:
            _nc_var(grp, f"{nc_name}_{se_label}", "f4", dims, _FV_F32,
                    f"{ln} standard {'deviation' if se_label == 'SD' else 'error'}", u,
                    coordinates=dim_name)[:] = se
        if has_qc:
            _nc_var(grp, f"{nc_name}_QC", "u1", dims, _FV_U8,
                    f"{ln} quality flag", "1",
                    coordinates=dim_name,
                    **_QC_FLAG_ATTRS)[:] = qc

    # Return any skipped column names to the caller via side-channel
    # (they will remain in the consumed set as un-consumed → fall to 1-D)
    return skipped


# ─────────────────────────────────────────────────────────────────────────────
# Top-level multi-dim dispatcher
# ─────────────────────────────────────────────────────────────────────────────

def _write_multidim(grp: nc.Dataset, df: pd.DataFrame,
                    column_instruments: dict | None = None) -> set[str]:
    """
    Detect multi-dimensional FLUXNET variable families in *df*, write them
    to *grp*, and return the set of source column names that were consumed.
    Columns not in the returned set are handled by the 1-D fallback.
    """
    consumed: set[str] = set()
    data_cols = [c for c in df.columns if c not in _TS_COLS]

    # --- NEE: exclude RANDUNC_METHOD / RANDUNC_N (stay as 1-D) ---------------
    nee_cols = [
        c for c in data_cols
        if (m := _NEE_RE.match(c))
        and m.group(3) not in ("RANDUNC_METHOD", "RANDUNC_N")
    ]

    # --- GPP / RECO -----------------------------------------------------------
    gpp_cols  = [c for c in data_cols
                 if (m := _GPPECO_RE.match(c)) and m.group(1) == "GPP"]
    reco_cols = [c for c in data_cols
                 if (m := _GPPECO_RE.match(c)) and m.group(1) == "RECO"]

    # --- Soil profiles --------------------------------------------------------
    ts_cols  = [c for c in data_cols
                if (m := _SOIL_RE.match(c)) and m.group(1) == "TS"]
    swc_cols = [c for c in data_cols
                if (m := _SOIL_RE.match(c)) and m.group(1) == "SWC"]

    # --- Energy EBC correction -----------------------------------------------
    le_corr = [c for c in data_cols
               if (m := _ENERGY_CORR_RE.match(c)) and m.group(1) == "LE"]
    h_corr  = [c for c in data_cols
               if (m := _ENERGY_CORR_RE.match(c)) and m.group(1) == "H"]

    # --- Write ----------------------------------------------------------------
    if nee_cols:
        _write_nee(grp, df, nee_cols)
        consumed.update(nee_cols)

    for base, cols in (("GPP", gpp_cols), ("RECO", reco_cols)):
        if cols:
            _write_gppeco(grp, df, cols, base)
            consumed.update(cols)

    if ts_cols or swc_cols:
        _write_soil(grp, df, ts_cols, swc_cols)
        consumed.update(ts_cols + swc_cols)

    for base, cols in (("LE", le_corr), ("H", h_corr)):
        if cols:
            _write_energy_corr(grp, df, cols, base)
            consumed.update(cols)

    # --- BADM triple-index profiles (METEOSENS: VARBASE_R_H_V[_N|_SE|_QC]) --
    remaining = [c for c in data_cols if c not in consumed]
    profile_cols = [c for c in remaining if _PROFILE_RE.match(c)]
    if profile_cols:
        _write_profile_vars(grp, df, profile_cols, column_instruments)
        consumed.update(profile_cols)

    # --- Single-index gradients / replicates (FLUXES/METEO: VARBASE_IDX[…]) --
    remaining = [c for c in data_cols if c not in consumed]
    single_idx_cols = [c for c in remaining if _SINGLE_IDX_RE.match(c)]
    if single_idx_cols:
        not_collapsed = _write_single_idx_vars(grp, df, single_idx_cols)
        # consume all except those the function decided to leave as 1-D
        consumed.update(c for c in single_idx_cols if c not in not_collapsed)

    return consumed


# ─────────────────────────────────────────────────────────────────────────────
# 1-D fallback writer (handles anything not collapsed into N-D arrays)
# ─────────────────────────────────────────────────────────────────────────────

def _write_1d_vars(grp: nc.Dataset, df: pd.DataFrame,
                   skip: set[str]) -> set[str]:
    """
    Write all columns in *df* that are not in *skip* as ordinary (time,) variables.
    Returns the set of column names successfully written.
    """
    written: set[str] = set()
    skipped: list[str] = []

    for col in df.columns:
        if col in _TS_COLS or col in skip:
            continue

        is_qc = col.endswith("_QC") or col.endswith("_FLAG")
        raw   = df[col].values
        mask  = np.asarray(pd.isna(df[col]))

        if is_qc:
            safe = np.where(mask, 0.0, np.asarray(raw, dtype=np.float64))
            if np.any(safe != np.floor(safe)):
                dtype, fv = "f4", _FV_F32
                arr = np.where(mask, fv, safe.astype(np.float32))
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
            safe = np.where(mask, 0.0, f64)
            # Promote integer-valued float columns to a compact signed integer
            # NC type (e.g. RANDUNC_METHOD = 1|2 → i1,  RANDUNC_N ≤ 509 → i2)
            result = _choose_int_dtype(f64[~mask])
            if result is not None:
                dtype, fv = result
                arr = np.where(mask, fv, safe.astype(np.dtype(dtype)))
            else:
                dtype, fv = "f4", _FV_F32
                arr = np.where(mask, fv, f64.astype(np.float32))

        nc_name = col
        if nc_name in grp.variables:
            nc_name = col + "_raw"
            print(f"    Renaming {col!r} → {nc_name!r} "
                  f"(name taken by a multi-dim variable)")

        var = grp.createVariable(nc_name, dtype, ("time",),
                                 fill_value=fv, zlib=True, complevel=4)
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
                    "Each cipher encodes a specific check."
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


# ─────────────────────────────────────────────────────────────────────────────
# Group writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_group(
    root_ds:            nc.Dataset,
    grp_name:           str,
    df:                 pd.DataFrame,
    ts_start:           pd.DatetimeIndex,
    ts_end:             pd.DatetimeIndex | None,
    freq_code:          str,
    source_file:        str,
    skip_vars:          set[str],
    grp:                nc.Dataset | None = None,
    column_instruments: dict | None = None,
) -> set[str]:
    """
    Write one temporal group.  Returns the set of source column names written
    (for use as *skip_vars* in lower-priority products at the same resolution).

    Pass *grp=root_ds* to write directly into the root dataset (HH data).
    """
    if grp is None:
        grp = root_ds.createGroup(grp_name)
        for attr in _GLOBAL_ATTRS:
            val = getattr(root_ds, attr, None)
            if val is not None:
                setattr(grp, attr, val)

    grp.source              = source_file
    grp.temporal_resolution = _FREQ_ISO.get(freq_code, "unknown")
    grp.featureType         = "timeSeries"

    # ── Time coordinate ───────────────────────────────────────────────────────
    grp.createDimension("time", len(ts_start))
    epoch      = ts_start[0].replace(month=1, day=1, hour=0, minute=0,
                                      second=0, microsecond=0)
    time_units = f"minutes since {epoch.strftime('%Y-%m-%d %H:%M:%S')}"
    time_vals  = _to_cf_time(ts_start, epoch)

    tvar               = grp.createVariable("time", "f8", ("time",))
    tvar.standard_name = "time"
    tvar.long_name     = "time at start of averaging period"
    tvar.units         = time_units
    tvar.calendar      = "standard"
    tvar.axis          = "T"
    tvar[:]            = time_vals

    if ts_end is not None:
        grp.createDimension("nv", 2)
        tvar.bounds  = "time_bounds"
        end_vals     = _to_cf_time(ts_end, epoch)
        tbvar        = grp.createVariable("time_bounds", "f8", ("time", "nv"))
        tbvar.units  = time_units
        tbvar.calendar = "standard"
        tbvar[:, 0]  = time_vals
        tbvar[:, 1]  = end_vals

    # ── Multi-dimensional variable families ───────────────────────────────────
    # Work only on columns not already written by a higher-priority product
    available_df = df[[c for c in df.columns
                        if c in _TS_COLS or c not in skip_vars]]
    consumed = _write_multidim(grp, available_df, column_instruments)

    # ── 1-D fallback for remaining columns ────────────────────────────────────
    written_1d = _write_1d_vars(grp, available_df, skip=consumed)

    written = consumed | written_1d

    # Summarise
    n_nd = sum(1 for v in grp.variables
               if v not in ("time", "time_bounds")
               and len(grp[v].dimensions) > 1)
    n_1d = sum(1 for v in grp.variables
               if v not in ("time", "time_bounds")
               and len(grp[v].dimensions) == 1)
    print(f"      {n_nd:2d} multi-dim var(s)  [{len(consumed):3d} flat columns collapsed]"
          f"  +  {n_1d:3d} 1-D var(s)")

    return written


def _qc_norm(col: str) -> str:
    if col.endswith("_QC"):    return col[:-3]
    if col.endswith("_FLAG"):  return col[:-5]
    return col


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def restructure(csv_paths: list[Path], nc_path: Path,
                args: argparse.Namespace) -> None:

    sources: list[tuple[int, str, Path, pd.DataFrame, str]] = []
    site_id = args.site_id

    for csv_path in csv_paths:
        print(f"Reading  {csv_path.name}")
        df    = _read_csv(csv_path)
        finfo = parse_filename(csv_path)
        if not site_id:
            site_id = finfo.get("site_id", "unknown")

        if "TIMESTAMP_START" in df.columns:
            ts_col = "TIMESTAMP_START"
        elif "TIMESTAMP" in df.columns:
            ts_col = "TIMESTAMP"
        else:
            print(f"  WARNING: no timestamp column — skipping {csv_path.name}",
                  file=sys.stderr)
            continue

        ts_start  = _parse_timestamps(df[ts_col])
        freq_code = _detect_freq_code(csv_path) or _infer_freq_code(ts_start)
        sources.append((
            _product_priority(csv_path),
            _group_name(csv_path),
            csv_path, df, freq_code,
        ))

    if not sources:
        sys.exit("ERROR: no valid CSV files found.")

    sources.sort(key=lambda x: (x[4], x[0]))   # sort by (freq_code, priority)

    HH_CODES    = {"HH", "HR"}
    hh_sources  = [(p, g, path, df, fc) for p, g, path, df, fc in sources
                   if fc in HH_CODES]
    agg_sources = [(p, g, path, df, fc) for p, g, path, df, fc in sources
                   if fc not in HH_CODES]

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
        root_ds.title       = f"ICOS ETC L2 restructured data — site {site_id}"
        root_ds.institution = "ICOS Carbon Portal / FLUXNET"
        root_ds.site_id     = site_id
        root_ds.featureType = "timeSeries"
        root_ds.history     = (
            f"Created {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by fluxnet_restructure.py"
        )
        root_ds.references  = (
            "Pastorello et al. (2020) The FLUXNET2015 dataset and the ONEFlux "
            "processing pipeline for eddy covariance data. "
            "Scientific Data 7:225. https://doi.org/10.1038/s41597-020-0534-3"
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

        column_instruments: dict = getattr(args, "column_instruments", {}) or {}

        written_by_res: dict[str, set[str]] = {}

        # ── Half-hourly: merge all HH products, write to root ─────────────────
        if hh_sources:
            # Identify columns that appear in more than one HH product
            # (_QC and _FLAG are treated as the same variable for this check)
            norm_count:  dict[str, int] = {}
            col_to_norm: dict[str, str] = {}
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

            hh_freq_code = hh_sources[0][4]
            parsed: list[tuple[Path, pd.DataFrame, pd.DatetimeIndex]] = []
            for _, _, path, df, _ in hh_sources:
                src_ts = ("TIMESTAMP_START" if "TIMESTAMP_START" in df.columns
                          else "TIMESTAMP")
                parsed.append((path, df, _parse_timestamps(df[src_ts])))

            source_names = [p.name for p, _, _ in parsed]
            all_ts: pd.DatetimeIndex = parsed[0][2]
            for _, _, ts in parsed[1:]:
                all_ts = all_ts.union(ts)

            data_parts: list[pd.DataFrame] = []
            for _, df, ts_idx in parsed:
                unique_cols = [c for c in df.columns
                               if c not in _TS_COLS and c not in hh_dupes]
                if unique_cols:
                    part = df[unique_cols].copy()
                    part.index = ts_idx
                    data_parts.append(part.reindex(all_ts))
            merged = (pd.concat(data_parts, axis=1)
                      if data_parts else pd.DataFrame(index=all_ts))

            ts_start = all_ts
            ts_end: pd.DatetimeIndex | None = (
                pd.DatetimeIndex([t + _FREQ_OFFSET[hh_freq_code] for t in ts_start])
                if hh_freq_code in _FREQ_OFFSET else None
            )

            print(f"  /  (root)  [{hh_freq_code}]"
                  f"  ({len(hh_dupes)} duplicate column(s) removed)")
            written = _write_group(
                root_ds, "hh_merged", merged, ts_start, ts_end,
                hh_freq_code, ", ".join(source_names), set(),
                grp=root_ds,
                column_instruments=column_instruments,
            )
            written_by_res[hh_freq_code] = written

        # ── Aggregated products: one child group each ─────────────────────────
        for _priority, grp_name, csv_path, df, freq_code in agg_sources:
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
            n_dup = sum(1 for c in df.columns
                        if c not in _TS_COLS and c in skip_vars)
            dup_note = f"  ({n_dup} duplicate(s) skipped)" if n_dup else ""
            print(f"  /{grp_name:22s}  [{freq_code}]{dup_note}")

            written = _write_group(
                root_ds, grp_name, df, ts_start, ts_end,
                freq_code, csv_path.name, skip_vars,
                column_instruments=column_instruments,
            )
            written_by_res.setdefault(freq_code, set()).update(written)

    print(f"Done.    {nc_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Zarr / xarray dataset builders  (no netCDF4 dependency)
# ─────────────────────────────────────────────────────────────────────────────

def _col_to_f32_xr(series: pd.Series) -> np.ndarray:
    """Like _col_to_f32 but uses NaN for missing values (for xarray path)."""
    arr  = series.to_numpy(dtype=np.float64, na_value=np.nan)
    mask = np.isnan(arr) | (arr == float(FILL_VALUE_IN))
    return np.where(mask, np.nan, arr).astype(np.float32)


def _col_to_qc_xr(series: pd.Series) -> np.ndarray:
    """Like _col_to_qc_u8 but keeps 255 for missing (zarr path)."""
    arr  = series.to_numpy(dtype=np.float64, na_value=np.nan)
    mask = np.isnan(arr) | (arr == float(FILL_VALUE_IN))
    safe = np.where(mask, 0.0, arr)
    return np.where(mask, _FV_U8,
                    np.clip(np.round(safe), 0, 3).astype(np.uint8))


def _free_zarr_name(used: set[str], base: str) -> str:
    """Return *base* if not in *used*, else try _obs, _obs2, _obs3, _raw."""
    if base not in used:
        return base
    for suffix in ("_obs", "_obs2", "_obs3", "_raw"):
        name = base + suffix
        if name not in used:
            return name
    raise RuntimeError(f"Cannot find free zarr name for {base!r}")


def _build_nee_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex):
    """NEE(time, ustar_threshold, nee_variant) + associated arrays → xr.Dataset."""
    import xarray as xr

    n_t      = len(df)
    shape    = (n_t, len(USTAR_LABELS), len(NEE_VARIANT_LABELS))
    consumed: set[str] = set()

    val      = np.full(shape, np.nan,  dtype=np.float32)
    qc       = np.full(shape, _FV_U8,  dtype=np.uint8)
    runc     = np.full(shape, np.nan,  dtype=np.float32)
    junc     = np.full(shape, np.nan,  dtype=np.float32)
    day_runc = np.full(shape, np.nan,  dtype=np.float32)
    ngt_runc = np.full(shape, np.nan,  dtype=np.float32)
    has_day = has_ngt = False

    for col in df.columns:
        if col in _TS_COLS:
            continue
        m = _NEE_RE.match(col)
        if not m:
            continue
        ustar, variant, stat = m.groups()
        if stat in ("RANDUNC_METHOD", "RANDUNC_N"):
            continue
        ui = _USTAR_IDX[ustar]
        vi = _VARIANT_IDX[variant]
        consumed.add(col)
        if   stat is None:            val[:, ui, vi]      = _col_to_f32_xr(df[col])
        elif stat == "QC":            qc[:, ui, vi]       = _col_to_qc_xr(df[col])
        elif stat == "RANDUNC":       runc[:, ui, vi]     = _col_to_f32_xr(df[col])
        elif stat == "JOINTUNC":      junc[:, ui, vi]     = _col_to_f32_xr(df[col])
        elif stat == "DAY_RANDUNC":   day_runc[:, ui, vi] = _col_to_f32_xr(df[col]); has_day = True
        elif stat == "NIGHT_RANDUNC": ngt_runc[:, ui, vi] = _col_to_f32_xr(df[col]); has_ngt = True

    if not consumed:
        return xr.Dataset(), consumed

    u    = _get_units("NEE") or "umolCO2 m-2 s-1"
    dims = ["time", "ustar_threshold", "nee_variant"]
    coords = {
        "ustar_threshold": xr.DataArray(USTAR_LABELS,      dims=["ustar_threshold"],
                                        attrs={"long_name": "USTAR threshold method"}),
        "nee_variant":     xr.DataArray(NEE_VARIANT_LABELS, dims=["nee_variant"],
                                        attrs={"long_name": "NEE ensemble variant"}),
    }
    dv = {
        "NEE":         xr.DataArray(val,  dims=dims, attrs={
            "long_name": "Net Ecosystem Exchange", "units": u,
            "coordinates": "ustar_threshold nee_variant",
            "comment": ("ustar_threshold: CUT = constant, VUT = variable. "
                        "nee_variant: REF is most representative."),
        }),
        "NEE_QC":      xr.DataArray(qc,   dims=dims, attrs={
            "long_name": "NEE gap-fill quality flag", "units": "1",
            "coordinates": "ustar_threshold nee_variant",
            "comment": "255 = missing or not applicable",
            **_QC_FLAG_ATTRS,
        }),
        "NEE_RANDUNC": xr.DataArray(runc, dims=dims, attrs={
            "long_name": "NEE random uncertainty", "units": u,
            "coordinates": "ustar_threshold nee_variant",
        }),
        "NEE_JOINTUNC": xr.DataArray(junc, dims=dims, attrs={
            "long_name": "NEE joint uncertainty", "units": u,
            "coordinates": "ustar_threshold nee_variant",
        }),
    }
    if has_day:
        dv["NEE_DAY_RANDUNC"]   = xr.DataArray(day_runc, dims=dims, attrs={
            "long_name": "NEE daytime random uncertainty", "units": u,
            "coordinates": "ustar_threshold nee_variant",
        })
    if has_ngt:
        dv["NEE_NIGHT_RANDUNC"] = xr.DataArray(ngt_runc, dims=dims, attrs={
            "long_name": "NEE nighttime random uncertainty", "units": u,
            "coordinates": "ustar_threshold nee_variant",
        })
    return xr.Dataset(dv, coords=coords), consumed


def _build_gppeco_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex, base: str):
    """GPP or RECO(time, partition_method, ustar_threshold, nee_variant) → xr.Dataset."""
    import xarray as xr

    n_t      = len(df)
    shape    = (n_t, len(PARTITION_LABELS), len(USTAR_LABELS), len(NEE_VARIANT_LABELS))
    consumed: set[str] = set()

    val = np.full(shape, np.nan, dtype=np.float32)
    qc  = np.full(shape, _FV_U8, dtype=np.uint8)

    for col in df.columns:
        if col in _TS_COLS:
            continue
        m = _GPPECO_RE.match(col)
        if not m or m.group(1) != base:
            continue
        _, part, ustar, variant, stat = m.groups()
        pi = _PART_IDX[part]
        ui = _USTAR_IDX[ustar]
        vi = _VARIANT_IDX[variant]
        consumed.add(col)
        if   stat is None: val[:, pi, ui, vi] = _col_to_f32_xr(df[col])
        elif stat == "QC": qc[:, pi, ui, vi]  = _col_to_qc_xr(df[col])

    if not consumed:
        return xr.Dataset(), consumed

    desc = {"GPP": "Gross Primary Production", "RECO": "Ecosystem Respiration"}
    u    = _get_units(base) or "umolCO2 m-2 s-1"
    dims = ["time", "partition_method", "ustar_threshold", "nee_variant"]
    coords = {
        "partition_method": xr.DataArray(PARTITION_LABELS,  dims=["partition_method"],
                                         attrs={"long_name": "CO2 flux partitioning method"}),
        "ustar_threshold":  xr.DataArray(USTAR_LABELS,      dims=["ustar_threshold"],
                                         attrs={"long_name": "USTAR threshold method"}),
        "nee_variant":      xr.DataArray(NEE_VARIANT_LABELS, dims=["nee_variant"],
                                         attrs={"long_name": "NEE ensemble variant"}),
    }
    dv = {
        base: xr.DataArray(val, dims=dims, attrs={
            "long_name": desc[base], "units": u,
            "coordinates": "partition_method ustar_threshold nee_variant",
            "comment": ("NT: nighttime method (Reichstein 2005); "
                        "DT: daytime method (Lasslop 2010)."),
        }),
        f"{base}_QC": xr.DataArray(qc, dims=dims, attrs={
            "long_name": f"{desc[base]} gap-fill quality flag", "units": "1",
            "comment": "255 = missing or not applicable",
            **_QC_FLAG_ATTRS,
        }),
    }
    return xr.Dataset(dv, coords=coords), consumed


def _build_soil_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex):
    """TS and SWC(time, soil_layer) → xr.Dataset."""
    import xarray as xr

    consumed: set[str] = set()
    ts_cols  = [c for c in df.columns if (m := _SOIL_RE.match(c)) and m.group(1) == "TS"]
    swc_cols = [c for c in df.columns if (m := _SOIL_RE.match(c)) and m.group(1) == "SWC"]
    all_cols = ts_cols + swc_cols
    if not all_cols:
        return xr.Dataset(), consumed

    layer_indices = sorted({
        int(m.group(2))
        for c in all_cols
        if (m := _SOIL_RE.match(c))
    })
    n_layers = max(layer_indices)
    n_t      = len(df)
    shape    = (n_t, n_layers)
    dims     = ["time", "soil_layer"]

    coords = {
        "soil_layer": xr.DataArray(
            np.arange(1, n_layers + 1, dtype=np.int16), dims=["soil_layer"],
            attrs={"long_name": "soil layer index (1 = shallowest)", "units": "1",
                   "comment": "Layer depths in site BADM metadata (VAR_INFO_HEIGHT)."},
        ),
    }
    desc  = {"TS": "Soil temperature (MDS gap-filled)", "SWC": "Soil water content (MDS gap-filled)"}
    units = {"TS": "degC", "SWC": "%"}
    dv: dict = {}

    for base, cols in (("TS", ts_cols), ("SWC", swc_cols)):
        if not cols:
            continue
        val = np.full(shape, np.nan, dtype=np.float32)
        qc  = np.full(shape, _FV_U8, dtype=np.uint8)
        for col in cols:
            m = _SOIL_RE.match(col)
            if not m:
                continue
            li = int(m.group(2)) - 1
            if   m.group(3) is None: val[:, li] = _col_to_f32_xr(df[col])
            elif m.group(3) == "QC": qc[:, li]  = _col_to_qc_xr(df[col])
        consumed.update(cols)
        dv[base]          = xr.DataArray(val, dims=dims, attrs={
            "long_name": desc[base], "units": units[base], "coordinates": "soil_layer",
        })
        dv[f"{base}_QC"]  = xr.DataArray(qc,  dims=dims, attrs={
            "long_name": f"{desc[base]} gap-fill quality flag", "units": "1",
            "coordinates": "soil_layer", "comment": "255 = missing",
            **_QC_FLAG_ATTRS,
        })
    return xr.Dataset(dv, coords=coords), consumed


def _build_energy_corr_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex, base: str):
    """LE_CORR or H_CORR(time, corr_pct) → xr.Dataset."""
    import xarray as xr

    consumed: set[str] = set()
    n_t      = len(df)
    shape    = (n_t, len(CORR_PCT_LABELS))
    arr      = np.full(shape, np.nan, dtype=np.float32)

    for col in df.columns:
        if col in _TS_COLS:
            continue
        m = _ENERGY_CORR_RE.match(col)
        if not m or m.group(1) != base:
            continue
        arr[:, _CORRPCT_IDX[m.group(2)]] = _col_to_f32_xr(df[col])
        consumed.add(col)

    if not consumed:
        return xr.Dataset(), consumed

    desc = {"LE": "Latent heat flux", "H": "Sensible heat flux"}
    coords = {
        "corr_pct": xr.DataArray(CORR_PCT_LABELS, dims=["corr_pct"],
                                  attrs={"long_name": "EBC correction factor percentile"}),
    }
    dv = {
        f"{base}_CORR": xr.DataArray(arr, dims=["time", "corr_pct"], attrs={
            "long_name": f"{desc[base]} (energy balance corrected)", "units": "W m-2",
            "coordinates": "corr_pct",
            "comment": ("p50 (median) is recommended; p25/p75 bound the correction uncertainty."),
        }),
    }
    return xr.Dataset(dv, coords=coords), consumed


def _build_profile_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex,
                            used_names: set[str],
                            column_instruments: dict | None = None):
    """BADM triple-index VARBASE_R_H_V → VARBASE(time, r, h, v) xr.Dataset."""
    import xarray as xr

    consumed: set[str] = set()
    dv:     dict = {}
    coords: dict = {}

    groups: dict[str, list] = {}
    for col in df.columns:
        if col in _TS_COLS:
            continue
        m = _PROFILE_RE.match(col)
        if not m:
            continue
        base = m.group(1)
        r, h, v = int(m.group(2)), int(m.group(3)), int(m.group(4))
        stat = m.group(5)
        groups.setdefault(base, []).append((col, r, h, v, stat))

    for base, entries in groups.items():
        max_r = max(e[1] for e in entries)
        max_h = max(e[2] for e in entries)
        max_v = max(e[3] for e in entries)

        nc_name = _free_zarr_name(used_names, base)
        used_names.add(nc_name)
        consumed.update(col for col, *_ in entries)

        dim_r, dim_h, dim_v = f"{nc_name}_r", f"{nc_name}_h", f"{nc_name}_v"
        coords[dim_r] = xr.DataArray(np.arange(1, max_r + 1, dtype=np.int16), dims=[dim_r],
                                     attrs={"long_name": f"{base} horizontal position index", "units": "1"})
        coords[dim_h] = xr.DataArray(np.arange(1, max_h + 1, dtype=np.int16), dims=[dim_h],
                                     attrs={"long_name": f"{base} height/depth index", "units": "1"})
        coords[dim_v] = xr.DataArray(np.arange(1, max_v + 1, dtype=np.int16), dims=[dim_v],
                                     attrs={"long_name": f"{base} vertical replicate index", "units": "1"})

        n_t   = len(df)
        shape = (n_t, max_r, max_h, max_v)
        vdims = ["time", dim_r, dim_h, dim_v]

        val = np.full(shape, np.nan,  dtype=np.float32)
        cnt = np.full(shape, np.nan,  dtype=np.float32)
        se  = np.full(shape, np.nan,  dtype=np.float32)
        qc  = np.full(shape, _FV_U8,  dtype=np.uint8)
        has_n = has_se = has_qc = False

        for col, r, h, v, stat in entries:
            ri, hi, vi = r - 1, h - 1, v - 1
            if   stat is None: val[:, ri, hi, vi] = _col_to_f32_xr(df[col])
            elif stat == "N":  cnt[:, ri, hi, vi] = _col_to_f32_xr(df[col]); has_n  = True
            elif stat == "SE": se[:, ri, hi, vi]  = _col_to_f32_xr(df[col]); has_se = True
            elif stat == "QC": qc[:, ri, hi, vi]  = _col_to_qc_xr(df[col]);  has_qc = True

        u  = _get_units(base) or "1"
        ln = _build_long_name(base)
        if nc_name != base:
            ln += " (observed multi-replicate)"
        coord_str = f"{dim_r} {dim_h} {dim_v}"

        var_attrs: dict = {"long_name": ln, "units": u, "coordinates": coord_str}
        if column_instruments:
            all_deps = []
            for col, r, h, v, stat in entries:
                if stat is not None:
                    continue
                for dep in (column_instruments.get(col) or []):
                    all_deps.append({"r": r, "h": h, "v": v, **dep})
            if all_deps:
                var_attrs["instrument_deployments"] = json.dumps(all_deps, separators=(",", ":"))

        dv[nc_name] = xr.DataArray(val, dims=vdims, attrs=var_attrs)

        if has_n:
            cnt_f32 = np.where(np.isnan(cnt), _FV_F32, cnt.astype(np.float32))
            cnt_dtype, cnt_fv, cnt_arr = _promote_count_arr(cnt_f32, _FV_F32)
            dv[f"{nc_name}_N"] = xr.DataArray(cnt_arr, dims=vdims, attrs={
                "long_name": f"{ln} sample count", "units": "1", "coordinates": coord_str,
            })
        if has_se:
            dv[f"{nc_name}_SE"] = xr.DataArray(se, dims=vdims, attrs={
                "long_name": f"{ln} standard error", "units": u, "coordinates": coord_str,
            })
        if has_qc:
            dv[f"{nc_name}_QC"] = xr.DataArray(qc, dims=vdims, attrs={
                "long_name": f"{ln} quality flag", "units": "1", "coordinates": coord_str,
                **_QC_FLAG_ATTRS,
            })

    return xr.Dataset(dv, coords=coords), consumed


def _build_single_idx_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex,
                               used_names: set[str]):
    """Single-integer-suffix VARBASE_IDX → VARBASE(time, level) xr.Dataset."""
    import xarray as xr

    consumed: set[str] = set()
    not_collapsed: list[str] = []
    dv:     dict = {}
    coords: dict = {}

    groups: dict[str, list] = {}
    for col in df.columns:
        if col in _TS_COLS:
            continue
        m = _SINGLE_IDX_RE.match(col)
        if not m:
            continue
        base = m.group(1)
        idx  = int(m.group(2))
        stat = m.group(3)
        groups.setdefault(base, []).append((col, idx, stat))

    for base, entries in groups.items():
        levels_with_val  = {idx for _, idx, st in entries if st is None}
        levels_with_stat = {idx for _, idx, st in entries if st is not None}
        distinct_levels  = levels_with_val | levels_with_stat

        if len(distinct_levels) < 2 and not (levels_with_val & levels_with_stat):
            not_collapsed.extend(col for col, _, _ in entries)
            continue

        sorted_levels = sorted(distinct_levels)
        level_to_i    = {lvl: i for i, lvl in enumerate(sorted_levels)}
        n_levels      = len(sorted_levels)

        nc_name  = _free_zarr_name(used_names, base)
        used_names.add(nc_name)
        consumed.update(col for col, _, _ in entries)

        dim_name = f"{nc_name}_level"
        coords[dim_name] = xr.DataArray(
            np.array(sorted_levels, dtype=np.int32), dims=[dim_name],
            attrs={"long_name": f"{base} level index", "units": "1"},
        )

        n_t   = len(df)
        shape = (n_t, n_levels)
        vdims = ["time", dim_name]

        val = np.full(shape, np.nan,  dtype=np.float32)
        cnt = np.full(shape, np.nan,  dtype=np.float32)
        se  = np.full(shape, np.nan,  dtype=np.float32)
        qc  = np.full(shape, _FV_U8,  dtype=np.uint8)
        has_n = has_se = has_qc = False
        se_label = "SE"

        for col, idx, stat in entries:
            li = level_to_i[idx]
            if   stat is None:         val[:, li] = _col_to_f32_xr(df[col])
            elif stat == "N":          cnt[:, li] = _col_to_f32_xr(df[col]); has_n  = True
            elif stat in ("SE", "SD"): se[:, li]  = _col_to_f32_xr(df[col]); has_se = True; se_label = stat
            elif stat == "QC":         qc[:, li]  = _col_to_qc_xr(df[col]);  has_qc = True

        u  = _get_units(base) or "1"
        ln = _build_long_name(base)
        if nc_name != base:
            ln += " (observed)"

        dv[nc_name] = xr.DataArray(val, dims=vdims, attrs={
            "long_name": ln, "units": u, "coordinates": dim_name,
        })
        if has_n:
            cnt_f32 = np.where(np.isnan(cnt), _FV_F32, cnt.astype(np.float32))
            cnt_dtype, cnt_fv, cnt_arr = _promote_count_arr(cnt_f32, _FV_F32)
            dv[f"{nc_name}_N"] = xr.DataArray(cnt_arr, dims=vdims, attrs={
                "long_name": f"{ln} sample count", "units": "1", "coordinates": dim_name,
            })
        if has_se:
            dv[f"{nc_name}_{se_label}"] = xr.DataArray(se, dims=vdims, attrs={
                "long_name": f"{ln} standard {'deviation' if se_label == 'SD' else 'error'}",
                "units": u, "coordinates": dim_name,
            })
        if has_qc:
            dv[f"{nc_name}_QC"] = xr.DataArray(qc, dims=vdims, attrs={
                "long_name": f"{ln} quality flag", "units": "1", "coordinates": dim_name,
                **_QC_FLAG_ATTRS,
            })

    return xr.Dataset(dv, coords=coords), consumed, not_collapsed


def _build_1d_dataset(df: pd.DataFrame, ts_start: pd.DatetimeIndex,
                       ts_end: pd.DatetimeIndex | None, freq_code: str,
                       skip: set[str], used_names: set[str]) -> "xr.Dataset":
    """Build 1-D (time,) xarray Dataset for all columns not in *skip*."""
    import xarray as xr

    # ── Time coordinate ───────────────────────────────────────────────────────
    time_attrs: dict = {
        "standard_name": "time",
        "long_name":     "time at start of averaging period",
        "axis":          "T",
    }
    if ts_end is not None:
        time_attrs["bounds"] = "time_bounds"

    coords: dict = {"time": xr.DataArray(ts_start, dims=["time"], attrs=time_attrs)}
    dv:     dict = {}

    if ts_end is not None:
        tb = np.stack([ts_start.values, ts_end.values], axis=1)  # (n, 2) datetime64
        dv["time_bounds"] = xr.DataArray(tb, dims=["time", "nv"])

    dv["temporal_resolution"] = xr.DataArray(
        _FREQ_ISO.get(freq_code, "unknown"),
        attrs={"long_name": "ISO 8601 duration of each time step"},
    )

    skipped: list[str] = []
    for col in df.columns:
        if col in _TS_COLS or col in skip:
            continue

        is_qc = col.endswith("_QC") or col.endswith("_FLAG")
        raw   = df[col].values
        mask  = np.asarray(pd.isna(df[col]))

        if is_qc:
            safe = np.where(mask, 0.0, np.asarray(raw, dtype=np.float64))
            if np.any(safe != np.floor(safe)):
                arr = np.where(mask, np.nan, safe).astype(np.float32)
                dtype = "f4"
            else:
                vmax = int(safe.max()) if safe.size else 0
                if vmax <= 254:
                    arr = np.where(mask, _FV_U8, safe.astype(np.uint8))
                    dtype = "u1"
                elif vmax <= 2_147_483_647:
                    arr = np.where(mask, np.int32(-1), safe.astype(np.int32))
                    dtype = "i4"
                else:
                    arr = np.where(mask, np.int64(-1), safe.astype(np.int64))
                    dtype = "i8"
        else:
            try:
                f64 = np.asarray(raw, dtype=np.float64)
            except (ValueError, TypeError):
                skipped.append(col)
                continue
            safe   = np.where(mask, 0.0, f64)
            result = _choose_int_dtype(f64[~mask])
            if result is not None:
                nc_dtype, fv_int = result
                arr   = np.where(mask, fv_int, safe.astype(np.dtype(nc_dtype)))
                dtype = nc_dtype
            else:
                arr   = np.where(mask, np.nan, f64).astype(np.float32)
                dtype = "f4"

        nc_name = col
        if nc_name in used_names:
            nc_name = col + "_raw"
            print(f"    Renaming {col!r} → {nc_name!r} (name taken by multi-dim variable)")
        used_names.add(nc_name)

        var_attrs: dict = {"long_name": _build_long_name(col, is_qc=is_qc)}

        if is_qc:
            var_attrs["units"] = "1"
            if dtype == "u1":
                var_attrs.update(_QC_FLAG_ATTRS)
            elif dtype in ("i4", "i8"):
                var_attrs["comment"] = (
                    "12-digit composite quality flag (METEOSENS). "
                    "Each cipher encodes a specific check."
                )
        else:
            var_attrs["units"] = _get_units(col)
            sn = _get_standard_name(col)
            if sn:
                var_attrs["standard_name"] = sn

        dv[nc_name] = xr.DataArray(arr, dims=["time"], attrs=var_attrs)

    if skipped:
        print(f"    Skipped {len(skipped)} non-numeric column(s): "
              f"{', '.join(skipped[:5])}{'…' if len(skipped) > 5 else ''}")

    return xr.Dataset(dv, coords=coords)


def _encoding_for(ds) -> dict:
    """Return a per-variable encoding dict for *ds* (fill values + dtypes)."""
    enc: dict = {}
    for vname, da in ds.data_vars.items():
        if da.dtype in (np.float32, np.float64):
            enc[vname] = {"_FillValue": float(FILL_VALUE_OUT), "dtype": "float32"}
        elif da.dtype == np.uint8:
            enc[vname] = {"_FillValue": int(_FV_U8), "dtype": "uint8"}
        elif da.dtype == np.int8:
            enc[vname] = {"_FillValue": -1}
        elif da.dtype in (np.int16, np.int32, np.int64):
            enc[vname] = {"_FillValue": -9999}
    return enc


def _write_group_to_zarr(
    store_path: str,
    group_path: str,
    df: pd.DataFrame,
    ts_start: pd.DatetimeIndex,
    ts_end: pd.DatetimeIndex | None,
    freq_code: str,
    global_attrs: dict,
    column_instruments: dict,
) -> None:
    """Build datasets from *df* and write them incrementally to zarr at *group_path*.

    Each variable family is written as soon as it is built — no xr.merge() over
    all variables — keeping peak memory low and avoiding the O(n²) merge cost.
    """
    import xarray as xr

    used_names: set[str] = set()
    consumed:   set[str] = set()
    first      = True   # first write uses mode="w" (creates group + global attrs)
    n_nd = n_1d = 0

    def _flush(ds: "xr.Dataset") -> None:
        """Write *ds* to the zarr group; first call creates it, rest append vars."""
        nonlocal first
        if not ds.data_vars:
            return
        ds.attrs.update(global_attrs if first else {})
        ds.to_zarr(store_path, group=group_path,
                   mode="w" if first else "a",
                   encoding=_encoding_for(ds))
        first = False

    # ── Multi-dimensional families ────────────────────────────────────────────
    nee_ds, c = _build_nee_dataset(df, ts_start)
    consumed |= c; used_names.update(nee_ds.data_vars)
    n_nd += len(nee_ds.data_vars); _flush(nee_ds); del nee_ds

    for _base in ("GPP", "RECO"):
        ds, c = _build_gppeco_dataset(df, ts_start, _base)
        consumed |= c; used_names.update(ds.data_vars)
        n_nd += len(ds.data_vars); _flush(ds); del ds

    for _base in ("LE", "H"):
        ds, c = _build_energy_corr_dataset(df, ts_start, _base)
        consumed |= c; used_names.update(ds.data_vars)
        n_nd += len(ds.data_vars); _flush(ds); del ds

    soil_ds, c = _build_soil_dataset(df, ts_start)
    consumed |= c; used_names.update(soil_ds.data_vars)
    n_nd += len(soil_ds.data_vars); _flush(soil_ds); del soil_ds

    # Profile (METEOSENS triple-index) — only from columns not already consumed
    remaining_df = df[[col for col in df.columns if col in _TS_COLS or col not in consumed]]
    prof_ds, c = _build_profile_dataset(remaining_df, ts_start, used_names, column_instruments)
    consumed |= c; used_names.update(prof_ds.data_vars)
    n_nd += len(prof_ds.data_vars); _flush(prof_ds); del prof_ds

    # Single-index gradients
    remaining_df = df[[col for col in df.columns if col in _TS_COLS or col not in consumed]]
    sidx_ds, c, _ = _build_single_idx_dataset(remaining_df, ts_start, used_names)
    consumed |= c; used_names.update(sidx_ds.data_vars)
    n_nd += len(sidx_ds.data_vars); _flush(sidx_ds); del sidx_ds

    # ── 1-D fallback ──────────────────────────────────────────────────────────
    ds_1d = _build_1d_dataset(df, ts_start, ts_end, freq_code, consumed, used_names)
    n_1d = len(ds_1d.data_vars); _flush(ds_1d); del ds_1d

    print(f"      {n_nd:2d} multi-dim var(s)  [{len(consumed):3d} flat columns collapsed]"
          f"  +  {n_1d:3d} 1-D var(s)")


def restructure_to_zarr(
    csv_paths: list[Path],
    store_path: str,
    zarr_group: str,
    args: argparse.Namespace,
) -> None:
    """Restructure ICOS/FLUXNET CSV files and write directly to a zarr store.

    Mirrors ``restructure()`` exactly for CSV reading and data preparation,
    but writes xarray Datasets to zarr instead of NetCDF4 — no intermediate
    .nc file is created.

    Parameters
    ----------
    csv_paths  : list of CSV file paths (all products for one station)
    store_path : path to the zarr directory store (e.g. "icos-fluxnet.zarr")
    zarr_group : top-level group name in the store (e.g. "SE-Svb")
    args       : Namespace with site_id, doi_url, dobj_citation,
                 column_instruments, comment
    """
    sources: list[tuple[int, str, Path, pd.DataFrame, str]] = []
    site_id = args.site_id

    for csv_path in csv_paths:
        print(f"Reading  {csv_path.name}")
        df    = _read_csv(csv_path)
        finfo = parse_filename(csv_path)
        if not site_id:
            site_id = finfo.get("site_id", "unknown")

        if "TIMESTAMP_START" in df.columns:
            ts_col = "TIMESTAMP_START"
        elif "TIMESTAMP" in df.columns:
            ts_col = "TIMESTAMP"
        else:
            print(f"  WARNING: no timestamp column — skipping {csv_path.name}",
                  file=sys.stderr)
            continue

        ts_start  = _parse_timestamps(df[ts_col])
        freq_code = _detect_freq_code(csv_path) or _infer_freq_code(ts_start)
        sources.append((
            _product_priority(csv_path),
            _group_name(csv_path),
            csv_path, df, freq_code,
        ))

    if not sources:
        sys.exit("ERROR: no valid CSV files found.")

    sources.sort(key=lambda x: (x[4], x[0]))

    HH_CODES    = {"HH", "HR"}
    hh_sources  = [(p, g, path, df, fc) for p, g, path, df, fc in sources if fc in HH_CODES]
    agg_sources = [(p, g, path, df, fc) for p, g, path, df, fc in sources if fc not in HH_CODES]

    print(f"Fetching ICOS station metadata for {site_id} …")
    station_meta = fetch_icos_station_meta(site_id)

    doi_url      = getattr(args, "doi_url", "") or ""
    doi_citation = getattr(args, "doi_citation", "") or ""
    if not doi_url and getattr(args, "doi", None):
        print(f"Fetching APA citation for DOI {args.doi} …")
        doi_url, doi_citation = fetch_doi_citation(args.doi)

    dobj_citation      = getattr(args, "dobj_citation", "") or ""
    column_instruments = getattr(args, "column_instruments", {}) or {}

    # ── Build global attributes dict ─────────────────────────────────────────
    global_attrs: dict = {
        "Conventions": "CF-1.12",
        "title":       f"ICOS ETC L2 restructured data — site {site_id}",
        "institution": "ICOS Carbon Portal / FLUXNET",
        "site_id":     site_id,
        "featureType": "timeSeries",
        "history":     (
            f"Created {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by fluxnet2zarr.py"
        ),
        "references": (
            "Pastorello et al. (2020) The FLUXNET2015 dataset and the ONEFlux "
            "processing pipeline for eddy covariance data. "
            "Scientific Data 7:225. https://doi.org/10.1038/s41597-020-0534-3"
        ),
    }
    if getattr(args, "comment", ""):
        global_attrs["comment"] = args.comment
    global_attrs.update({k: str(v) for k, v in station_meta.items()})
    if doi_url:
        global_attrs["source_doi"]    = doi_url
    if doi_citation:
        global_attrs["PartOfDataset"] = doi_citation
    if dobj_citation:
        global_attrs["citation"]      = dobj_citation

    written_by_res: dict[str, set[str]] = {}

    # ── Half-hourly: merge all HH products, write to root zarr group ─────────
    if hh_sources:
        norm_count:  dict[str, int] = {}
        col_to_norm: dict[str, str] = {}
        for _, _, _, df, _ in hh_sources:
            seen: set[str] = set()
            for col in df.columns:
                if col not in _TS_COLS:
                    norm = _qc_norm(col)
                    col_to_norm[col] = norm
                    if norm not in seen:
                        norm_count[norm] = norm_count.get(norm, 0) + 1
                        seen.add(norm)

        hh_dupes    = {col for col, norm in col_to_norm.items()
                       if norm_count.get(norm, 0) > 1}
        hh_freq_code = hh_sources[0][4]

        parsed: list[tuple[Path, pd.DataFrame, pd.DatetimeIndex]] = []
        for _, _, path, df, _ in hh_sources:
            src_ts = ("TIMESTAMP_START" if "TIMESTAMP_START" in df.columns else "TIMESTAMP")
            parsed.append((path, df, _parse_timestamps(df[src_ts])))

        all_ts: pd.DatetimeIndex = parsed[0][2]
        for _, _, ts in parsed[1:]:
            all_ts = all_ts.union(ts)

        data_parts: list[pd.DataFrame] = []
        for _, df, ts_idx in parsed:
            unique_cols = [c for c in df.columns
                           if c not in _TS_COLS and c not in hh_dupes]
            if unique_cols:
                part       = df[unique_cols].copy()
                part.index = ts_idx
                data_parts.append(part.reindex(all_ts))
        merged = (pd.concat(data_parts, axis=1) if data_parts
                  else pd.DataFrame(index=all_ts))

        ts_start = all_ts
        ts_end_hh: pd.DatetimeIndex | None = (
            pd.DatetimeIndex([t + _FREQ_OFFSET[hh_freq_code] for t in ts_start])
            if hh_freq_code in _FREQ_OFFSET else None
        )

        source_names = [p.name for p, _, _ in parsed]
        grp_attrs    = {**global_attrs, "source": ", ".join(source_names)}
        print(f"  /  (root)  [{hh_freq_code}]  ({len(hh_dupes)} duplicate column(s) removed)")
        _write_group_to_zarr(store_path, zarr_group, merged, ts_start, ts_end_hh,
                             hh_freq_code, grp_attrs, column_instruments)
        written_by_res[hh_freq_code] = set(merged.columns) - _TS_COLS

    # ── Aggregated products: one child zarr group each ────────────────────────
    for _priority, grp_name, csv_path, df, freq_code in agg_sources:
        if "TIMESTAMP_START" in df.columns:
            ts_start = _parse_timestamps(df["TIMESTAMP_START"])
            ts_end   = (_parse_timestamps(df["TIMESTAMP_END"])
                        if "TIMESTAMP_END" in df.columns else None)
        else:
            ts_start = _parse_timestamps(df["TIMESTAMP"])
            ts_end   = None

        if ts_end is None and freq_code in _FREQ_OFFSET:
            ts_end = pd.DatetimeIndex([t + _FREQ_OFFSET[freq_code] for t in ts_start])

        skip_vars = written_by_res.get(freq_code, set())
        n_dup     = sum(1 for c in df.columns if c not in _TS_COLS and c in skip_vars)
        dup_note  = f"  ({n_dup} duplicate(s) skipped)" if n_dup else ""
        print(f"  /{grp_name:22s}  [{freq_code}]{dup_note}")

        # Build a filtered DataFrame excluding already-written columns
        avail_cols = [c for c in df.columns if c in _TS_COLS or c not in skip_vars]
        avail_df   = df[avail_cols]

        grp_attrs = {**global_attrs, "source": csv_path.name}
        _write_group_to_zarr(store_path, f"{zarr_group}/{grp_name}", avail_df,
                             ts_start, ts_end, freq_code, grp_attrs, {})

        written_cols = set(avail_df.columns) - _TS_COLS
        written_by_res.setdefault(freq_code, set()).update(written_cols)

    print(f"Done →   zarr://{store_path}/{zarr_group}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Restructure ICOS/FLUXNET CSV files into a CF-1.12 NetCDF4 "
            "with multi-dimensional NEE / GPP / RECO / TS / SWC / LE / H arrays "
            "reflecting the ONEFlux processing pipeline."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "csv", nargs="+",
        help="Input CSV files (glob patterns accepted, e.g. ICOSETC_SE-Svb_*.csv)",
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=None, metavar="NC",
        help=(
            "Output NetCDF file "
            "(default: ICOSETC_<site_id>_restructured.nc beside the first input)"
        ),
    )
    parser.add_argument(
        "--site-id", default="",
        help="Override site ID (auto-detected from the ICOS filename convention)",
    )
    parser.add_argument(
        "--comment", default="",
        help="Free-text comment appended as a global attribute",
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

    # Expand glob patterns (needed on Windows where the shell does not do it)
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
        interim = "_INTERIM" if any("INTERIM" in p.stem.upper()
                                    for p in csv_paths) else ""
        nc_path = (csv_paths[0].parent
                   / f"ICOSETC_{site}{interim}_restructured.nc")

    restructure(csv_paths, nc_path, args)


if __name__ == "__main__":
    main()
