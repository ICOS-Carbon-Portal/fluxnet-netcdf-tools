#!/usr/bin/env python3
"""
Convert ICOS FLUXNET CSV files to CF-1.12-compliant NetCDF4.

Supports FLUXNET2015 FULLSET/SUBSET products (half-hourly and hourly).
Auto-detects site metadata from the standard ICOS filename convention:
  FLX_<SITE-ID>_FLUXNET2015_<SUBSET>_<FREQ>_<YYYY>-<YYYY>_<version>.csv

Usage:
    python fluxnet2nc.py FLX_DE-Hai_FLUXNET2015_FULLSET_HH_1999-2019_2-3.csv
    python fluxnet2nc.py input.csv -o output.nc --comment "My site data"

Dependencies:
    pip install numpy pandas netCDF4
"""

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd


# ── Fill values ──────────────────────────────────────────────────────────────
FILL_VALUE_IN = -9999           # FLUXNET missing data flag
FILL_VALUE_OUT = np.float32(9.96921e+36)   # standard NetCDF _FillValue

# Per-process caches so sibling DD/WW/MM/YY files don't re-fetch
_station_meta_cache: dict[str, dict] = {}
_doi_citation_cache: dict[str, tuple[str, str]] = {}
_dobj_citation_cache: dict[str, tuple[str, str]] = {}
_column_instruments_cache: dict[str, dict[str, list[dict]]] = {}

# ── Variable metadata: root name → (CF standard_name | None, long_name, units)
VAR_META: dict[str, tuple] = {
    # Carbon / greenhouse gas fluxes
    "NEE":       (None,
                  "Net Ecosystem CO2 Exchange",
                  "umol m-2 s-1"),
    "GPP":       (None,
                  "Gross Primary Production",
                  "umol m-2 s-1"),
    "RECO":      (None,
                  "Ecosystem Respiration",
                  "umol m-2 s-1"),
    "FC":        (None,
                  "CO2 Turbulent Flux (no storage correction)",
                  "umol m-2 s-1"),
    "SC":        (None,
                  "CO2 Storage Flux",
                  "umol m-2 s-1"),
    "CO2":       ("mole_fraction_of_carbon_dioxide_in_air",
                  "CO2 Mole Fraction",
                  "umol mol-1"),
    # Energy fluxes
    "LE":        ("surface_upward_latent_heat_flux",
                  "Latent Heat Flux",
                  "W m-2"),
    "H":         ("surface_upward_sensible_heat_flux",
                  "Sensible Heat Flux",
                  "W m-2"),
    "G":         ("downward_heat_flux_in_soil",
                  "Soil Heat Flux",
                  "W m-2"),
    "NETRAD":    ("surface_net_downward_radiative_flux",
                  "Net Radiation",
                  "W m-2"),
    # Radiation components
    "SW_IN":     ("surface_downwelling_shortwave_flux_in_air",
                  "Incoming Shortwave Radiation",
                  "W m-2"),
    "SW_OUT":    ("surface_upwelling_shortwave_flux_in_air",
                  "Outgoing Shortwave Radiation",
                  "W m-2"),
    "SW_DIF":    (None,
                  "Diffuse Shortwave Radiation",
                  "W m-2"),
    "LW_IN":     ("surface_downwelling_longwave_flux_in_air",
                  "Incoming Longwave Radiation",
                  "W m-2"),
    "LW_OUT":    ("surface_upwelling_longwave_flux_in_air",
                  "Outgoing Longwave Radiation",
                  "W m-2"),
    "PPFD_IN":   ("surface_downwelling_photosynthetic_radiative_flux",
                  "Photosynthetic Photon Flux Density Incoming",
                  "umol m-2 s-1"),
    "PPFD_OUT":  (None,
                  "Photosynthetic Photon Flux Density Outgoing",
                  "umol m-2 s-1"),
    "PPFD_DIF":  (None,
                  "Photosynthetic Photon Flux Density Diffuse",
                  "umol m-2 s-1"),
    # Meteorology
    "TA":        ("air_temperature",
                  "Air Temperature",
                  "degC"),
    "RH":        ("relative_humidity",
                  "Relative Humidity",
                  "%"),
    "VPD":       (None,
                  "Vapour Pressure Deficit",
                  "hPa"),
    "PA":        ("surface_air_pressure",
                  "Atmospheric Pressure",
                  "kPa"),
    "P":         ("precipitation_amount",
                  "Precipitation",
                  "mm"),
    "WS":        ("wind_speed",
                  "Wind Speed",
                  "m s-1"),
    "WD":        ("wind_from_direction",
                  "Wind Direction",
                  "degree"),
    "USTAR":     (None,
                  "Friction Velocity",
                  "m s-1"),
    "ZL":        (None,
                  "Monin-Obukhov Stability Parameter",
                  "1"),
    "H2O":       (None,
                  "Water Vapour Mole Fraction",
                  "mmol mol-1"),
    # Soil profile
    "TS":        ("soil_temperature",
                  "Soil Temperature",
                  "degC"),
    "SWC":       (None,
                  "Soil Water Content (volumetric)",
                  "%"),
    "WTD":       (None,
                  "Water Table Depth",
                  "m"),
    # Footprint
    "FETCH_MAX": (None, "Fetch at Maximum Flux Contribution",    "m"),
    "FETCH_90":  (None, "Fetch Encompassing 90% of Flux",        "m"),
    "FETCH_55":  (None, "Fetch Encompassing 55% of Flux",        "m"),
    "FETCH_FILTER": (None, "Fetch Quality Filter",               "1"),
    # Energy balance
    "EBC_CF_N":  (None, "Energy Balance Closure Correction Factor N", "1"),
    "EBC_CF_METHOD": (None, "Energy Balance Closure Method Flag", "1"),
}

# Human-readable descriptions for common column suffixes
_SUFFIX_DESC: dict[str, str] = {
    "F_MDS":        "gap-filled by Marginal Distribution Sampling",
    "F_ANN":        "gap-filled by Artificial Neural Network",
    "F":            "gap-filled",
    "VUT_REF":      "VUT reference partitioning",
    "VUT_MEAN":     "VUT mean partitioning",
    "VUT_SE":       "VUT partitioning standard error",
    "VUT_USTAR50":  "VUT partitioning at USTAR 50th percentile",
    "CUT_REF":      "CUT reference partitioning",
    "CUT_MEAN":     "CUT mean partitioning",
    "NT_VUT_REF":   "night-time partitioning, VUT reference",
    "NT_CUT_REF":   "night-time partitioning, CUT reference",
    "DT_VUT_REF":   "day-time partitioning, VUT reference",
    "DT_CUT_REF":   "day-time partitioning, CUT reference",
    "RANDUNC":      "random uncertainty",
    "RANDUNC_METHOD": "random uncertainty method flag",
    "JOINTUNC":     "joint uncertainty",
    "SSITC_TEST":   "Steady State and Integral Turbulence Characteristics test",
}


# ── Filename parsing ──────────────────────────────────────────────────────────

def parse_filename(path: Path) -> dict:
    """Extract site metadata from an ICOS/FLUXNET standard filename."""
    m = re.match(
        r"FLX_([^_]+)_(FLUXNET2015|ICOS[^_]*)_([^_]+)_([^_]+)_(\d{4})-(\d{4})_(.+)",
        path.stem, re.IGNORECASE,
    )
    if m:
        return {
            "site_id":    m.group(1),
            "product":    m.group(2),
            "subset":     m.group(3),
            "freq":       m.group(4),
            "year_start": m.group(5),
            "year_end":   m.group(6),
            "version":    m.group(7),
        }
    # ICOS Interim format: ICOSETC_<SITE>_<PRODUCT>_<LEVEL>.csv
    m2 = re.match(r"ICOSETC_([^_]+)_(.+)", path.stem, re.IGNORECASE)
    if m2:
        return {"site_id": m2.group(1), "product": m2.group(2)}
    return {}


# ── Variable / column helpers ─────────────────────────────────────────────────

# Strip trailing profile/replicate notation like _1_1_1 or _1_2
_PROFILE_RE = re.compile(r'(_\d+){1,3}$')


def _get_root_and_suffix(col: str) -> tuple[str, str]:
    """Split a FLUXNET column name into (root, suffix) using VAR_META keys."""
    base = _PROFILE_RE.sub('', col)
    parts = base.split('_')
    # Try longest match first
    for length in range(len(parts), 0, -1):
        candidate = '_'.join(parts[:length])
        if candidate in VAR_META:
            suffix = '_'.join(parts[length:])
            return candidate, suffix
    # Unknown root: whole stripped name is root
    return base, ''


def _build_long_name(col: str, is_qc: bool = False) -> str:
    if is_qc:
        base_col = col[:-3] if col.endswith("_QC") else col[:-5]  # strip _QC or _FLAG
        root, suffix = _get_root_and_suffix(base_col)
        base_meta = VAR_META.get(root)
        base_long = base_meta[1] if base_meta else base_col.replace('_', ' ').title()
        desc = _SUFFIX_DESC.get(suffix, suffix.replace('_', ' ')) if suffix else ''
        qualifier = f" ({desc})" if desc else ""
        return f"Quality Control Flag for {base_long}{qualifier}"

    root, suffix = _get_root_and_suffix(col)
    base_meta = VAR_META.get(root)
    base_long = base_meta[1] if base_meta else root.replace('_', ' ').title()
    if suffix:
        desc = _SUFFIX_DESC.get(suffix, suffix.replace('_', ' '))
        return f"{base_long} ({desc})"
    return base_long


def _get_units(col: str) -> str:
    root, _ = _get_root_and_suffix(col)
    meta = VAR_META.get(root)
    return meta[2] if meta else "1"


def _get_standard_name(col: str) -> str | None:
    root, suffix = _get_root_and_suffix(col)
    # Only assign CF standard_name when there is no suffix that changes meaning
    if suffix:
        return None
    meta = VAR_META.get(root)
    return meta[0] if meta else None


# ── Time helpers ──────────────────────────────────────────────────────────────

def _parse_timestamps(series: pd.Series) -> pd.DatetimeIndex:
    """Parse FLUXNET timestamps; auto-detects format from value length."""
    sample = str(series.iloc[0]).strip()
    if len(sample) >= 12:
        fmt = "%Y%m%d%H%M"
    elif len(sample) == 8:
        fmt = "%Y%m%d"
    elif len(sample) == 6:
        fmt = "%Y%m"
    else:  # 4-digit year (YY files)
        fmt = "%Y"
    return pd.DatetimeIndex(pd.to_datetime(series.astype(str).str.strip(), format=fmt))


def _to_cf_time(datetimes: pd.DatetimeIndex, epoch: pd.Timestamp) -> np.ndarray:
    return np.asarray((datetimes - epoch).total_seconds(), dtype=np.float64) / 60.0


# ── Frequency / sibling helpers ───────────────────────────────────────────────

# ISO 8601 duration string per FLUXNET frequency token
_FREQ_ISO: dict[str, str] = {
    "HH": "PT30M", "HR": "PT1H",
    "DD": "P1D", "WW": "P7D", "MM": "P1M", "YY": "P1Y",
}

# Period length used to compute time_bounds end when TIMESTAMP_END is absent
_FREQ_OFFSET: dict[str, pd.DateOffset] = {
    "HH": pd.DateOffset(minutes=30),
    "HR": pd.DateOffset(hours=1),
    "DD": pd.DateOffset(days=1),
    "WW": pd.DateOffset(weeks=1),
    "MM": pd.DateOffset(months=1),
    "YY": pd.DateOffset(years=1),
}

# ── Product priority for deduplication (lower = higher priority) ──────────────
# NOTE: METEOSENS must be listed before METEO to avoid substring match.
_PRIORITY_ORDER: list[tuple[str, int]] = [
    ("FLUXNET",   0),
    ("FLUXES",    1),
    ("METEOSENS", 2),
    ("METEO",     3),
]

# Global attributes propagated to every child group so each group is
# self-describing when opened in isolation.
_GLOBAL_ATTRS: tuple[str, ...] = (
    "Conventions", "title", "institution", "site_id",
    "featureType", "history", "comment", "references",
    "icos_landing_page", "geospatial_lat", "geospatial_lon",
    "station_elevation", "station_elevation_units",
    "country", "icos_station_class", "icos_labeling_date",
    "time_zone", "ecosystem", "climate_zone",
    "mean_annual_temperature", "mean_annual_temperature_units",
    "mean_annual_precipitation", "mean_annual_precipitation_units",
    "mean_annual_sw_radiation", "mean_annual_sw_radiation_units",
    "icos_documentation", "principal_investigator",
    "researcher", "data_manager",
    "station_engineer", "station_administrator",
    "source_doi", "PartOfDataset", "citation",
)


def _choose_int_dtype(valid: np.ndarray) -> tuple[str, np.generic] | None:
    """Return (nc_dtype, fill_value) if *valid* contains only integer values, else None.

    *valid* must be the non-masked subset of a float64 column
    (i.e. already filtered with ``safe[~mask]``).
    """
    if not (valid.size and not np.any(valid != np.floor(valid))):
        return None
    vmin, vmax = int(valid.min()), int(valid.max())
    if -128 <= vmin and vmax <= 127:        return "i1", np.int8(-1)
    if -32_768 <= vmin and vmax <= 32_767:  return "i2", np.int16(-9999)
    if vmax <= 2_147_483_647:               return "i4", np.int32(-9999)
    return "i8", np.int64(-9999)


def _detect_freq_code(path: Path) -> str | None:
    """Return the FLUXNET frequency token (HH, HR, DD, WW, MM, YY) from the filename."""
    stem_up = path.stem.upper()
    for code in ("HH", "HR", "DD", "WW", "MM", "YY"):
        if f"_{code}_" in stem_up:
            return code
    return None


def fetch_icos_station_meta(site_id: str) -> dict:
    """Fetch station metadata from the ICOS Carbon Portal and return a dict of
    global-attribute key/value pairs ready to be written to a NetCDF file.

    Uses content negotiation (Accept: application/json) against the station
    landing page at https://meta.icos-cp.eu/resources/stations/ES_{site_id}.
    Returns an empty dict on any network or parse error.
    """
    import json
    import urllib.request

    landing_page = f"https://meta.icos-cp.eu/resources/stations/ES_{site_id}"
    try:
        req = urllib.request.Request(
            landing_page, headers={"Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
    except Exception as exc:
        print(f"  WARNING: could not fetch ICOS metadata for {site_id}: {exc}",
              file=sys.stderr)
        return {}

    attrs: dict = {}
    attrs["icos_landing_page"] = landing_page

    # Location
    loc = data.get("location", {})
    if "lat" in loc:
        attrs["geospatial_lat"]       = float(loc["lat"])
    if "lon" in loc:
        attrs["geospatial_lon"]       = float(loc["lon"])
    if "alt" in loc:
        attrs["station_elevation"]    = float(loc["alt"])
        attrs["station_elevation_units"] = "m above sea level"

    # Country
    if "countryCode" in data:
        attrs["country"] = data["countryCode"]

    # specificInfo block
    si = data.get("specificInfo", {})
    if "stationClass" in si:
        attrs["icos_station_class"]   = str(si["stationClass"])
    if "labelingDate" in si:
        attrs["icos_labeling_date"]   = si["labelingDate"]
    if "timeZoneOffset" in si:
        tz = int(si["timeZoneOffset"])
        attrs["time_zone"]            = f"UTC{tz:+03d}:00"
    if "ecosystemType" in si:
        attrs["ecosystem"]            = si["ecosystemType"]["label"]
    if "climateZone" in si:
        attrs["climate_zone"]         = si["climateZone"]["label"]
    if "meanAnnualTemp" in si:
        attrs["mean_annual_temperature"]    = float(si["meanAnnualTemp"])
        attrs["mean_annual_temperature_units"] = "degC"
    if "meanAnnualPrecip" in si:
        attrs["mean_annual_precipitation"]  = float(si["meanAnnualPrecip"])
        attrs["mean_annual_precipitation_units"] = "mm"
    if "meanAnnualRad" in si:
        attrs["mean_annual_sw_radiation"]   = float(si["meanAnnualRad"])
        attrs["mean_annual_sw_radiation_units"] = "W m-2"

    # Documentation (labelling report)
    docs = si.get("documentation", [])
    if docs:
        attrs["icos_documentation"] = docs[0].get("res", "")

    # Current staff (no end date)
    current = [
        s for s in data.get("staff", [])
        if s.get("role", {}).get("end") is None
    ]
    by_role: dict[str, list[str]] = {}
    for s in current:
        role  = s["role"]["role"]["label"]
        fname = s["person"]["firstName"]
        lname = s["person"]["lastName"]
        by_role.setdefault(role, []).append(f"{fname} {lname}")

    role_attr = {
        "Principal Investigator": "principal_investigator",
        "Researcher":             "researcher",
        "Data Manager":           "data_manager",
        "Engineer":               "station_engineer",
        "Administrator":          "station_administrator",
    }
    for role_label, attr_key in role_attr.items():
        if role_label in by_role:
            attrs[attr_key] = ", ".join(by_role[role_label])

    return attrs


def fetch_doi_citation(doi: str) -> tuple[str, str]:
    """Return (canonical_doi_url, apa_citation) for *doi*.

    Uses content negotiation against https://doi.org/ with
    Accept: text/x-bibliography; style=apa.
    Strips any HTML tags from the returned citation text.
    Returns ('', '') on any error.
    """
    import urllib.request

    doi = doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    doi_url = f"https://doi.org/{doi}"
    try:
        req = urllib.request.Request(
            doi_url, headers={"Accept": "text/x-bibliography; style=apa"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            citation = resp.read().decode("utf-8").strip()
    except Exception as exc:
        print(f"  WARNING: could not fetch APA citation for {doi_url}: {exc}",
              file=__import__("sys").stderr)
        return "", ""

    # Strip HTML tags (e.g. <i>...</i> used for journal/book titles)
    citation = re.sub(r"<[^>]+>", "", citation)
    return doi_url, citation


def fetch_dobj_citation(res_url: str) -> tuple[str, str]:
    """Return (pid_url, citation_string) for an ICOS CP data object.

    Fetches the data object's JSON metadata from *res_url*
    (e.g. ``https://meta.icos-cp.eu/objects/{hash}``), and extracts:
    - ``references.citationString`` — the pre-formatted citation
    - ``pid``                       — used to build the handle URL
      ``https://hdl.handle.net/{pid}``

    Returns ('', '') on any error or if the fields are absent.
    """
    import json
    import urllib.request

    if res_url in _dobj_citation_cache:
        return _dobj_citation_cache[res_url]

    try:
        req = urllib.request.Request(
            res_url, headers={"Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.load(resp)
    except Exception as exc:
        print(
            f"  WARNING: could not fetch data object metadata from {res_url}: {exc}",
            file=sys.stderr,
        )
        _dobj_citation_cache[res_url] = ("", "")
        return ("", "")

    citation = data.get("references", {}).get("citationString", "") or ""
    pid      = data.get("pid", "") or ""
    pid_url  = f"https://hdl.handle.net/{pid}" if pid else ""
    result   = (pid_url, citation)
    _dobj_citation_cache[res_url] = result
    return result


def fetch_column_instruments(res_url: str) -> dict[str, list[dict]]:
    """Return a column-label → instrument-deployment-list mapping for a METEOSENS object.

    Fetches the data object JSON from *res_url* and inspects
    ``specificInfo.columns``.  Only columns that carry at least one
    ``instrumentDeployments`` entry are included in the result.

    Each deployment record contains:
      instrument             – human-readable label (model + serial)
      instrument_uri         – ICOS CP instrument URI
      instrument_description – first comment string from the instrument record
      lat, lon, alt          – deployment position
      start                  – ISO-8601 UTC deployment start
      stop                   – ISO-8601 UTC deployment end, or None if ongoing

    Returns an empty dict on any error or if the object has no deployment info.
    """
    import json
    import urllib.request

    if res_url in _column_instruments_cache:
        return _column_instruments_cache[res_url]

    try:
        req = urllib.request.Request(
            res_url, headers={"Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.load(resp)
    except Exception as exc:
        print(
            f"  WARNING: could not fetch column instrument metadata from {res_url}: {exc}",
            file=sys.stderr,
        )
        _column_instruments_cache[res_url] = {}
        return {}

    columns = data.get("specificInfo", {}).get("columns", [])
    result: dict[str, list[dict]] = {}
    for col in columns:
        deployments = col.get("instrumentDeployments", [])
        if not deployments:
            continue
        col_label = col["label"]
        result[col_label] = [
            {
                "instrument":             d["instrument"]["label"],
                "instrument_uri":         d["instrument"]["uri"],
                "instrument_description": (d["instrument"].get("comments") or [""])[0],
                "lat":   d["pos"]["lat"],
                "lon":   d["pos"]["lon"],
                "alt":   d["pos"]["alt"],
                "start": d["start"],
                "stop":  d.get("stop"),
            }
            for d in deployments
        ]
    _column_instruments_cache[res_url] = result
    return result


def _sibling_csv(hh_csv: Path, hh_token: str, freq_code: str) -> Path:
    """Derive sibling CSV path by replacing the HH/HR freq token in the stem."""
    stem = hh_csv.stem
    marker = f"_{hh_token.upper()}_"
    idx = stem.upper().find(marker)
    if idx == -1:
        return hh_csv.parent / f"{stem}_{freq_code}{hh_csv.suffix}"
    new_stem = stem[:idx] + f"_{freq_code}_" + stem[idx + len(marker):]
    return hh_csv.parent / (new_stem + hh_csv.suffix)


# ── Shared CSV / product helpers (also imported by icos_combined and fluxnet_restructure)

def _read_csv(csv_path: Path) -> pd.DataFrame:
    """Read a FLUXNET/ICOS CSV, handling two-line wrapped headers."""
    na_vals = [str(FILL_VALUE_IN), str(float(FILL_VALUE_IN)), FILL_VALUE_IN]
    with open(csv_path, encoding="utf-8") as fh:
        line1 = fh.readline().rstrip("\n")
        line2 = fh.readline().rstrip("\n")
    if not line2.split(",")[0].strip().isdigit():
        columns = (line1 + line2).split(",")
        return pd.read_csv(
            csv_path, header=None, skiprows=2, names=columns,
            na_values=na_vals, low_memory=False,
        )
    return pd.read_csv(csv_path, na_values=na_vals, low_memory=False)


def _infer_freq_code(ts_start: pd.DatetimeIndex) -> str:
    """Guess the FLUXNET frequency code from the spacing between the first two timestamps."""
    if len(ts_start) < 2:
        return "HH"
    freq_min = int((ts_start[1] - ts_start[0]).total_seconds() / 60)
    if freq_min <= 60:      return "HH"
    if freq_min <= 1_441:   return "DD"
    if freq_min <= 10_081:  return "WW"
    if freq_min <= 44_641:  return "MM"
    return "YY"


def _group_name(csv_path: Path) -> str:
    """Derive a safe NetCDF4 group name from an ICOS filename."""
    finfo   = parse_filename(csv_path)
    product = finfo.get("product", csv_path.stem)
    name = re.sub(r"_(INTERIM|L[0-9]).*", "", product, flags=re.IGNORECASE)
    name = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()
    return name or "data"


def _product_priority(csv_path: Path) -> int:
    """Return the product priority for deduplication (lower = higher priority)."""
    stem_up = csv_path.stem.upper()
    for prod, pri in _PRIORITY_ORDER:
        if prod in stem_up:
            return pri
    return 99


# ── Main conversion ───────────────────────────────────────────────────────────

def convert(csv_path: Path, nc_path: Path, args: argparse.Namespace) -> None:
    print(f"Reading  {csv_path}")

    # Some ICOS files have a wrapped header (column names split across two lines).
    # Detect this by comparing field count of the first two lines.
    na_vals = [str(FILL_VALUE_IN), str(float(FILL_VALUE_IN)), FILL_VALUE_IN]
    with open(csv_path, encoding="utf-8") as fh:
        line1 = fh.readline().rstrip("\n")
        line2 = fh.readline().rstrip("\n")
    n1 = len(line1.split(","))
    n2 = len(line2.split(","))
    # A data line starts with a 12-digit timestamp; a header continuation does not
    if not line2.split(",")[0].isdigit():
        # Header is wrapped — rejoin and skip both header lines when reading data
        columns = (line1 + line2).split(",")
        df = pd.read_csv(
            csv_path,
            header=None, skiprows=2,
            names=columns,
            na_values=na_vals,
            low_memory=False,
        )
    else:
        df = pd.read_csv(csv_path, na_values=na_vals, low_memory=False)

    finfo = parse_filename(csv_path)
    site_id = args.site_id or finfo.get("site_id", "unknown")

    # Detect timestamp columns
    if "TIMESTAMP_START" in df.columns:
        ts_start_col = "TIMESTAMP_START"
        ts_end_col   = "TIMESTAMP_END" if "TIMESTAMP_END" in df.columns else None
    elif "TIMESTAMP" in df.columns:
        ts_start_col = "TIMESTAMP"
        ts_end_col   = None
    else:
        sys.exit("ERROR: No TIMESTAMP or TIMESTAMP_START column found.")

    ts_start = _parse_timestamps(df[ts_start_col])
    freq_min = int((ts_start[1] - ts_start[0]).total_seconds() / 60)

    freq_code = _detect_freq_code(csv_path)
    iso_res   = _FREQ_ISO.get(freq_code or "", f"PT{freq_min}M")

    # CF time epoch: start of first year in the file
    epoch = ts_start[0].replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    time_units = f"minutes since {epoch.strftime('%Y-%m-%d %H:%M:%S')}"
    time_vals  = _to_cf_time(ts_start, epoch)

    ts_cols = {ts_start_col, ts_end_col} - {None}
    data_cols = [c for c in df.columns if c not in ts_cols]

    print(f"  Site: {site_id} | Records: {len(df)} | Resolution: {freq_min} min | Variables: {len(data_cols)}")
    print(f"Writing  {nc_path}")

    with nc.Dataset(nc_path, "w", format="NETCDF4") as ds:
        # ── Dimensions ────────────────────────────────────────────────────────
        ds.createDimension("time", len(df))

        # ── Global attributes ─────────────────────────────────────────────────
        ds.Conventions          = "CF-1.12"
        ds.title                = f"ICOS FLUXNET flux tower data — site {site_id}"
        ds.institution          = "ICOS Carbon Portal / FLUXNET"
        ds.source               = csv_path.name
        ds.site_id              = site_id
        ds.featureType          = "timeSeries"
        ds.temporal_resolution  = iso_res
        ds.history              = (
            f"Created {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by fluxnet2nc.py"
        )
        if finfo.get("product"):
            ds.product          = finfo["product"]
        if finfo.get("subset"):
            ds.processing_level = finfo["subset"]
        if finfo.get("version"):
            ds.version          = finfo["version"]
        if finfo.get("year_start") and finfo.get("year_end"):
            ds.time_coverage_start = finfo["year_start"]
            ds.time_coverage_end   = finfo["year_end"]
        if args.comment:
            ds.comment          = args.comment

        # ── ICOS station metadata (fetched once per site, cached) ─────────────
        if site_id not in _station_meta_cache:
            print(f"  Fetching ICOS station metadata for {site_id} …")
            _station_meta_cache[site_id] = fetch_icos_station_meta(site_id)
        for attr_key, attr_val in _station_meta_cache[site_id].items():
            setattr(ds, attr_key, attr_val)

        # ── DOI / PartOfDataset ───────────────────────────────────────────────
        # Pre-fetched per-archive citation takes priority (set by the download
        # pipeline via args.doi_url / args.doi_citation); fall back to fetching
        # from args.doi for standalone CLI use.
        doi_url      = getattr(args, "doi_url", "") or ""
        doi_citation = getattr(args, "doi_citation", "") or ""
        if not doi_url:
            doi_arg = getattr(args, "doi", "")
            if doi_arg and doi_arg not in _doi_citation_cache:
                print(f"  Fetching APA citation for DOI {doi_arg} …")
                _doi_citation_cache[doi_arg] = fetch_doi_citation(doi_arg)
            doi_url, doi_citation = _doi_citation_cache.get(doi_arg, ("", ""))
        if doi_url:
            ds.source_doi    = doi_url
        if doi_citation:
            ds.PartOfDataset = doi_citation
        dobj_citation = getattr(args, "dobj_citation", "") or ""
        if dobj_citation:
            ds.citation = dobj_citation

        # ── ICOS ETC L2 product metadata ──────────────────────────────────────
        product = finfo.get("product", "").upper()
        if product and site_id != "unknown":
            ds.naming_authority    = "ICOS ETC (Eddy Covariance Thematic Centre)"
            ds.processing_level    = "2"
            ds.station_classes     = "Class 1, Class 2, Associated"
            ds.update_interval     = (
                "Released once or twice per year; mid-year interim versions append "
                "new data to the last official release."
            )
            if "FLUXNET" in product:
                _tok = freq_code or "HH"
                ds.data_type_label     = "ETC L2 FLUXNET"
                ds.summary             = (
                    "Standard FLUXNET product including gap-filled NEE, GPP, "
                    "RECO and all the related variables and uncertainty estimates, and "
                    "gap-filled versions of selected meteorological variables. Data are "
                    "processed by the ICOS ETC starting from the FLUXES and METEO data "
                    "files using the ONEFlux pipeline."
                )
                ds.references          = (
                    "Pastorello et al. 2020, Scientific Data, "
                    "https://doi.org/10.1038/s41597-020-0534-3; "
                    "variable codes and units: "
                    "https://hdl.handle.net/11676/x4W1OhRrHkVmY_a-8Y4lF0tI"
                )
                ds.file_name_structure = (
                    f"ICOSETC_CC-###_FLUXNET_{_tok}_L2 "
                    f"(interim: ICOSETC_CC-###_FLUXNET_{_tok}_INTERIM_L2)"
                )
                ds.keywords            = "eddy covariance, carbon flux, NEE, GPP, RECO, gap-filling, ICOS, FLUXNET"
            elif "FLUXES" in product:
                ds.data_type_label     = "ETC L2 Fluxes"
                ds.summary             = (
                    "Half-hourly fluxes along with all the related quality flags and "
                    "statistics and auxiliary variables (e.g. storage fluxes, footprint "
                    "estimations, atmospheric parameters) calculated by the ICOS ETC "
                    "starting from the 10 or 20 Hz raw data for Class 1 and Class 2 "
                    "stations. Micrometeorological data are processed using the method "
                    "described in Vitale et al. 2020 (Biogeosciences). Data not "
                    "gap-filled and without partitioning and other post-processing."
                )
                ds.references          = (
                    "Vitale et al. 2020, Biogeosciences; "
                    "variable codes and units: "
                    "https://meta.icos-cp.eu/objects/E0KJJERYMTDbg4PfW26xazhk"
                )
                ds.file_name_structure = "ICOSETC_CC-###_FLUXES_L2 (interim: ICOSETC_CC-###_FLUXES_INTERIM_L2)"
                ds.keywords            = "eddy covariance, carbon flux, energy balance, ICOS, flux tower"
            elif "METEOSENS" in product:
                ds.data_type_label     = "ETC L2 Meteosens"
                ds.summary             = (
                    "Half-hourly meteorological data and all the related quality flags "
                    "and connected variables measured by each single sensor used in the "
                    "station. Data are processed and quality checked by the ICOS ETC "
                    "starting from the raw data at higher time resolution (Class 1 and 2 "
                    "stations). Data not gap-filled. Variable names are composed by the "
                    "variable code and three numeric indexes or positional qualifiers "
                    "indicating relative positions of observations at the site. The suffix "
                    "_SE denotes standard error, _N the number of single measurements, "
                    "and _QC a 12-cipher quality flag string encoding format checks, "
                    "out-of-range and step tests, missing data, disturbances, BADM "
                    "mapping status, and half-hour retention criteria."
                )
                ds.references          = (
                    "variable codes and units: "
                    "https://hdl.handle.net/11676/rlIMSsmAoDi2W3W44rGzQI3X"
                )
                ds.file_name_structure = "ICOSETC_CC-###_METEOSENS_L2 (interim: ICOSETC_CC-###_METEOSENS_INTERIM_L2)"
                ds.keywords            = "meteorology, sensor, micrometeorology, ICOS, flux tower"
            elif "METEO" in product:
                ds.data_type_label     = "ETC L2 Meteo"
                ds.summary             = (
                    "Half-hourly meteorological data and all the related quality flags "
                    "and connected variables obtained by aggregating possible different "
                    "sensors that for their characteristics and positions can be averaged. "
                    "Data are processed and quality checked by the ICOS ETC starting from "
                    "the METEOSENS data file (where each single sensor is provided). "
                    "Data not gap-filled. Variable names are composed by the variable "
                    "code and a numeric index to identify relative height or depth in "
                    "case of a vertical profile. The suffix _SD denotes standard deviation "
                    "and _N the number of sensors used in spatial aggregation."
                )
                ds.references          = (
                    "variable codes and units: "
                    "https://hdl.handle.net/11676/rlIMSsmAoDi2W3W44rGzQI3X"
                )
                ds.file_name_structure = "ICOSETC_CC-###_METEO_L2 (interim: ICOSETC_CC-###_METEO_INTERIM_L2)"
                ds.keywords            = "meteorology, micrometeorology, ICOS, flux tower"

        # ── Time coordinate ───────────────────────────────────────────────────
        tvar = ds.createVariable("time", "f8", ("time",))
        tvar.standard_name = "time"
        tvar.long_name     = "time at start of averaging period"
        tvar.units         = time_units
        tvar.calendar      = "standard"
        tvar.axis          = "T"
        tvar[:]            = time_vals

        # Time bounds: use TIMESTAMP_END when available; otherwise compute from
        # the known period length for DD/WW/MM/YY files that carry only TIMESTAMP.
        if ts_end_col:
            ts_end: pd.DatetimeIndex | None = _parse_timestamps(df[ts_end_col])
        elif freq_code in _FREQ_OFFSET:
            ts_end = pd.DatetimeIndex(
                [t + _FREQ_OFFSET[freq_code] for t in ts_start]
            )
        else:
            ts_end = None
        if ts_end is not None:
            ds.createDimension("nv", 2)
            tvar.bounds = "time_bounds"
            end_vals    = _to_cf_time(ts_end, epoch)
            tbvar = ds.createVariable("time_bounds", "f8", ("time", "nv"))
            tbvar.units    = time_units
            tbvar.calendar = "standard"
            tbvar[:, 0]    = time_vals
            tbvar[:, 1]    = end_vals

        # ── Data variables ────────────────────────────────────────────────────
        skipped = []
        for col in data_cols:
            is_qc    = col.endswith("_QC") or col.endswith("_FLAG")
            raw      = df[col].values
            mask     = np.asarray(pd.isna(df[col]))

            # QC/FLAG columns: integers for HH files (0-3 or 12-cipher codes).
            # Aggregated products (DD/WW/MM/YY) store QC as fractional means
            # (e.g. 0.5 = 50 % measured), so use float32 when any value is
            # non-integer.  Integer type selection:
            #   u1 (0-254)  for standard QC flags (0-3)
            #   i4          for values up to ~2e9
            #   i8          for METEOSENS 12-cipher codes (up to ~1e12)
            if is_qc:
                safe = np.where(mask, 0, np.asarray(raw, dtype=np.float64))
                if np.any(safe != np.floor(safe)):
                    # Fractional QC — store as float32
                    dtype, fv = "f4", FILL_VALUE_OUT
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
                safe  = np.where(mask, 0.0, f64)
                int_t = _choose_int_dtype(safe[~mask])
                if int_t:
                    dtype, fv = int_t
                    arr = np.where(mask, fv, safe.astype(np.dtype(dtype)))
                else:
                    dtype, fv = "f4", FILL_VALUE_OUT
                    arr = np.where(mask, fv, f64.astype(np.float32))

            var = ds.createVariable(
                col, dtype, ("time",),
                fill_value=fv,
                zlib=True, complevel=4,
            )
            var.long_name    = _build_long_name(col, is_qc=is_qc)
            var.missing_value = fv

            if is_qc:
                var.units = "1"
                if dtype == "u1":
                    # Standard FLUXNET/ICOS QC: 0=measured, 1-3=gap-fill quality
                    var.flag_values   = np.array([0, 1, 2, 3], dtype=np.uint8)
                    var.flag_meanings = (
                        "measured "
                        "good_quality_gap_fill "
                        "medium_quality_gap_fill "
                        "poor_quality_gap_fill"
                    )
                else:
                    # METEOSENS 12-cipher composite flag — see product description
                    var.comment = (
                        "12-digit composite quality flag. Each cipher position encodes "
                        "a specific check: format presence/compliance, out-of-range test, "
                        "step test, missing records, disturbances, BADM mapping, and "
                        "half-hour retention criteria. First cipher is always 8."
                    )
            else:
                var.units = _get_units(col)
                sn = _get_standard_name(col)
                if sn:
                    var.standard_name = sn
                if dtype == "f4":
                    var.fluxnet_missing_value = np.int32(FILL_VALUE_IN)

            var[:] = arr

    if skipped:
        print(f"  Skipped {len(skipped)} non-numeric column(s): {', '.join(skipped)}")
    print(f"Done.    {nc_path}")

    # ── Convert sibling DD/WW/MM/YY CSV files when input is HH/HR ─────────────
    if freq_min <= 60 and not getattr(args, "no_agg", False):
        hh_token = finfo.get("freq") or next(
            (tok for tok in ("HH", "HR") if f"_{tok}_" in csv_path.stem.upper()), None
        )
        if hh_token:
            for agg_code in ("DD", "WW", "MM", "YY"):
                sib = _sibling_csv(csv_path, hh_token, agg_code)
                if sib.exists():
                    convert(sib, sib.with_suffix(".nc"), args)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert an ICOS FLUXNET CSV file to CF-1.12 NetCDF4.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "csv",
        nargs="+",
        help="Input FLUXNET CSV file(s); glob patterns (e.g. *.csv) are supported",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        metavar="NC",
        help="Output NetCDF file; only valid when a single input file is given",
    )
    parser.add_argument(
        "--site-id",
        default="",
        metavar="SITE",
        help="Override site ID (auto-detected from ICOS filename convention)",
    )
    parser.add_argument(
        "--comment",
        default="",
        help="Free-text comment added as a global attribute",
    )
    parser.add_argument(
        "--no-agg",
        action="store_true",
        default=False,
        help="Skip automatic conversion of sibling DD/WW/MM/YY CSV files for HH/HR input",
    )
    parser.add_argument(
        "--doi",
        default="",
        metavar="DOI",
        help=(
            "DOI of the source dataset collection (e.g. 10.18160/R3G6-Z8ZH). "
            "Stored as source_doi; APA citation fetched from doi.org stored as PartOfDataset."
        ),
    )
    args = parser.parse_args()

    # Expand any glob patterns (needed on Windows where the shell does not expand them)
    csv_paths: list[Path] = []
    for pattern in args.csv:
        matches = sorted(Path().glob(pattern)) if any(c in pattern for c in "*?[") else [Path(pattern)]
        csv_paths.extend(matches)

    if not csv_paths:
        sys.exit("ERROR: No files matched the given pattern(s).")

    if args.output and len(csv_paths) > 1:
        sys.exit("ERROR: -o/--output can only be used with a single input file.")

    for csv_path in csv_paths:
        csv_path = csv_path.resolve()
        if not csv_path.exists():
            print(f"WARNING: File not found, skipping: {csv_path}", file=sys.stderr)
            continue
        nc_path = args.output.resolve() if args.output else csv_path.with_suffix(".nc")
        convert(csv_path, nc_path, args)


if __name__ == "__main__":
    main()
