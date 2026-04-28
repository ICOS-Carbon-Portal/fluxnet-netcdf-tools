# Obspack → zarr conversion plan

Build a zarr v2 store of ICOS atmospheric greenhouse gas Obspack data
(CO2, CH4, N2O, CO), modeled on the existing `fluxnet2zarr.py` toolkit.

**Source**: ICOS Obspack collection
[10.18160/1PZ9-SDJ2](https://doi.org/10.18160/1PZ9-SDJ2) — 357 CF-NetCDF
files for 1972–2026 covering ~70 European stations.

---

## File naming convention (observed in collection)

```
ch4_arn_tower-insitu_478_allvalid-10magl.nc      ← gas, trigram, kind, dataset_num, level/sampling
co2_cbw_tower-insitu_445_allvalid-127magl.nc
co_cmn_surface-insitu_443_allvalid.nc            ← surface stations have no -magl suffix
n2o_kre_tower-insitu_147_allvalid-250magl.nc
```

| Token | Meaning |
|---|---|
| `gas` | `co2`, `ch4`, `n2o`, `co` |
| `trigram` | 3-letter station code (e.g. `arn`, `cbw`, `cmn`) |
| `kind` | `tower-insitu` (multiple heights) or `surface-insitu` (one level) |
| `dataset_num` | ICOS data set number |
| `magl` | sampling height (meters above ground level) — absent for surface stations |

Counts: `co2`=98, `ch4`=98, `co`=91, `n2o`=70 files; `tower-insitu`=301, `surface-insitu`=56.

---

## Station ID convention

Use **trigram + height (rounded integer)**: e.g. `HTM150`, `CBW207`, `CMN0`.

- For surface stations without `magl` in the filename, use `0` (or read `intake_height` from the file and use that).
- All four gases for the same trigram + height end up in the **same group** (one zarr group per (station, height) tuple).

---

## Store layout

```
icos-obspack.zarr/
  HTM150/                      ← root group: all four gases at this trigram+height
    .zgroup
    .zattrs                    ← station metadata + _provenance JSON
    time/                      ← shared time axis (union or per-gas?  see Q1 below)
    co2/                       ← per-gas measurement variables
    co2_unc/
    co2_qcflag/
    co2_nvalue/
    ch4/
    ch4_unc/
    …
    n2o/
    co/
  CBW207/
    …
  CMN0/
    …
```

Per-station `.zattrs._provenance` keeps `created`, `last_updated`,
`source_dois` (one per gas/file ingested), `citations`, and a `history`
array of every ingest action.

### Static (non-time-varying) metadata as group attributes — NOT columns

Per user request, do **not** duplicate values that don't change with time as columns.
Store these once in `.zattrs`, not as zarr arrays:

| Attribute | Source |
|---|---|
| `latitude`, `longitude` | from netCDF global attrs / `latitude`, `longitude` variables |
| `altitude` (masl) | global attr or `altitude` variable |
| `elevation` (surface masl) | global attr |
| `intake_height` (magl) | global attr or filename token |
| `station_name` | global attr or station landing page |
| `country`, `pi`, `wmo_region` | global attrs / station landing page |
| `instrument`, `method` | sample-level — see Q2 below |

When `intake_height` varies (rare — moved sensor), it stays a column
(time-varying). At ingest time, check whether `latitude`/`longitude`/
`altitude`/`intake_height` are all-equal across the time axis: if yes,
write as scalar attribute; if no, write as 1-D zarr array.

---

## CLI

Mirrors `fluxnet2zarr.py` exactly:

```
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --station HTM150 CBW207
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --gas co2 ch4
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --store /data/obspack.zarr --keep-nc
python obspack2zarr.py list   [--store icos-obspack.zarr]
python obspack2zarr.py info   HTM150  [--store icos-obspack.zarr]
python obspack2zarr.py remove HTM150  [--store icos-obspack.zarr]
```

---

## Implementation

### 1. `obspack2zarr.py` (new — main script, ~250 lines)

Reuses from existing toolkit:
- `icos_download_restructure.resolve_collection_url()` — DOI → CP collection URL
- `icos_download_restructure.get_collection_members()` — list members of the collection
- `fluxnet2nc.fetch_dobj_citation()` — JSON-LD citation per data object
- ICOS station landing-page fetcher (similar pattern to fluxnet — but trigram-based station resource URLs)

New:
- `_parse_obspack_filename(name)` → `{gas, trigram, kind, dataset_num, height_magl}`
- `_station_id(trigram, height_magl)` → `"HTM150"` / `"CMN0"`
- `_group_files_by_station(members)` → `{station_id: [member, …]}`
- `cmd_populate(args)` — for each station+height tuple: download all four gas files, ingest each gas into the same zarr group, update provenance.
- `cmd_list`, `cmd_info`, `cmd_remove` — straight copies, no per-gas logic needed.

### 2. `obspack_ingest.py` (new — netCDF → zarr writer, ~200 lines)

```python
def ingest_obspack_file(
    nc_path: pathlib.Path,
    zarr_store: pathlib.Path,
    station_id: str,
    gas: str,
) -> None:
    """
    Read an Obspack netCDF file and append its data into
    {zarr_store}/{station_id}/{gas} (and ancillary {gas}_unc, {gas}_qcflag, …).
    Compute static-attr promotion, decode time, write group .zattrs once.
    """
```

Steps:
1. `xr.open_dataset(nc_path)` — Obspack files are CF-conformant.
2. Time decoding — Obspack supports `time` (POSIX seconds), `time_decimal`,
   `time_components`, `datetime`. Use `time` → decode to `datetime64[ns]`.
3. Identify the measurement variable (`value` or per-gas name) and its
   ancillaries: `value_unc`, `value_std_dev`, `nvalue`, `qcflag`,
   `instrument`, `method`, `intake_height`.
4. Rename `value` → `<gas>` (e.g. `value` → `co2`); rename `value_unc` →
   `<gas>_unc`, etc.
5. Promote static columns: for each candidate column (`latitude`,
   `longitude`, `altitude`, `intake_height`), if `np.all(arr == arr[0])`
   then write as scalar `.zattrs` entry, drop the column.
6. Merge into existing group:
   - First write for this group: `ds.to_zarr(store, group=station_id, mode="w")`
   - Subsequent writes (different gas, same station): use `mode="a"`
     and add the new variables to the existing group.
   - Time alignment: if the new gas's time axis differs from the
     existing one, store separate `time_<gas>` per gas (see Q1).

### 3. Station-level metadata helper (new)

ICOS atmosphere stations have a different resource URL pattern than
ecosystem stations (`AS_` prefix instead of `ES_`):

```
https://meta.icos-cp.eu/resources/stations/AS_HTM
https://meta.icos-cp.eu/resources/stations/AS_CMN
```

Add `fetch_atc_station_metadata(trigram)` returning `{lat, lon,
elevation, station_name, country, wmo_region, pi, …}`. Cache by trigram.

### 4. README update

Document `obspack2zarr.py` alongside `fluxnet2zarr.py`. The proxy
(`run_proxy.py`) already serves any `*.zarr` directory in `--store-dir`,
so `icos-obspack.zarr` is automatically usable via the data-passport
proxy with no proxy-side changes needed.

---

## Critical files

| File | Status | Lines (est.) |
|---|---|---|
| `obspack2zarr.py` | NEW | ~250 |
| `obspack_ingest.py` | NEW | ~200 |
| `obspack_zarr_readme.md` | NEW (this file) | — |
| `README.md` | append section | +30 |

No changes to `fluxnet*.py`, `zarr_proxy/`, `datapassport_zarr.py`.

---

## Findings from a real Obspack file

Verified against `ch4_arn_tower-insitu_478_allvalid-10magl.nc` (8316 hourly samples).

**Dimensions**: `time` (variable), `calendar_components`=6, `dim_concerns`=6.

**Time-varying variables** (per-sample):

| Variable | Type | Notes |
|---|---|---|
| `value` | float32 | the measurement in `mol mol-1` — rename + rescale (see below) |
| `value_std_dev` | float32 | uncertainty in `mol mol-1` — rename + rescale to match |
| `nvalue` | float64 | number of samples averaged |
| `start_time`, `datetime`, `time_decimal` | datetime/str/float | redundant with `time` coord — drop |
| `time_components`, `solartime_components` | (time, 6) int | redundant — drop |
| `qc_flag` | bytes | 1–2 unique values typically |
| `obspack_id` | bytes (200) | unique per sample — keep for round-trip |
| `obspack_num` | int32 | the global sample index — keep |
| `obs_num` | int32 | within-file index — drop |
| `assimilation_concerns` | (time, 6) int8 | model-assimilation flags |
| `obs_flag` | float32 | 1=large-scale, 0=local |
| `quality_id`, `icos_datalevel`, `icos_LTR`, `icos_SMR`, `icos_STTB` | float | ICOS-specific QC fields |

**Static columns** (promote to `.zattrs`):

| Variable | Source for promotion |
|---|---|
| `latitude`, `longitude` | always single value for fixed stations |
| `altitude` (masl), `intake_height` (magl) | always single value |
| `instrument` (numeric ID) | static for this file |

In our test file, all five candidates have exactly 1 unique value → confirmed
static-promotion rule is correct for fixed stations. Keep the all-equal check
in code for the rare case where an instrument was swapped mid-record.

**Global attrs to copy into group `.zattrs`**:

```
site_code, site_name, site_country, site_latitude, site_longitude,
site_elevation, site_url, site_utc2lst,
dataset_calibration_scale, dataset_data_frequency, dataset_data_frequency_unit,
dataset_intake_ht, dataset_intake_ht_unit, dataset_parameter,
dataset_globalview_prefix, dataset_name, dataset_num,
dataset_provider_citation_1..N (collect into a list),
dataset_provider_license, dataset_provider_license_url,
obspack_name, obspack_citation, obspack_data_license,
obspack_identifier_link, obspack_creation_date,
Conventions, source
```

**Measurement variable rename + unit rescale**:

The Obspack `value` variable is unitless (`mol mol-1`). Rename to the gas
name and rescale to community-standard mole fraction units:

| Gas | Output variable | Output units | Scale factor |
|---|---|---|---|
| CO2 | `co2` | `ppm` (µmol mol⁻¹) | × 1e6 |
| CH4 | `ch4` | `ppb` (nmol mol⁻¹) | × 1e9 |
| N2O | `n2o` | `ppb` | × 1e9 |
| CO | `co` | `ppb` | × 1e9 |

Also rescale the matching `value_std_dev` → `<gas>_std_dev` and any other
mole-fraction variable (e.g. ICOS `icos_LTR`, `icos_SMR`, `icos_STTB`).
Carry the original `dataset_calibration_scale` attribute (`"WMO X2004A"`,
`"WMO X2007"`, etc.) on the renamed variable so the calibration is
preserved.

**Filename token clarification** — the spec line in `dataset_selection_tag`
shows `allvalid-10.0magl` (with decimal), while filenames use
`allvalid-10magl` (rounded). Use the rounded integer for station IDs;
keep the exact float `dataset_intake_ht` in `.zattrs` for precision.

---

## Open questions to resolve before / during implementation

**Q1 — Time axis per gas or unified?**
Obspack files are sampled per-gas-per-instrument; CO2 and CH4 at the
same station+height almost always come from different instruments and
have different time axes. Two options:

- **A. Per-gas time axes** (recommended): each gas variable has its own
  dimension `time_co2`, `time_ch4`, etc. Cleaner, never aligns/wastes.
- **B. Union time axis with NaN padding**: simpler queries but huge NaN
  matrices when time grids differ.

→ **Default to A.** Writing each gas as its own xarray Dataset with its
own time coordinate, all merged into the same group via `to_zarr(mode="a")`.

**Q2 — Sample-level metadata (`instrument`, `method`)**
These can change mid-record. Keep as 1-D arrays (not attrs). Same
deduplication rule applies: if all-equal, promote to scalar; otherwise
keep as column.

**Q3 — Surface stations height — `0` or actual `intake_height`?**
Surface stations have a real intake height (typically 2–10 m). Read
`intake_height` from the file and round to integer. If the file has no
`intake_height`, fall back to `0`.

**Q4 — Provenance with multiple gas files per station**
Each gas/file ingested adds an entry to `_provenance.history`. A single
station group's `_provenance.source_dois` is a list of all (gas → DOI)
mappings, not a scalar.

**Q5 — Update semantics**
If the user re-runs `populate` with the same DOI, the script should:
- Re-download nothing if `last_updated` matches the collection version
- Otherwise re-ingest only changed files (compare object hashes)

---

## Verification plan

```bash
pip install xarray netCDF4 zarr<3

# Populate one station
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --station HTM150

# Inspect
python obspack2zarr.py list
python obspack2zarr.py info HTM150

# Python checks
python -c "
import xarray as xr, zarr, json
z = zarr.open_group('icos-obspack.zarr', 'r')
print('stations:', sorted(z.group_keys()))
ds = xr.open_zarr('icos-obspack.zarr', group='HTM150')
print(ds)
print('co2 shape:', ds['co2'].shape)
attrs = z['HTM150'].attrs
print('lat:', attrs.get('latitude'), 'lon:', attrs.get('longitude'))
print('intake_height:', attrs.get('intake_height'))
prov = json.loads(attrs['_provenance'])
print('history:', prov['history'])
"

# Full collection (357 files, ~70 stations)
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2

# Serve via existing proxy — no proxy-side changes
python run_proxy.py --store-dir .
# → http://localhost:8000/icos-obspack.zarr/HTM150/co2/...
```

---

## Future work (not in initial implementation)

### Build custom Obspacks from the zarr store

Once the store is populated, add a tool that produces a new Obspack-format
NetCDF compilation from a user-selected subset:

```
python zarr2obspack.py \
    --gas co2 ch4 \
    --station HTM150 CBW207 KRE250 \
    --start 2020-01-01 --end 2024-12-31 \
    --output my_obspack.nc
```

Implementation outline:
- Read the requested `(station, gas)` combinations from the zarr store via
  `xr.open_zarr(group=station_id)`.
- Slice each by the requested time window.
- Reconstruct Obspack-conformant variables — re-encode static `.zattrs`
  fields back into 1-D variables (`latitude`, `longitude`, `intake_height`)
  for each sample, restore `obspack_id`, `obspack_num`, etc.
- Concatenate along the `obs` dimension; write as CF-NetCDF matching the
  source Obspack specification.
- Mint a passport for the derived product (re-use `zarr_proxy.passport`
  builder if served via the proxy, or generate a standalone passport for
  CLI-built compilations).

This requires storing enough Obspack-specific provenance per sample to
reconstruct it — so the ingest step must keep `obspack_id` and any
sample-level provenance fields rather than dropping them. **Action:** when
implementing `obspack_ingest.py`, preserve all CF-Obspack required variables
even if redundant, so a faithful round-trip is possible later.

### Interactive viewer notebook

A counterpart to `multistation.ipynb` for atmosphere data:

- Dropdowns for: gas (CO2/CH4/N2O/CO), station (with sampling-height suffix
  visible), time window.
- Multi-select for stations to overlay on a single time-series plot.
- Map view (lat/lon dots from group `.zattrs`) for spatial selection.
- "Export" button that calls into the `zarr2obspack.py` logic above to
  download the user's selection as a custom Obspack file.

Reuses the same `datapassport_zarr` proxy → every viewing session
automatically mints a passport, identical to fluxnet pattern.

---

## Progress log

(updated during implementation)

- [x] Download one sample file (`ch4_arn_tower-insitu_478_allvalid-10magl.nc`)
- [x] Inspect dims, vars, global attrs → confirmed plan assumptions
- [x] Confirm static-column promotion rule (lat/lon/alt/intake_height all-equal)
- [x] Write `obspack_ingest.py`: file → xr.Dataset → zarr group (filename parse,
      static promotion, mole-fraction rescale, per-gas dim rename + var prefix,
      `merge_attrs()` accumulator)
- [x] Filename parser + station-ID generator (`parse_filename`, `ObspackFileInfo.station_id`)
- [x] Resolve Q1 → confirmed per-gas time axes (CH4 8316 vs CO2 8295 at ARN)
- [x] Static-column → attribute promotion (verified on ARN10)
- [x] Multi-gas merge: same station, different gas, append mode (CH4 + CO2 in ARN10)
      — note: `to_zarr(mode="a")` wipes group `.zattrs`, so attrs must be re-applied
      after every `to_zarr` call from an in-memory accumulator
- [x] Write `obspack2zarr.py` skeleton (CLI: populate / list / info / remove)
- [x] Provenance history accumulator (`_update_provenance`)
- [x] Smoke test on a tower-insitu (BIR10 / BIR50 / BIR75 — 9 files, 3 heights)
      — verified per-height station IDs, unit scaling, calibration scales
- [x] Per-station + store-root metadata consolidation
- [x] Workaround for `to_zarr(mode="a")` wiping group attrs (in-memory accumulator)
- [x] Workaround for Windows file-handle leak (`xr.open_dataset(...).load()`)
- [x] ATC station metadata fetcher (`AS_<trigram>`) — `fetch_atc_station_metadata()`
      adds `station_class`, `station_labeling_date`, `wigos_id`,
      `time_zone_offset`, `responsible_organization`, `current_pi`,
      `country_code`, `station_landing_page`. Cached per trigram across heights.
      Verified on ARN10.
- [x] Full ingest of 357 files — completed 2026-04-28: 357/357 files ingested,
      0 failures, ~65 minutes total. 100 station-groups (TRIGRAM+height) across
      40 unique trigrams.
- [x] README update — section added between fluxnet2zarr and run_proxy
