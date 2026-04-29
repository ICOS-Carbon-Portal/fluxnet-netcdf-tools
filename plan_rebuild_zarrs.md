# Plan — rebuild zarr stores with a `station` dimension

## Goal

Make spatial filtering work as a real xarray operation across all stations:

```python
ds = xr.open_zarr("icos-obspack.zarr/co2_combined")
nl  = ds.where(
    (ds.lat >= 50.7) & (ds.lat <= 53.6) &
    (ds.lon >=  3.3) & (ds.lon <=  7.3),
    drop=True,
).sel(time_co2=slice("2024-01-01", "2024-12-31"))
```

No per-group iteration, no manual `.zattrs` lookup, no helper code.
The whole thing is one xarray expression.

## Why a redesign is needed

Today's layout is **one zarr group per (station, height)**:

```
icos-obspack.zarr/
  CBW27/   {co2, ch4, co, …; time_co2; .zattrs={lat, lon, …}}
  CBW67/
  CBW127/
  CBW207/
  JUE50/
  …
```

xarray sees each group as an independent `Dataset`. There is no shared
coordinate that xarray can filter against — the lat/lon are scalars
attached to the *group attrs*, not to a dimension.

To filter across all stations as one xarray call, every variable must
share a `station` dimension and `lat`/`lon` must be 1-D coordinates
along that dimension.

## Proposed layout — Obspack

One zarr group per gas, with a single `station` dimension:

```
icos-obspack.zarr/
  co2/                    ← group
    .zgroup
    .zattrs               (collection-level: DOI, calibration scales, …)
    station/              ← coord, 1-D, dtype |U6 ("CBW27", "CBW67", …)
    lat/                  ← coord, 1-D along station, float64
    lon/                  ← coord, 1-D along station, float64
    intake_height/        ← coord, 1-D along station, float64
    site_name/            ← coord, 1-D, |U…
    country/              ← coord, 1-D, |U2
    time_co2/             ← coord, 1-D, datetime64[ns]
    co2/                  ← data var, 2-D (station, time_co2), float32
    co2_std_dev/          ← 2-D (station, time_co2)
    co2_qc_flag/          ← 2-D (station, time_co2), object/|S1
    obspack_id/           ← 2-D (station, time_co2), |S200
    obspack_num/          ← 2-D (station, time_co2), int32
  ch4/                    ← same shape, different stations may participate
    station/  lat/  lon/  …
    time_ch4/
    ch4/  ch4_std_dev/  …
  n2o/   …
  co/    …
```

**Tradeoffs**

- The 2-D `(station, time_co2)` is sparse: each station has its own time
  range. Stored on disk that's fine — zarr stores are sparse via chunk
  filtering — but in-memory it's a dense rectangle of NaNs whenever you
  open it. For a station-set of 100 and a 5-year hourly axis, that's
  100 × 44 000 = 4.4 M floats = ~17 MB; tractable.
- Time axis is the **union** of all participating stations' time axes
  for that gas. Stations missing data at a given timestamp get NaN.
- Per-gas grouping keeps the four time axes distinct (`time_co2`,
  `time_ch4`, …) — each gas's union differs because not every station
  measures every gas.
- Static metadata (`lat`, `lon`, `intake_height`, `instrument`,
  `calibration_scale`) becomes 1-D along `station` instead of scalar
  attrs. Calibration scale per-station-per-gas is intrinsically per-gas
  so it lives in the gas group; lat/lon/intake_height are duplicated
  across gas groups (small cost — a few hundred bytes per gas).

## Proposed layout — Fluxnet

Trickier because each station's HH dataset has 600+ variables and
extra dims (`ustar_threshold`, `nee_variant`, `partition_method`,
`corr_pct`, `soil_layer`, lots of `*_level` dims). Three options:

### Option A — combined HH + per-frequency aggregations, station dim only

Mirror the obspack approach: per-aggregation group, with a `station`
coord:

```
icos-fluxnet.zarr/
  hh/                        ← group, half-hourly
    station/  lat/  lon/  …
    time/
    NEE/                     ← 4-D (station, time, ustar_threshold, nee_variant)
    GPP/                     ← 5-D (station, time, ustar_threshold, partition_method, nee_variant)
    TA_F/                    ← 2-D (station, time)
    SW_IN_F/                 ← 2-D (station, time)
    …
  fluxnet_dd/                ← daily group, same structure
  fluxnet_mm/
  fluxnet_ww/
  fluxnet_yy/
  meteosens/                 ← profile vars on (station, time, r, h, v)
```

**Hard parts**:
- Each station has different `*_level` dims for soil, profile, etc.
  Either:
  1. Take the union of all station's level coordinates and pad missing
     stations with NaN at unused indices (waste OK for sparse cases).
  2. Drop the per-station-specific `*_level` dims from the combined
     store; keep only variables whose dims are in the universal set
     `{station, time, ustar_threshold, nee_variant, partition_method,
       corr_pct, soil_layer}`.

  Option 1 keeps everything but bloats the store; option 2 loses
  station-specific level data. **Pragmatic answer: option 2** for the
  combined store, while keeping the existing per-station store
  alongside for variables with station-specific axes.

- `meteosens` variables have `(time, r, h, v)` with per-station r/h/v
  cardinality. Truly station-specific — keep in the per-station store
  only; don't include in the combined store.

### Option B — keep per-station groups; add a sibling combined group

Don't migrate; add a NEW combined group alongside:

```
icos-fluxnet.zarr/
  SE-Svb/                ← unchanged
  NL-Loo/                ← unchanged
  …
  _combined/             ← new sibling
    fluxnet_mm/
      station/ lat/ lon/ …
      time/
      NEE/ GPP/ RECO/ TA_F/ …
```

Pros: zero risk to existing consumers; both views coexist; combined
view only needs the variables we actually want to filter on (NEE,
GPP, RECO, LE, H, TA_F, SW_IN_F, P_F, VPD_F, …).

Cons: a small subset of variables, manually curated. The full
multi-dim NEE etc. with all 11 nee_variants × 2 ustar_thresholds *can*
be carried over — just keeps the dim count to manageable levels.

→ **Recommend B** for fluxnet (low-risk, additive) and a per-gas
combined group for obspack (one combined group per gas, each gas being
~1/4 of the data).

## Implementation outline

### New script `combine_to_dim.py`

A separate script (not folded into `fluxnet2zarr.py` / `obspack2zarr.py`)
that reads an existing per-station store and writes a combined view.

```bash
python combine_to_dim.py obspack \
    --in icos-obspack.zarr  --out icos-obspack.zarr  --gas co2 ch4 n2o co
python combine_to_dim.py fluxnet \
    --in icos-fluxnet.zarr  --out icos-fluxnet.zarr \
    --freq dd mm ww yy \
    --vars NEE GPP RECO LE_CORR H_CORR TA_F SW_IN_F P_F VPD_F NETRAD_F
```

Steps for obspack/co2:
1. Walk `icos-obspack.zarr/<sid>/co2` for every station that has co2.
2. Build the union time axis: `time_union = sorted(set ∪ of all time_co2)`.
3. Allocate target arrays `(n_stations, len(time_union))` filled with
   NaN (or `_FillValue` for ints).
4. For each station, compute the index map from its `time_co2` →
   `time_union`, scatter values in.
5. Build coord arrays: `station` (object), `lat`, `lon`,
   `intake_height` (each by reading the per-station `.zattrs`).
6. Write as `icos-obspack.zarr/co2_combined/` (separate group, doesn't
   touch existing per-station data).

Steps for fluxnet:
- Same idea but per frequency (`fluxnet_mm`, etc.) and limited to
  curated variables.

### Sanity checks

- Round-trip query: pick one station + variable, compare values from
  the combined view to the per-station view (with the same time slice).
- Filter test: NL bounding box query collapses to one xarray expression.
- Storage cost: report the combined store's on-disk size; should be
  roughly proportional to (n_stations × union_time_length × n_vars).

### Migration

The per-station groups stay where they are; the combined groups are
purely additive. So no data migration, no consumer break. Once the
combined view is validated, `nl_2024_minimal.ipynb` and similar notebooks
get a much shorter form.

## Risks / open questions

1. **Time axis bloat for obspack**. Some stations have 50 years of
   hourly CO2; others have 6 months. The union time axis will be the
   long one, and short stations will be 99% NaN. For 100 stations × 50
   years × hourly = 4.4 G floats per gas = 17 GB if dense. zarr chunks
   can be sparse but the index is still huge. **Mitigation**: chunk
   the combined arrays by station × time-year, so missing-data chunks
   skip to disk only when they have any value at all. Need a small
   experiment to verify zarr 2 v2's sparse-chunk behaviour with NaN-only
   chunks.

2. **Per-station `*_level` dims** in fluxnet. Soil variables `TS`, `SWC`
   have `soil_layer` of varying length per station. If we union the
   layers, we mix layers that mean different physical depths across
   stations (BE-Lon's layer 3 is not necessarily DE-Tha's layer 3).
   Better: omit layered variables from the combined view, or expose
   only the `_F` (gap-filled, single-value) versions.

3. **`obspack_id`** is a per-sample 200-byte string. Across 100 stations
   × 100k samples × 200 bytes = 2 GB. Feasible but expensive. **Decide**
   whether obspack_id needs to be in the combined view or only in the
   per-station view.

4. **`_provenance`**. Combined group will need its own provenance
   summarising all source DOIs, with a `station` coord-aligned
   `source_doi` array if we want one-DOI-per-station traceability.

## Estimated effort

- `combine_to_dim.py`: ~400 lines, ~1 day to write + smoke-test
- Validation: ~½ day (round-trip queries + storage measurements)
- Notebook updates: ~½ day to rewrite `nl_2024_minimal.ipynb` and
  similar in the new combined-view style
- Documentation in `README.md` and `obspack_zarr_readme.md`: ~½ day

Total: ~2.5 days work.

## Bonus — simplifies the data passport

Today's passport records the SHA-256 of every chunk the proxy served,
because at the proxy level all we see are byte ranges — not what the user
*meant* to fetch. That works but ties provenance to a low-level
implementation detail (chunk layout) rather than to the scientific intent.

With the combined-view store, the user's full query is a single xarray
expression:

```python
(ds["co2"]
   .where((ds.lat.between(50.7, 53.6)) & (ds.lon.between(3.3, 7.3)),
          drop=True)
   .sel(time_co2=slice("2024-01-01", "2024-12-31")))
```

Given a content-addressed snapshot of the store, this expression is
sufficient to reproduce the result bit-for-bit. The passport collapses
from "list every byte delivered" to "record what was asked":

```json
{
  "store_doi":   "10.18160/...",
  "store_pid":   "hdl:11676/...",
  "store_sha":   "<sha-256 of consolidated .zmetadata of the combined group>",
  "query": {
    "group":  "co2",
    "where":  "(lat >= 50.7) & (lat <= 53.6) & (lon >= 3.3) & (lon <= 7.3)",
    "sel":    {"time_co2": ["2024-01-01", "2024-12-31"]},
    "result_sha": "<sha-256 of materialized DataArray bytes>"
  }
}
```

Reproducibility check: re-open the store at `store_sha`, run the query,
hash the result, compare against `result_sha`. One pass = whole passport
verified. No 134-chunk manifest needed.

**Chunk-level passports remain useful in two cases**:

1. Streaming clients that bypass `datapassport_zarr` — the proxy still
   can't see the query, only bytes. The `X-DataPassport-Warning` header
   logic stays for those.
2. Partial deliveries (timeout / disconnect) where some chunks were
   missed. Chunk SHAs prove what was vs. wasn't transferred.

For the wrapped-client path (the recommended one), the query-level
passport is smaller, more meaningful, and content-addresses the
scientific output rather than the byte stream. **Action**: once the
combined view is in place, add a `query` block to `passport.build()` and
demote the chunk manifest to a fallback for unwrapped clients.

## Decision points before starting

- [ ] Confirm Option B (additive combined groups) is acceptable, vs
      replacing per-station layout.
- [ ] Pick which fluxnet variables go into the combined view.
- [ ] Decide whether `obspack_id` is included in combined obspack.
- [ ] Confirm sparse chunking strategy with a 2-station prototype before
      scaling up.
