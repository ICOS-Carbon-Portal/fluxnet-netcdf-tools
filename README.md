# ICOS / FLUXNET → CF-1.12 NetCDF4 conversion toolkit

Python scripts to convert ICOS ETC L2 CSV data (FLUXES, FLUXNET, METEO,
METEOSENS) to CF-1.12–compliant NetCDF4 files.  Station metadata and
citations are fetched live from the ICOS Carbon Portal.

## Scripts

### `fluxnet2nc.py` — single-file conversion

Converts one ICOS/FLUXNET CSV to a stand-alone NetCDF4 file.

```
python fluxnet2nc.py ICOSETC_SE-Svb_FLUXES_INTERIM_L2.csv
python fluxnet2nc.py ICOSETC_SE-Svb_FLUXES_INTERIM_L2.csv --doi 10.18160/R3G6-Z8ZH
python fluxnet2nc.py ICOSETC_SE-Svb_FLUXES_INTERIM_L2.csv \
       --output ./out/SE-Svb_FLUXES.nc
```

| Option | Default | Description |
|---|---|---|
| `csv` | (required) | Input CSV file |
| `--output FILE` | same dir as input | Output `.nc` path |
| `--doi DOI` | — | Collection DOI; adds `source_doi` and `PartOfDataset` (APA citation) global attrs |

---

### `fluxnet_restructure.py` — multi-file station restructure

Combines all CSV products for one station into a single, hierarchically
grouped NetCDF4 file (one group per product type).

```
python fluxnet_restructure.py ICOSETC_SE-Svb_*_L2.csv
python fluxnet_restructure.py ICOSETC_SE-Svb_*_L2.csv \
       --site-id SE-Svb --doi 10.18160/R3G6-Z8ZH --output ./out/SE-Svb.nc
```

| Option | Default | Description |
|---|---|---|
| `csv ...` | (required) | One or more input CSV files |
| `--site-id ID` | parsed from filename | Station ID (e.g. `SE-Svb`) |
| `--output FILE` | `ICOSETC_{site_id}_restructured.nc` | Output `.nc` path |
| `--doi DOI` | — | Collection DOI |
| `--comment TEXT` | — | Free-text comment added as global attribute |

---

### `icos_combined.py` — multi-station combined file

Merges all stations and all products into one NetCDF4 file with a
station dimension.

```
python icos_combined.py ICOSETC_*_FLUXES_*_L2.csv
python icos_combined.py ICOSETC_*_FLUXES_*_L2.csv \
       --doi 10.18160/R3G6-Z8ZH --output combined.nc
```

| Option | Default | Description |
|---|---|---|
| `csv ...` | (required) | Input CSV files (multiple stations allowed) |
| `--output FILE` | `icos_combined.nc` | Output `.nc` path |
| `--doi DOI` | — | Collection DOI |
| `--comment TEXT` | — | Free-text comment |

---

### `patch_citation.py` — backfill `citation` into existing files

Adds the `citation` global attribute to existing `.nc` files without
reprocessing — one HTTP request per station.

```
python patch_citation.py 10.18160/R3G6-Z8ZH
python patch_citation.py 10.18160/R3G6-Z8ZH --ncdir /data/icos_l2
python patch_citation.py 10.18160/R3G6-Z8ZH --overwrite
```

---

### `patch_instruments.py` — backfill `instrument_deployments` into existing files

Fetches METEOSENS instrument deployment metadata from the ICOS CP landing
page for each station and writes it as a per-variable `instrument_deployments`
attribute (JSON string) to the 4-D METEOSENS variables in existing `.nc` files.

```
python patch_instruments.py 10.18160/R3G6-Z8ZH
python patch_instruments.py 10.18160/R3G6-Z8ZH --ncdir /data/icos_l2
python patch_instruments.py 10.18160/R3G6-Z8ZH --station SE-Svb
python patch_instruments.py 10.18160/R3G6-Z8ZH --overwrite
```

| Option | Default | Description |
|---|---|---|
| `doi` | (required) | Collection DOI |
| `--ncdir DIR` | `.` | Directory containing `.nc` files |
| `--pattern GLOB` | `*_restructured.nc` | Filename pattern |
| `--station ID ...` | all | Limit to specific station IDs |
| `--overwrite` | off | Replace attribute if already present |

---

### `fluxnet2zarr.py` — zarr store from DOI (no intermediate .nc files)

Resolves an ICOS collection DOI, downloads the ARCHIVE zip for each station,
extracts the needed CSVs, and writes directly to a zarr v2 store.  No
intermediate `*_restructured.nc` files are created.

```
# Populate one station
python fluxnet2zarr.py 10.18160/R3G6-Z8ZH --station SE-Svb

# Populate all 35 stations
python fluxnet2zarr.py 10.18160/R3G6-Z8ZH

# Custom store location, keep intermediate files
python fluxnet2zarr.py 10.18160/R3G6-Z8ZH \
       --store /data/icos.zarr --keep-zip --keep-csv

# Manage the store
python fluxnet2zarr.py list
python fluxnet2zarr.py info  SE-Svb
python fluxnet2zarr.py remove SE-Svb
```

**`populate` options:**

| Option | Default | Description |
|---|---|---|
| `doi` | (required) | Collection DOI (with or without `https://doi.org/` prefix) |
| `--store DIR` | `icos-fluxnet.zarr` | Zarr store directory |
| `--station ID ...` | all | Limit to specific station IDs |
| `--outdir DIR` | `.` | Directory for temporary downloads and CSVs |
| `--keep-zip` | off | Keep downloaded ARCHIVE zip after extraction |
| `--keep-csv` | off | Keep extracted CSV files after ingestion |
| `--comment TEXT` | — | Free-text comment added as a global attribute |

**Store layout:**

```
icos-fluxnet.zarr/
  SE-Svb/               ← root zarr group: merged HH data
    .zgroup
    .zattrs             ← CF global attrs + _provenance JSON
    time/               ← zarr arrays
    NEE/                ← 3-D: (time, ustar_threshold, nee_variant)
    TA/                 ← 4-D: (time, r, h, v) for METEOSENS profiles
    …
    fluxnet_dd/         ← daily aggregated sub-group
    fluxnet_mm/         ← monthly aggregated sub-group
    fluxnet_ww/         ← weekly aggregated sub-group
    fluxnet_yy/         ← yearly aggregated sub-group
  DE-Hai/
    …
```

Each station group's `.zattrs` contains a `_provenance` JSON key with
`created`, `last_updated`, `archive`, `source_doi`, `citation`, and a
`history` list recording every ingest action.

**Reading with xarray:**

```python
import xarray as xr
ds = xr.open_zarr("icos-fluxnet.zarr", group="SE-Svb")
print(ds["NEE"])   # DataArray(time, ustar_threshold, nee_variant)
```

---

### `obspack2zarr.py` — zarr store from an ICOS Obspack collection

Companion to `fluxnet2zarr.py` for the ICOS atmosphere thematic centre.
Resolves an Obspack collection DOI (CO2 / CH4 / N2O / CO data),
downloads the per-station CF-NetCDF files, and ingests them into a zarr v2
store.  Multiple gases at the same station + sampling height share a single
group; each gas keeps its own time dimension because instruments and
cadences differ between gases.

```
# Populate one station+height
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --station HTM150

# Limit to specific gases
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2 --gas co2 ch4

# Populate all 357 files (~70 stations, ~65 min)
python obspack2zarr.py populate 10.18160/1PZ9-SDJ2

# Manage the store
python obspack2zarr.py list
python obspack2zarr.py info  HTM150
python obspack2zarr.py remove HTM150
```

**Station ID convention**: trigram + sampling height (m a.g.l. rounded to
integer), e.g. `HTM150`, `CBW207`, `CMN0`.  Surface stations get height `0`.

**Store layout**:

```
icos-obspack.zarr/
  HTM150/                ← root group: all gases at this trigram + height
    .zgroup
    .zattrs              ← static site metadata + ATC landing-page enrichment
                           + _provenance JSON
    co2/                 ← rescaled to ppm; calibration_scale on the array
    co2_std_dev/
    co2_qc_flag/
    …
    ch4/                 ← rescaled to ppb
    ch4_std_dev/
    …
    n2o/  co/
    time_co2/  time_ch4/ …  ← per-gas time axes
  CBW207/  …
  CMN0/  …
```

Static columns that don't vary along the time axis (`latitude`, `longitude`,
`altitude`, `intake_height`, `instrument`) are promoted to group `.zattrs`
instead of being stored as duplicated 1-D arrays.

**Reading with xarray**:

```python
import xarray as xr
ds = xr.open_zarr("icos-obspack.zarr", group="HTM150")
print(ds["co2"])         # ppm  ─ time_co2 axis
print(ds["ch4"])         # ppb  ─ time_ch4 axis
print(ds.attrs["intake_height"], ds.attrs["site_latitude"])
```

`explore_obspack.ipynb` provides a notebook viewer with cascading
trigram → height → gas dropdowns and a time-series plot.

For full design rationale, observed Obspack file structure, and future
work (custom Obspack export, interactive map viewer), see
`obspack_zarr_readme.md`.

The same `run_proxy.py` (below) serves the Obspack store too — point a
client at `http://host:port/icos-obspack.zarr/HTM150` exactly like the
Fluxnet store.

---

### `run_proxy.py` + `datapassport_zarr.py` — zarr data passport proxy

Serves one or more zarr stores as standard zarr v2 HTTP stores and
automatically generates a **data passport** for every client session.

```
python run_proxy.py [--host HOST] [--port PORT] [--store-dir DIR]
```

Each zarr store inside `--store-dir` is served under its directory name:

```
GET /icos-fluxnet.zarr/{key}   →  icos-fluxnet.zarr/.zgroup, chunks, …
GET /other-store.zarr/{key}    →  other-store.zarr/…
GET /                          →  {"stores": ["icos-fluxnet.zarr", …]}
```

**How it works**

1. The proxy (FastAPI) records every zarr chunk served, keyed by `(client IP, store)`.
2. When a session closes, it builds a ROCrate JSON-LD passport with
   SHA-256 checksums of all delivered data, mints a Handle PID, uploads
   the passport to the ICOS Carbon Portal, and fires a Matomo usage event.
3. The client receives the Handle PID synchronously via `POST /{store}/session/close`.

**Recommended client — `datapassport_zarr.open_zarr()`**

`DataPassportDataset` wraps `xr.Dataset` and is a drop-in replacement.
It records every `.sel()` / `.isel()` call as the query log and sends it
to the proxy on close.  Passport covers only the data actually delivered
(lazy arrays that were never computed are not included).

```python
from datapassport_zarr import open_zarr

# Context manager — passport minted automatically on __exit__
with open_zarr("http://localhost:8000/icos-fluxnet.zarr", group="SE-Svb") as ds:
    nee = ds["NEE"].sel(ustar_threshold="VUT", nee_variant="REF") \
                   .isel(time=slice(0, 100)).values
    ta  = ds["TA_F"].values
# Passport minted : hdl:11676/3f2a1b9c-...
# Landing page    : https://meta.icos-cp.eu/objects/...
# Saved to        : .passport/20260416T210000_SE-Svb.json

# Explicit close — useful in notebooks; returns the full info dict
ds = open_zarr("http://localhost:8000/icos-fluxnet.zarr", group="SE-Svb/fluxnet_dd")
gpp = ds["GPP"].values
info = ds.close()
print(info["passport_pid"])    # "hdl:11676/..."
print(info["queries"])         # recorded selection steps
```

A `__del__` safety net fires the close if the user never calls it
explicitly (e.g. script exits without using a context manager).

Clients that do not use `datapassport_zarr` receive an
`X-DataPassport-Warning` response header on every chunk request,
reminding them to install the wrapper.

`open_zarr` options:

| Argument | Default | Description |
|---|---|---|
| `proxy_url` | (required) | Base URL including store name, e.g. `http://localhost:8000/icos-fluxnet.zarr` |
| `group` | `""` | Zarr group, e.g. `"SE-Svb"` or `"SE-Svb/fluxnet_dd"` |
| `save_passport` | `True` | Save passport JSON to `passport_dir` on close |
| `passport_dir` | `".passport/"` | Directory for saved passport files |
| `verbose` | `True` | Print PID and landing page on close |
| `**xr_kwargs` | — | Forwarded to `xr.open_zarr()` |

**Proxy HTTP endpoints**

| Endpoint | Description |
|---|---|
| `GET /` | List available zarr stores |
| `GET /{store}/` | Serve root `.zgroup` for a store |
| `GET /{store}/{key}` | Serve zarr key (metadata or chunk); chunks are tracked |
| `POST /{store}/session/close` | Close session, mint passport synchronously, return PID |
| `GET /{store}/session/passport` | Retrieve PID for current/last session (polling fallback) |

`POST /{store}/session/close` accepts an optional JSON body
`{"queries": [...]}` (sent automatically by `datapassport_zarr`) and returns:

```json
{
  "passport_pid":  "hdl:11676/3f2a1b9c-...",
  "passport_url":  "https://meta.icos-cp.eu/objects/...",
  "chunks":        134,
  "bytes_served":  1851103,
  "arrays":        ["NEE", "TA_F", "time", ...],
  "queries":       [...]
}
```

If the client never calls `/{store}/session/close`, the idle-timeout reaper
(default 5 min) mints the passport automatically.

**Demo notebook**

`explore_zarr.ipynb` demonstrates the full passport workflow.  Set
`USE_PROXY = True` at the top of the notebook, start the proxy, and run
all cells — a passport is minted automatically on the final cell.

```python
# explore_zarr.ipynb — top cell
USE_PROXY  = True                                    # False → read store directly
PROXY_URL  = "http://localhost:8000/icos-fluxnet.zarr"
```

```bash
# Start the proxy (separate terminal)
python run_proxy.py --store-dir .
```

**Landing page**

`zarr_proxy/render_passport.py` generates an HTML landing page from a
saved `.jsonld` passport file:

```python
from zarr_proxy.render_passport import render
import pathlib
html = render(pathlib.Path("passports/fed285a18bddad47.jsonld"))
pathlib.Path("landing.html").write_text(html)
```

**Configuration** (environment variables):

| Variable | Default | Description |
|---|---|---|
| `ZARR_STORE_DIR` | `.` | Directory containing one or more zarr stores |
| `SESSION_TIMEOUT_SEC` | `300` | Idle seconds before session closes automatically |
| `HANDLE_PREFIX` | `11676` | EPIC Handle prefix |
| `HANDLE_ENDPOINT` | `https://epic5.storage.surfsara.nl/api/handles` | Handle REST API |
| `HANDLE_TOKEN` | — | Bearer token for Handle minting |
| `CP_META_UPLOAD` | `https://meta.icos-cp.eu/upload` | ICOS CP metadata upload endpoint |
| `CP_AUTH_URL` | `https://cpauth.icos-cp.eu/password/login` | CPauth login |
| `CP_USERNAME` / `CP_PASSWORD` | — | CP credentials |
| `CP_SUBMITTER_ID` | — | CP submitter ID (assigned by CP team) |
| `CP_OBJ_SPEC_URL` | — | DataPassport object type URL (TBD with CP team) |
| `MATOMO_URL` | — | Matomo base URL |
| `MATOMO_SITE_ID` | — | Matomo site ID |
| `MATOMO_TOKEN` | — | Matomo auth token |
| `PASSPORT_DIR` | `passports/` | Local directory for saved passport files |

**Module layout:**

| File | Responsibility |
|---|---|
| `run_proxy.py` | CLI launcher (`--host`, `--port`, `--store-dir`) |
| `datapassport_zarr.py` | Client wrapper: `open_zarr()`, `DataPassportDataset`, `_TrackedArray` |
| `zarr_proxy/main.py` | FastAPI app, multi-store router, session endpoints |
| `zarr_proxy/session.py` | `(IP, store)` session accumulator, idle-timeout reaper |
| `zarr_proxy/passport.py` | ROCrate JSON-LD builder, SHA-256 checksums |
| `zarr_proxy/render_passport.py` | HTML landing page generator from `.jsonld` passports |
| `zarr_proxy/handle_client.py` | EPIC Handle PID minting and updating |
| `zarr_proxy/cp_client.py` | ICOS CP metadata upload via CPauth |
| `zarr_proxy/matomo_client.py` | Server-side Matomo tracking event |
| `zarr_proxy/config.py` | All configuration with env-var overrides |

**Example data passport** (`passports/{sha256[:16]}.jsonld`):

```json
{
  "@context": [
    "https://w3id.org/ro/crate/1.1/context",
    {"icos": "https://meta.icos-cp.eu/ontology/cpmeta/"}
  ],
  "@graph": [
    {
      "@id": "./",
      "@type": "Dataset",
      "hasPart": [{"@id": "SE-Svb/NEE"}, {"@id": "SE-Svb/TA_F"}]
    },
    {
      "@id": "hdl:11676/3f2a1b9c-7e4d-4a2f-8c1e-9d5f6a7b8c9d",
      "@type": ["Dataset", "icos:DataPassport"],
      "name": "Data access passport — NEE, TA_F (2026-04-16)",
      "url": "https://meta.icos-cp.eu/objects/passport_3f2a1b9c",
      "dateAccessed": "2026-04-16T21:00:00Z",
      "sessionStart": "2026-04-16T21:00:00Z",
      "sessionEnd":   "2026-04-16T21:03:32Z",
      "agent": {
        "@type": "Agent",
        "ipAnonymised": "192.168.1.0/24"
      },
      "accessedGroups": ["SE-Svb"],
      "accessedArrays": ["NEE", "TA_F"],
      "query": [
        {"variable": "NEE", "group": "SE-Svb"},
        {"variable": "NEE", "group": "SE-Svb",
         "sel": {"ustar_threshold": "VUT", "nee_variant": "REF"}},
        {"variable": "NEE", "group": "SE-Svb",
         "isel": {"time": {"start": 0, "stop": 100, "step": null}}},
        {"variable": "TA_F", "group": "SE-Svb"}
      ],
      "hasPart": [
        {
          "@id": "SE-Svb/NEE",
          "name": "NEE",
          "zarr_path": "SE-Svb/NEE",
          "sha256": "b8185a24d20970258b657bfa28b714d16ace1b1ec44a25566d13539bc18812fe",
          "sizeInBytes": 12288,
          "chunkCount": 3
        },
        {
          "@id": "SE-Svb/TA_F",
          "name": "TA_F",
          "zarr_path": "SE-Svb/TA_F",
          "sha256": "effc3f966be6fde17d0d1b61f52f9a5b7fa35472e08861e7b3f42353a513affb",
          "sizeInBytes": 4096,
          "chunkCount": 1
        }
      ],
      "totalBytesServed": 16384,
      "totalChunks": 4,
      "isPartOf": {"@id": "https://doi.org/10.18160/R3G6-Z8ZH"},
      "citation": "Peichl et al. (2025). ETC L2 ARCHIVE from Svartberget …",
      "passportSha256": "fed285a18bddad4712bc1a86002fb20fd5ea9c39ba50e2eebe47a965d378c9c3"
    }
  ]
}
```

Key passport fields:

| Field | Description |
|---|---|
| `@id` | Handle PID minted for this passport (`hdl:11676/…`) |
| `url` | ICOS CP landing page for the passport |
| `agent.ipAnonymised` | Caller IP anonymised to /24 (IPv4) or /48 (IPv6); full IP logged server-side and sent to Matomo |
| `query` | Ordered list of xarray variable accesses and `.sel()`/`.isel()` selections made by the client |
| `hasPart[].sha256` | SHA-256 of sorted chunk digests for each delivered array |
| `passportSha256` | SHA-256 of the complete passport JSON (with this field set to `null`) — guarantees self-describing integrity |

---

### `icos_download_restructure.py` — full pipeline from DOI

Resolves an ICOS collection DOI, downloads the ARCHIVE zip for each
station, extracts the needed CSVs, runs `fluxnet_restructure`, and
cleans up.  Requires no pre-downloaded files.

```
# All stations in the collection
python icos_download_restructure.py 10.18160/R3G6-Z8ZH

# Specific stations only
python icos_download_restructure.py 10.18160/R3G6-Z8ZH --station SE-Svb DE-Hai

# Custom output directory, keep intermediate files
python icos_download_restructure.py 10.18160/R3G6-Z8ZH \
       --outdir /data/icos_l2 --keep-zip --keep-csv
```

| Option | Default | Description |
|---|---|---|
| `doi` | (required) | Collection DOI (with or without `https://doi.org/` prefix) |
| `--outdir DIR` | `.` | Directory for downloads and output `.nc` files |
| `--station ID ...` | all | Limit to specific station IDs |
| `--keep-zip` | off | Keep downloaded ARCHIVE zip after extraction |
| `--keep-csv` | off | Keep extracted CSV files after restructuring |
| `--comment TEXT` | — | Free-text comment forwarded to restructure |

Pipeline steps:

1. DOI → ICOS CP collection URL via JSON-LD content negotiation
2. Collection → list of `*_ARCHIVE_*` members (one per station)
3. Per station: stream-download zip via licence-accept endpoint
4. Extract only the needed CSV products (FLUXES, FLUXNET_HH/DD/WW/MM/YY,
   METEO, METEOSENS); skip ANCILLARY, AUXDATA, VARINFO, PDFs, etc.
5. Run `fluxnet_restructure.restructure()` → one `.nc` file per station
6. Delete CSVs and zip (unless `--keep-*` flags are set)

---

## Global attributes written to every output file

| Attribute | Source |
|---|---|
| `geospatial_lat_min/max` | ICOS CP station landing page (JSON) |
| `geospatial_lon_min/max` | ICOS CP station landing page (JSON) |
| `geospatial_vertical_min/max` | elevation from station landing page |
| `station_class` | ICOS labelling class (1 or 2) |
| `station_labeling_date` | date of ICOS labelling |
| `time_zone` | station timezone |
| `ecosystem_type` | main ecosystem type |
| `climate_zone` | Köppen-Geiger climate zone |
| `mean_annual_temperature` | °C, from station page |
| `mean_annual_precipitation` | mm, from station page |
| `documentation` | URL to station documentation |
| `current_staff` | comma-separated list of station PIs / staff |
| `source_doi` | Canonical DOI / handle URL of the source data object |
| `PartOfDataset` | APA-style citation fetched from doi.org (populated when `--doi` is given) |
| `citation` | Pre-formatted citation string from the ICOS CP data object landing page (populated by the download pipeline) |
| `Conventions` | `CF-1.12` |

## Variable attribute: `instrument_deployments` (METEOSENS only)

Each 4-D METEOSENS variable (e.g. `TA`, `LW_IN`, `SWC`) carries an
`instrument_deployments` attribute — a compact JSON array describing which
physical instruments measured that variable, their exact deployment location,
and the active deployment period.

```json
[
  {"r": 1, "h": 1, "v": 1,
   "instrument": "TA-HMP155 (2483)",
   "instrument_uri": "http://meta.icos-cp.eu/resources/instruments/ETC_…",
   "instrument_description": "Relative humidity and temperature probe, Vaisala, HMP155",
   "lat": 56.097, "lon": 13.419, "alt": 28.0,
   "start": "2018-01-01T00:00:00Z", "stop": "2022-06-15T12:00:00Z"},
  {"r": 1, "h": 1, "v": 1,
   "instrument": "TA-HMP155 (3101)", …, "start": "2022-06-15T12:00:00Z", "stop": null}
]
```

- `r / h / v` — indices into the `(pos_r, height_h, vrep_v)` dimensions of the variable
- `stop: null` — deployment is ongoing
- Sourced from the ICOS CP METEOSENS data object landing page via content negotiation
- Use `patch_instruments.py` to backfill existing files

---

## Installation

```bash
pip install -r requirements.txt
```

Or individually:

```bash
pip install numpy pandas netCDF4 xarray "zarr<3" fastapi "uvicorn[standard]" fsspec aiohttp
```

Python 3.10+ required.

---

## Data source

ICOS Ecosystem Thematic Centre L2 data distributed via the
[ICOS Carbon Portal](https://www.icos-cp.eu/).  Data use is subject to
the [ICOS data licence](https://www.icos-cp.eu/data-services/about-data-portal/data-license).
