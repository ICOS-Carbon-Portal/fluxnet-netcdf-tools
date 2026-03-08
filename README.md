# ICOS / FLUXNET → CF-1.12 NetCDF4 conversion toolkit

Python scripts to convert ICOS ETC L2 CSV data (FLUXES, FLUXNET, METEO,
METEOSENS) to CF-1.12–compliant NetCDF4 files.  Station metadata and DOI
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
| `--doi DOI` | — | Collection DOI; adds `source_doi` and `PartOfDataset` global attrs |

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
| `source_doi` | e.g. `https://doi.org/10.18160/R3G6-Z8ZH` |
| `PartOfDataset` | APA citation of the collection DOI |
| `Conventions` | `CF-1.12` |

---

## Installation

```bash
pip install numpy pandas netCDF4
```

Python 3.10+ required.  No other external dependencies — all network
requests use the standard library (`urllib`).

---

## Data source

ICOS Ecosystem Thematic Centre L2 data distributed via the
[ICOS Carbon Portal](https://www.icos-cp.eu/).  Data use is subject to
the [ICOS data licence](https://www.icos-cp.eu/data-services/about-data-portal/data-license).
