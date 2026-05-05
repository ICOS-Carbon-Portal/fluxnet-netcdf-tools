#!/usr/bin/env python3
"""
GPP GeoTIFF batch converter.

Converts a directory of daily GPP GeoTIFFs (one per day, named
``GPP_YYYY_MM_DD.tif``, Int16 × 0.001 g C m⁻² d⁻¹ in EPSG:4326) into
either:

  • one CF-1.12 NetCDF-4 file **per month** stacking every day in that
    month along a ``time`` axis (default mode), or
  • a single combined zarr v2 store with one ``time`` axis spanning the
    whole archive (--zarr mode); store name defaults to
    ``EU-GPP-2014-2023.zarr``.

Both modes apply *lossy quantization* to **0.1 g C m⁻² d⁻¹**: raw values
are converted to physical units (×0.001), rounded to 1 decimal, and
re-encoded as Int16 with ``scale_factor=0.1`` and ``add_offset=0``.  The
range becomes 0…~21 000, comfortably inside the 16-bit signed limit.
This shrinks deflate-encoded outputs by 3-5× compared with full-precision
storage at no scientifically meaningful cost.

Per-month stacking is much more efficient than per-day NetCDF because
deflate compresses the (lat, lon) chunks individually, and the
~30-day stack amortizes the file-header / chunk-index overhead across
many days.

Usage
-----
    # Monthly-stack NetCDF mode (default; writes one .nc per month)
    python gpp_tif2nc.py /path/to/tifs --outdir /data/gpp_monthly

    # Append to / build a combined zarr store
    python gpp_tif2nc.py /path/to/tifs --zarr ./EU-GPP-2014-2023.zarr

    # Both at once
    python gpp_tif2nc.py /path/to/tifs --outdir /data/gpp_monthly \
                                       --zarr   ./EU-GPP-2014-2023.zarr

    # Limit to a date range
    python gpp_tif2nc.py /path/to/tifs --start 2014-05-01 --end 2014-12-31

Dependencies
------------
    pip install numpy "zarr<3" netCDF4
    # plus the gdal CLI for reading TIFFs:  apt-get install gdal-bin

(Doesn't depend on rasterio / python-gdal — uses ``gdal_translate`` to
emit a temporary headerless raw raster, then reads with NumPy.  Saves a
heavyweight binding install across machines.)
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

try:
    from netCDF4 import Dataset as NCDataset
except ImportError:
    print("netCDF4 not found — install with: pip install netCDF4", file=sys.stderr)
    sys.exit(1)

# zarr is optional (only needed for --zarr mode)
try:
    import zarr
    HAVE_ZARR = True
except ImportError:
    HAVE_ZARR = False


_FN_RE = re.compile(r"^GPP_(\d{4})_(\d{2})_(\d{2})\.tif$", re.IGNORECASE)

# Quantization parameters — see module docstring.
RAW_SCALE_FACTOR = 0.001    # raw Int16 × 0.001 → g C m⁻² d⁻¹  (input convention)
OUT_SCALE_FACTOR = 0.1      # output Int16 × 0.1 → g C m⁻² d⁻¹  (after quantization)
NODATA = -9999

VAR_NAME       = "gpp"
VAR_LONG_NAME  = "Gross primary production (carbon)"
# CF Standard Name Table v89 has no terrestrial-GPP entry, so we deliberately
# omit `standard_name` for the gpp variable — long_name + units are the CF
# fallback when no standard name fits.
VAR_UNITS      = "g m-2 day-1"


# ── Filename / discovery ──────────────────────────────────────────────────────

def parse_date_from_name(path: Path) -> date | None:
    m = _FN_RE.match(path.name)
    if not m:
        return None
    y, mo, d = (int(x) for x in m.groups())
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def discover_tifs(indir: Path, start: date | None, end: date | None) -> list[tuple[date, Path]]:
    out: list[tuple[date, Path]] = []
    for p in sorted(indir.iterdir()):
        if not p.is_file():
            continue
        d = parse_date_from_name(p)
        if d is None:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append((d, p))
    return out


# ── GeoTIFF reading via gdal_translate ────────────────────────────────────────

def _gdalinfo_json(path: Path) -> dict:
    try:
        proc = subprocess.run(
            ["gdalinfo", "-json", str(path)],
            check=True, capture_output=True, text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "gdalinfo not found on PATH — install gdal-bin (Linux/macOS) "
            "or 'gdal' (Windows OSGeo4W) and re-run."
        ) from exc
    return json.loads(proc.stdout)


def _read_tif_array(path: Path, info: dict) -> np.ndarray:
    """
    Read the first band as Int16 by writing a temporary ENVI raw raster
    via gdal_translate, then memmap'ing it.  This avoids requiring
    python-gdal / rasterio bindings.
    """
    width  = int(info["size"][0])
    height = int(info["size"][1])

    with tempfile.TemporaryDirectory() as td:
        raw_path = Path(td) / "band.bin"
        cmd = [
            "gdal_translate", "-q",
            "-of", "ENVI", "-ot", "Int16",
            "-b", "1",
            str(path), str(raw_path),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        arr = np.fromfile(raw_path, dtype="<i2").reshape(height, width)
        # Copy out of the temp dir before it's deleted
        return arr.copy()


def _grid_from_info(info: dict) -> tuple[np.ndarray, np.ndarray]:
    """Return 1-D lat (descending, top→bottom) and lon (ascending) coord arrays."""
    width  = int(info["size"][0])
    height = int(info["size"][1])
    gt = info["geoTransform"]      # (origin_x, dx, 0, origin_y, 0, dy)
    x0, dx, _, y0, _, dy = gt
    # Pixel-centre coords (GDAL origin is the corner of the upper-left pixel)
    lons = x0 + (np.arange(width)  + 0.5) * dx
    lats = y0 + (np.arange(height) + 0.5) * dy
    return lats, lons


# ── Quantization ──────────────────────────────────────────────────────────────

def encode(raw_int16: np.ndarray, *, quantize: bool) -> tuple[np.ndarray, float]:
    """
    Return (encoded_int16, scale_factor) ready for direct on-disk storage.

    - quantize=True (default): rescale to 0.1 g C m⁻² d⁻¹ resolution.
      raw × 0.001 → physical, rounded to 0.1, stored as Int16 with
      scale_factor=0.1.  ~3-5× smaller after deflate at no scientifically
      meaningful cost.

    - quantize=False: keep the raw Int16 bytes verbatim and just declare
      the producer's scale_factor=0.001, so decoded values are still in
      g C m⁻² d⁻¹.  Lossless round-trip; bigger files.

    NoData (-9999) is preserved either way.
    """
    if not quantize:
        return raw_int16.copy(), RAW_SCALE_FACTOR
    nd_mask  = (raw_int16 == NODATA)
    physical = raw_int16.astype(np.float32) * RAW_SCALE_FACTOR
    encoded  = np.round(physical / OUT_SCALE_FACTOR).astype(np.int16)
    encoded[nd_mask] = NODATA
    return encoded, OUT_SCALE_FACTOR


# ── Per-file NetCDF output ────────────────────────────────────────────────────

def write_monthly_netcdf(
    out_path: Path,
    days: list[date],
    stack: np.ndarray,                 # shape (n_days, lat, lon), Int16
    lats: np.ndarray,
    lons: np.ndarray,
    source_files: list[str],
    scale_factor: float,
) -> None:
    """Write a monthly stacked NetCDF (time, lat, lon) for one calendar month."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n_days = len(days)
    assert stack.shape == (n_days, lats.size, lons.size)

    with NCDataset(out_path, "w", format="NETCDF4_CLASSIC") as nc:
        nc.createDimension("time", n_days)
        nc.createDimension("lat",  lats.size)
        nc.createDimension("lon",  lons.size)

        v_time = nc.createVariable("time", "f8", ("time",))
        v_time.standard_name = "time"
        v_time.long_name     = "time"
        v_time.units         = "days since 1970-01-01 00:00:00"
        v_time.calendar      = "proleptic_gregorian"
        v_time.axis          = "T"
        epoch = date(1970, 1, 1)
        v_time[:] = np.asarray([(d - epoch).days for d in days], dtype="f8")

        v_lat = nc.createVariable("lat", "f4", ("lat",), zlib=True, complevel=4)
        v_lat.standard_name = "latitude"
        v_lat.long_name     = "latitude"
        v_lat.units         = "degrees_north"
        v_lat.axis          = "Y"
        v_lat[:] = lats

        v_lon = nc.createVariable("lon", "f4", ("lon",), zlib=True, complevel=4)
        v_lon.standard_name = "longitude"
        v_lon.long_name     = "longitude"
        v_lon.units         = "degrees_east"
        v_lon.axis          = "X"
        v_lon[:] = lons

        v = nc.createVariable(
            VAR_NAME, "i2", ("time", "lat", "lon"),
            fill_value=np.int16(NODATA),
            zlib=True, complevel=6, shuffle=True,
            chunksizes=(1, min(512, lats.size), min(512, lons.size)),
        )
        v.long_name      = VAR_LONG_NAME
        v.units          = VAR_UNITS
        v.scale_factor   = scale_factor
        v.add_offset     = 0.0
        v.grid_mapping   = "crs"
        # `stack` already holds final on-disk Int16 values (output of quantize()).
        # Disable mask/scale on write so netCDF4 stores bytes verbatim instead
        # of dividing by scale_factor again.
        v.set_auto_maskandscale(False)
        v[:, :, :] = stack

        # CRS (EPSG:4326 — geographic)
        v_crs = nc.createVariable("crs", "i4")
        v_crs.grid_mapping_name        = "latitude_longitude"
        v_crs.longitude_of_prime_meridian = 0.0
        v_crs.semi_major_axis          = 6378137.0
        v_crs.inverse_flattening       = 298.257223563
        v_crs.crs_wkt                  = (
            'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
            'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'
        )

        # Globals
        first, last = days[0], days[-1]
        nc.title       = (
            f"Gross Primary Production — {first.strftime('%Y-%m')} "
            f"({first.isoformat()} … {last.isoformat()}, {n_days} day(s))"
        )
        if scale_factor == OUT_SCALE_FACTOR:
            nc.summary = (
                "Daily GPP rasters quantized to 0.1 g C m-2 day-1, stacked along "
                "the time axis (one slice per day). Encoded as Int16 with "
                f"scale_factor={scale_factor}; NoData={NODATA}."
            )
        else:
            nc.summary = (
                "Daily GPP rasters in the producer's native scale, stacked along "
                "the time axis (one slice per day). Encoded as Int16 with "
                f"scale_factor={scale_factor}; NoData={NODATA}."
            )
        nc.Conventions = "CF-1.8"
        nc.history     = (
            f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} "
            f"created by gpp_tif2nc.py from {n_days} GeoTIFF(s)"
        )
        nc.source      = ", ".join(source_files)


# ── Combined zarr store ───────────────────────────────────────────────────────

def _open_or_create_zarr(
    store_path: Path, lats: np.ndarray, lons: np.ndarray, scale_factor: float,
) -> "zarr.Group":
    """
    Open an existing zarr store with a fixed (lat, lon) grid + a growable
    ``time`` axis, or create one matching the supplied lat/lon arrays.
    Returns the root group.
    """
    if not HAVE_ZARR:
        raise RuntimeError("zarr not installed — pip install 'zarr<3' to use --zarr mode")

    store_path = Path(store_path)
    if store_path.exists():
        grp = zarr.open_group(str(store_path), mode="a")
        # Sanity-check the grid against existing arrays
        if "lat" in grp and "lon" in grp:
            if grp["lat"].shape != lats.shape or grp["lon"].shape != lons.shape:
                raise RuntimeError(
                    f"Grid mismatch: existing store {store_path.name} has "
                    f"lat/lon shapes {grp['lat'].shape}/{grp['lon'].shape}, "
                    f"input has {lats.shape}/{lons.shape}"
                )
        return grp

    grp = zarr.open_group(str(store_path), mode="w")

    z_lat = grp.create_dataset("lat", data=lats.astype("f4"), chunks=(min(4096, lats.size),))
    z_lat.attrs.update({
        "_ARRAY_DIMENSIONS": ["lat"],
        "standard_name":     "latitude",
        "units":             "degrees_north",
    })

    z_lon = grp.create_dataset("lon", data=lons.astype("f4"), chunks=(min(4096, lons.size),))
    z_lon.attrs.update({
        "_ARRAY_DIMENSIONS": ["lon"],
        "standard_name":     "longitude",
        "units":             "degrees_east",
    })

    # time — initially empty, grows by 1 per day
    z_time = grp.create_dataset(
        "time",
        shape=(0,),
        chunks=(366,),
        dtype="f8",
        compressor=zarr.Blosc(cname="zstd", clevel=3),
    )
    z_time.attrs.update({
        "_ARRAY_DIMENSIONS": ["time"],
        "standard_name":     "time",
        "units":             "days since 1970-01-01 00:00:00 UTC",
        "calendar":          "proleptic_gregorian",
    })

    # gpp(time, lat, lon) — Int16 with quantized scale
    z_gpp = grp.create_dataset(
        VAR_NAME,
        shape=(0, lats.size, lons.size),
        chunks=(1, min(1024, lats.size), min(1024, lons.size)),
        dtype="i2",
        fill_value=np.int16(NODATA),
        compressor=zarr.Blosc(cname="zstd", clevel=5, shuffle=zarr.Blosc.SHUFFLE),
    )
    z_gpp.attrs.update({
        "_ARRAY_DIMENSIONS": ["time", "lat", "lon"],
        "long_name":         VAR_LONG_NAME,
        "units":             VAR_UNITS,
        "scale_factor":      scale_factor,
        "add_offset":        0.0,
        "_FillValue":        int(NODATA),
        "grid_mapping":      "crs",
    })

    # CRS
    grp.attrs.update({
        "title":         "Gross Primary Production — daily 2014-2023",
        "Conventions":   "CF-1.8",
        "institution":   "",
        "source":        "GPP daily GeoTIFFs",
        "history":       f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} created by gpp_tif2nc.py",
        "_provenance":   json.dumps({
            "created":      datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_doi":   "",
            "citation":     "",
            "history":      [],
        }),
    })
    return grp


def append_to_zarr(grp: "zarr.Group", file_date: date, quantized: np.ndarray) -> None:
    z_time = grp["time"]
    z_gpp  = grp[VAR_NAME]
    days = (file_date - date(1970, 1, 1)).days
    # Skip if this date is already in the store (idempotent re-runs)
    existing_times = z_time[:]
    if existing_times.size and float(days) in existing_times:
        idx = int(np.where(existing_times == float(days))[0][0])
        z_gpp[idx, :, :] = quantized
        return
    # Append
    n = z_time.shape[0]
    z_time.resize((n + 1,))
    z_time[n] = float(days)
    z_gpp.resize((n + 1, z_gpp.shape[1], z_gpp.shape[2]))
    z_gpp[n, :, :] = quantized


# ── Driver ────────────────────────────────────────────────────────────────────

def _read_and_encode(src: Path, *, quantize: bool) -> tuple[np.ndarray, float, np.ndarray, np.ndarray]:
    """Read one TIFF, return (encoded_int16, scale_factor, lats, lons)."""
    info = _gdalinfo_json(src)
    lats, lons = _grid_from_info(info)
    raw = _read_tif_array(src, info)
    enc, scale = encode(raw, quantize=quantize)
    return enc, scale, lats, lons


def _check_grid(
    src: Path, lats: np.ndarray, lons: np.ndarray,
    expected: tuple[np.ndarray, np.ndarray] | None,
) -> None:
    if expected is None:
        return
    e_lats, e_lons = expected
    if not (np.allclose(lats, e_lats, atol=1e-9) and np.allclose(lons, e_lons, atol=1e-9)):
        raise RuntimeError(
            f"{src.name}: lat/lon grid does not match the first file in this batch"
        )


def _group_by_month(files: list[tuple[date, Path]]) -> "dict[tuple[int,int], list[tuple[date, Path]]]":
    out: dict[tuple[int, int], list[tuple[date, Path]]] = defaultdict(list)
    for d, p in files:
        out[(d.year, d.month)].append((d, p))
    for k in out:
        out[k].sort()
    return dict(sorted(out.items()))


def _process_month(
    year: int, month: int,
    items: list[tuple[date, Path]],
    *,
    quantize: bool,
    nc_outdir: Path | None,
    zarr_grp: "zarr.Group | None",
    overwrite: bool,
    expected_grid: tuple[np.ndarray, np.ndarray] | None,
) -> tuple[np.ndarray, np.ndarray]:
    """Process all files for one calendar month: stack into one NetCDF + optionally append to zarr."""
    label = f"{year}-{month:02d}"
    nc_path: Path | None = None
    if nc_outdir is not None:
        nc_path = nc_outdir / f"GPP_{year}_{month:02d}.nc"
        if nc_path.exists() and not overwrite:
            print(f"  skip month (exists): {nc_path.name}")
            nc_path = None

    days: list[date] = []
    sources: list[str] = []
    stack: np.ndarray | None = None
    lats_first: np.ndarray | None = None
    lons_first: np.ndarray | None = None
    scale_factor_used: float | None = None

    for i, (d, src) in enumerate(items):
        enc, scale, lats, lons = _read_and_encode(src, quantize=quantize)
        _check_grid(src, lats, lons, expected_grid)
        if expected_grid is None:
            expected_grid = (lats, lons)
        if lats_first is None:
            lats_first, lons_first = lats, lons
            scale_factor_used = scale
            if nc_path is not None:
                stack = np.empty((len(items), lats.size, lons.size), dtype="i2")

        if stack is not None:
            stack[i, :, :] = enc
        days.append(d)
        sources.append(src.name)

        if zarr_grp is not None:
            append_to_zarr(zarr_grp, d, enc)

        print(f"    {d.isoformat()}  {src.name}")

    if nc_path is not None and stack is not None and scale_factor_used is not None:
        assert lats_first is not None and lons_first is not None
        write_monthly_netcdf(
            nc_path, days, stack, lats_first, lons_first, sources, scale_factor_used,
        )
        size_mb = nc_path.stat().st_size / 1e6
        src_mb  = sum(p.stat().st_size for _, p in items) / 1e6
        print(f"  wrote {nc_path.name}  {size_mb:.1f} MB "
              f"(month TIFF total {src_mb:.1f} MB · {len(items)} day(s))  [{label}]")

    return lats_first, lons_first  # type: ignore[return-value]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("indir", type=Path, help="Directory of GPP_YYYY_MM_DD.tif files")
    p.add_argument("--outdir", type=Path, default=None,
                   help="Output dir for monthly stacked .nc (default: alongside the input dir).")
    p.add_argument("--zarr", type=Path, default=None,
                   help="Append (or create) a combined zarr store at this path "
                        "(e.g. ./EU-GPP-2014-2023.zarr).")
    p.add_argument("--no-nc", action="store_true",
                   help="Skip writing per-month NetCDFs (only useful with --zarr).")
    p.add_argument("--start", type=date.fromisoformat, default=None,
                   metavar="YYYY-MM-DD", help="Earliest date to include")
    p.add_argument("--end",   type=date.fromisoformat, default=None,
                   metavar="YYYY-MM-DD", help="Latest date to include")
    p.add_argument("--overwrite", action="store_true",
                   help="Overwrite existing per-month NetCDFs")
    p.add_argument("--no-quantize", action="store_true",
                   help="Skip lossy quantization to 0.1 g C m⁻² d⁻¹; keep the "
                        "raw Int16 bytes verbatim with scale_factor=0.001 "
                        "(lossless, larger files).")
    args = p.parse_args(argv)
    quantize = not args.no_quantize

    if not args.indir.is_dir():
        sys.exit(f"Not a directory: {args.indir}")

    if shutil.which("gdal_translate") is None or shutil.which("gdalinfo") is None:
        sys.exit("gdal_translate / gdalinfo not on PATH — install gdal-bin first.")

    files = discover_tifs(args.indir, args.start, args.end)
    if not files:
        sys.exit(f"No GPP_YYYY_MM_DD.tif files found in {args.indir}")

    nc_outdir: Path | None = None
    if not args.no_nc:
        nc_outdir = (args.outdir or args.indir).resolve()

    by_month = _group_by_month(files)
    print(f"Found {len(files)} file(s) across {len(by_month)} month(s).")

    # Open zarr store on first file (need the grid).  Done lazily inside the loop.
    zarr_grp = None
    expected_grid: tuple[np.ndarray, np.ndarray] | None = None

    for (year, month), items in by_month.items():
        print(f"\n=== {year}-{month:02d}  ({len(items)} day(s)) ===")
        # Lazy zarr open: peek at the first file of the first month.
        if args.zarr is not None and zarr_grp is None:
            _, scale0, lats0, lons0 = _read_and_encode(items[0][1], quantize=quantize)
            zarr_grp = _open_or_create_zarr(args.zarr, lats0, lons0, scale0)
            expected_grid = (lats0, lons0)
        lats, lons = _process_month(
            year, month, items,
            quantize=quantize,
            nc_outdir=nc_outdir,
            zarr_grp=zarr_grp,
            overwrite=args.overwrite,
            expected_grid=expected_grid,
        )
        if expected_grid is None:
            expected_grid = (lats, lons)

    if zarr_grp is not None:
        zarr.consolidate_metadata(str(args.zarr))
        print(f"\nZarr store consolidated: {args.zarr}")

    print("\nDone.")


if __name__ == "__main__":
    main()
