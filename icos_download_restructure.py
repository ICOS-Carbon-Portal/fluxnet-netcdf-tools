#!/usr/bin/env python3
"""
Download ICOS ETC L2 ARCHIVE zip files from a collection DOI, extract the
needed CSV files, and restructure them into CF-1.12 NetCDF4 using
fluxnet_restructure.py — one output file per station.

Usage:
    python icos_download_restructure.py 10.18160/R3G6-Z8ZH
    python icos_download_restructure.py 10.18160/R3G6-Z8ZH --station SE-Svb DE-Hai
    python icos_download_restructure.py 10.18160/R3G6-Z8ZH --outdir /data/icos_l2
    python icos_download_restructure.py 10.18160/R3G6-Z8ZH --keep-zip --keep-csv

Dependencies:
    pip install numpy pandas netCDF4
"""

import argparse
import http.cookiejar
import json
import re
import shutil
import sys
import time
import urllib.request
import zipfile
from argparse import Namespace
from pathlib import Path

# Import restructure function and helpers from sibling scripts
sys.path.insert(0, str(Path(__file__).parent))
from fluxnet2nc import fetch_doi_citation, parse_filename
from fluxnet_restructure import restructure


# ── Constants ─────────────────────────────────────────────────────────────────

# CSV filenames to extract from the ARCHIVE zip (all others are skipped)
_NEEDED_CSV = re.compile(
    r"ICOSETC_[^_]+-[^_]+_"
    r"(?:FLUXES|FLUXNET_(?:HH|DD|WW|MM|YY)|METEO|METEOSENS)"
    r"(?:_INTERIM)?_L2\.csv$",
    re.IGNORECASE,
)

# Station ID pattern inside ARCHIVE zip filename
_STATION_RE = re.compile(r"ICOSETC_([^_]+-\w+)_ARCHIVE", re.IGNORECASE)


# ── ICOS CP collection API ────────────────────────────────────────────────────

def resolve_collection_url(doi: str) -> str:
    """Resolve a DOI to its ICOS CP collection URL via JSON-LD content negotiation."""
    doi = doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    req = urllib.request.Request(
        f"https://doi.org/{doi}",
        headers={"Accept": "application/ld+json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.load(resp)
    url = data.get("url", "")
    if not url:
        raise RuntimeError(f"No 'url' field in JSON-LD for DOI {doi}: {list(data)}")
    return url


def get_archive_members(collection_url: str) -> list[dict]:
    """Fetch collection members and return only ARCHIVE entries.

    Each entry is a dict with keys: name, res, hash_id.
    """
    req = urllib.request.Request(
        collection_url, headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.load(resp)
    members = data.get("members", [])
    archives = []
    for m in members:
        if "_ARCHIVE_" not in m.get("name", ""):
            continue
        res = m["res"]                        # https://meta.icos-cp.eu/objects/{hash}
        hash_id = res.rsplit("/", 1)[-1]
        archives.append({"name": m["name"], "res": res, "hash_id": hash_id})
    return archives


# ── Download ──────────────────────────────────────────────────────────────────

def download_zip(hash_id: str, dest: Path, label: str = "") -> None:
    """Stream-download an ICOS object (accepting the data licence automatically).

    Uses the /licence_accept endpoint which sets the per-object cookie and
    immediately redirects to the actual zip download.
    """
    accept_url = (
        f"https://data.icos-cp.eu/licence_accept"
        f"?ids=%5B%22{hash_id}%22%5D"
    )
    jar  = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    tag = f"[{label}] " if label else ""
    print(f"  {tag}Downloading …", end="", flush=True)

    with opener.open(urllib.request.Request(accept_url), timeout=600) as resp:
        total = resp.headers.get("Content-Length")
        total_mb = f" ({int(total)/1e6:.0f} MB)" if total else ""
        print(f"{total_mb}", end="", flush=True)
        with open(dest, "wb") as fh:
            downloaded = 0
            chunk = 1 << 20   # 1 MB
            while True:
                buf = resp.read(chunk)
                if not buf:
                    break
                fh.write(buf)
                downloaded += len(buf)
                mb = downloaded / 1e6
                print(f"\r  {tag}Downloading … {mb:.0f} MB{total_mb}   ", end="", flush=True)
    print(f"\r  {tag}Downloaded  {downloaded/1e6:.1f} MB → {dest.name}    ")


# ── ZIP extraction ────────────────────────────────────────────────────────────

def extract_needed_csvs(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extract CSV files matching _NEEDED_CSV from the zip into dest_dir.

    Returns the list of extracted file paths.
    """
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            # Ignore subdirectory prefixes — extract flat
            basename = Path(member).name
            if not _NEEDED_CSV.match(basename):
                continue
            dest_file = dest_dir / basename
            with zf.open(member) as src, open(dest_file, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(dest_file)
            print(f"    Extracted {basename} ({dest_file.stat().st_size/1e6:.1f} MB)")
    return extracted


# ── Restructure wrapper ───────────────────────────────────────────────────────

def run_restructure(
    site_id: str,
    csv_paths: list[Path],
    outdir: Path,
    doi: str,
    comment: str,
) -> Path:
    """Call restructure() directly (no subprocess), returning the output .nc path."""
    # Determine INTERIM tag from any of the CSV filenames
    interim = any("INTERIM" in p.stem.upper() for p in csv_paths)
    nc_path = outdir / f"ICOSETC_{site_id}{'_INTERIM' if interim else ''}_restructured.nc"

    args = Namespace(
        site_id  = site_id,
        output   = nc_path,
        comment  = comment,
        doi      = doi,
    )
    restructure(csv_paths, nc_path, args)
    return nc_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download ICOS ETC L2 ARCHIVE zips from a collection DOI and "
            "restructure the CSV data into CF-1.12 NetCDF4."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "doi",
        help="DOI of the ICOS collection (e.g. 10.18160/R3G6-Z8ZH)",
    )
    parser.add_argument(
        "--outdir", default=".", type=Path, metavar="DIR",
        help="Directory for downloaded zips, temporary CSVs, and output NC files",
    )
    parser.add_argument(
        "--station", nargs="+", default=[], metavar="ID",
        help="Process only these station IDs (e.g. SE-Svb DE-Hai); default: all",
    )
    parser.add_argument(
        "--keep-zip", action="store_true",
        help="Keep downloaded zip archives after extraction",
    )
    parser.add_argument(
        "--keep-csv", action="store_true",
        help="Keep extracted CSV files after restructuring",
    )
    parser.add_argument(
        "--comment", default="",
        help="Free-text comment added as a global attribute to every output file",
    )
    args = parser.parse_args()

    outdir: Path = args.outdir.resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    doi = args.doi.strip().removeprefix("https://doi.org/").removeprefix("http://doi.org/")
    station_filter = {s.upper() for s in args.station}

    # ── Step 1: resolve DOI → collection → archive list ──────────────────────
    print(f"Resolving DOI {doi} …")
    try:
        collection_url = resolve_collection_url(doi)
    except Exception as exc:
        sys.exit(f"ERROR: could not resolve DOI: {exc}")
    print(f"Collection: {collection_url}")

    archives = get_archive_members(collection_url)
    print(f"Found {len(archives)} ARCHIVE file(s) in collection")

    if station_filter:
        archives = [
            a for a in archives
            if (m := _STATION_RE.match(a["name"])) and m.group(1).upper() in station_filter
        ]
        print(f"Filtered to {len(archives)} station(s): {', '.join(station_filter)}")

    if not archives:
        sys.exit("ERROR: no ARCHIVE files matched — nothing to do.")

    # ── Step 2: process each station ─────────────────────────────────────────
    ok: list[str] = []
    failed: list[tuple[str, str]] = []

    for arch in archives:
        m = _STATION_RE.match(arch["name"])
        if not m:
            print(f"WARNING: cannot parse station from {arch['name']!r}, skipping")
            continue
        site_id  = m.group(1)
        zip_path = outdir / arch["name"]

        print(f"\n{'─'*60}")
        print(f"Station {site_id}")

        # ── Download ─────────────────────────────────────────────────────────
        download_ok = False
        for attempt in (1, 2):
            try:
                download_zip(arch["hash_id"], zip_path, label=site_id)
                download_ok = True
                break
            except Exception as exc:
                if attempt == 1:
                    print(f"  WARNING: download failed ({exc}); retrying in 30 s …")
                    time.sleep(30)
                else:
                    print(f"  ERROR: download failed twice: {exc}")
                    failed.append((site_id, f"download: {exc}"))
                    zip_path.unlink(missing_ok=True)
        if not download_ok:
            continue

        # ── Extract ──────────────────────────────────────────────────────────
        try:
            csv_paths = extract_needed_csvs(zip_path, outdir)
        except Exception as exc:
            print(f"  ERROR: extraction failed: {exc}")
            failed.append((site_id, f"extract: {exc}"))
            if not args.keep_zip:
                zip_path.unlink(missing_ok=True)
            continue

        if not csv_paths:
            print(f"  WARNING: no matching CSVs found in {arch['name']}")
            failed.append((site_id, "no matching CSVs"))
            if not args.keep_zip:
                zip_path.unlink(missing_ok=True)
            continue

        # ── Restructure ───────────────────────────────────────────────────────
        try:
            nc_path = run_restructure(site_id, csv_paths, outdir, doi, args.comment)
            print(f"  Output: {nc_path}")
            ok.append(site_id)
        except Exception as exc:
            print(f"  ERROR: restructure failed: {exc}")
            failed.append((site_id, f"restructure: {exc}"))

        # ── Cleanup ───────────────────────────────────────────────────────────
        if not args.keep_csv:
            for p in csv_paths:
                p.unlink(missing_ok=True)
        if not args.keep_zip:
            zip_path.unlink(missing_ok=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"Done.  {len(ok)} succeeded, {len(failed)} failed.")
    if ok:
        print(f"  OK:     {', '.join(ok)}")
    if failed:
        print("  Failed:")
        for site, reason in failed:
            print(f"    {site}: {reason}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
