"""
zarr_proxy — FastAPI multi-store zarr v2 HTTP proxy with data passport generation.

Each zarr store in ZARR_STORE_DIR is served under its directory name:
  GET /{store-name}/{key}   → .zgroup / .zattrs / .zarray / chunk files

Clients connect with:
  xr.open_zarr("http://localhost:8000/icos-fluxnet.zarr/")

Every chunk response is recorded into a session keyed by (client IP, store).
When a session goes idle (SESSION_TIMEOUT_SEC), a passport is minted,
a Handle PID is created, the passport is uploaded to ICOS CP, and a
Matomo tracking event is fired.

A background task periodically re-scans ZARR_STORE_DIR (interval set by
STORE_RESCAN_SEC, default 60 s) and logs any store added or removed.
The store list is otherwise read live from disk on every request, so
new stores are reachable as soon as they're created — the rescan is
just a periodic ground-truth refresh + visible log line for operators.
"""
import asyncio
import json
import logging
import pathlib
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from . import config, handle_client, matomo_client, passport, session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("zarr_proxy")

STORE_DIR = pathlib.Path(config.ZARR_STORE_DIR).resolve()


# ── Live store inventory (refreshed by the rescan task) ───────────────────────

def _scan_stores() -> set[str]:
    """Walk STORE_DIR and return every directory that looks like a zarr store."""
    if not STORE_DIR.is_dir():
        return set()
    return {
        p.name for p in STORE_DIR.iterdir()
        if p.is_dir() and ((p / ".zgroup").exists() or (p / ".zmetadata").exists())
    }


# Holds the most recent inventory seen by the rescan task. Updated atomically
# by replacing the set; readers always get a coherent snapshot. Initially
# empty — populated by the lifespan startup before traffic begins.
_known_stores: set[str] = set()


async def _rescan_stores_loop(interval: int) -> None:
    """Background task: refresh _known_stores and log any deltas."""
    global _known_stores
    while True:
        try:
            await asyncio.sleep(interval)
            current = _scan_stores()
            added   = sorted(current - _known_stores)
            removed = sorted(_known_stores - current)
            if added:
                log.info("[rescan] added: %s", added)
            if removed:
                log.info("[rescan] removed: %s", removed)
            _known_stores = current
        except asyncio.CancelledError:
            return
        except Exception as exc:                          # never let the task die
            log.warning("[rescan] error: %s", exc)


# ── Store resolution ──────────────────────────────────────────────────────────

def _resolve_store(store_name: str) -> pathlib.Path | None:
    """
    Return the resolved store path if *store_name* is a valid zarr store
    inside STORE_DIR, or None if it doesn't exist / is a path traversal attempt.
    Requires a .zgroup or .zmetadata marker to reject non-zarr directories.

    Reads the filesystem live every time so a newly added store is usable
    immediately (without waiting for the next periodic rescan).
    """
    store = (STORE_DIR / store_name).resolve()
    if not str(store).startswith(str(STORE_DIR)):
        return None
    if not store.is_dir():
        return None
    if not (store / ".zgroup").exists() and not (store / ".zmetadata").exists():
        return None
    return store


# ── Passport pipeline ────────────────────────────────────────────────────────

async def _mint_passport(s: session.Session) -> tuple[str, dict, str]:
    """
    Run the full passport pipeline for *s*.

    Returns (pid, passport_jsonld, saved_path):
      pid              — minted Handle PID, or "" if not configured
      passport_jsonld  — the full ROCrate JSON-LD passport dict (saved to disk)
      saved_path       — path of the file written under PASSPORT_DIR

    Stores the PID on s.passport_pid so GET /{store}/session/passport can
    retrieve it.
    """
    import hashlib
    from . import cp_client

    log.info(
        "[session] closing  ip=%-16s  store=%-24s  groups=%d  arrays=%d  chunks=%d  bytes=%d",
        s.ip, s.store, len(s.groups), len(s.arrays), len(s.chunks), s.bytes_total,
    )

    # 1. Build passport
    p, sha256 = passport.build(s)

    # 2. Mint Handle PID pointing to a placeholder until CP URL is known
    pid = handle_client.mint(target_url=f"https://data.icos-cp.eu/passport/{sha256[:16]}")
    if pid:
        p["@graph"][1]["@id"] = pid
        p["@graph"][1]["url"] = ""

    # 3. Upload to ICOS CP
    cp_url = cp_client.upload(p, sha256)
    if cp_url:
        p["@graph"][1]["url"] = cp_url
        if pid:
            handle_client.update(pid, target_url=cp_url)

    # 4. Recompute passportSha256 with final pid + cp_url
    p["@graph"][1]["passportSha256"] = None
    final_bytes = json.dumps(p, sort_keys=True, separators=(",", ":")).encode()
    final_sha   = hashlib.sha256(final_bytes).hexdigest()
    p["@graph"][1]["passportSha256"] = final_sha

    # 5. Save to disk
    path = passport.save(p, final_sha)
    log.info("[passport] saved  %s  pid=%s", path, pid or "(none)")

    # 6. Matomo
    matomo_client.track(s, passport_pid=pid)

    s.passport_pid = pid
    return pid, p, str(path)


async def _on_session_close(s: session.Session) -> None:
    await _mint_passport(s)


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _known_stores
    session.register_on_close(_on_session_close)
    session.start_reaper()

    _known_stores = _scan_stores()
    log.info(
        "zarr_proxy serving %d store(s) from %s: %s",
        len(_known_stores), STORE_DIR, sorted(_known_stores),
    )

    rescan_task: asyncio.Task | None = None
    if config.STORE_RESCAN_SEC > 0:
        rescan_task = asyncio.create_task(
            _rescan_stores_loop(config.STORE_RESCAN_SEC)
        )
        log.info("[rescan] periodic store rescan enabled (every %ds)",
                 config.STORE_RESCAN_SEC)
    try:
        yield
    finally:
        if rescan_task is not None:
            rescan_task.cancel()
            try:
                await rescan_task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="zarr-passport-proxy", lifespan=lifespan)


def _client_ip(request: Request) -> str:
    """Return the real client IP, honouring X-Forwarded-For if behind a proxy."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ── Root: list available stores ───────────────────────────────────────────────

@app.get("/")
async def root() -> JSONResponse:
    """List available zarr stores (always reflects current disk state)."""
    return JSONResponse({"stores": sorted(_scan_stores())})


@app.post("/admin/rescan")
async def admin_rescan() -> JSONResponse:
    """
    Force an immediate rescan of ZARR_STORE_DIR and return the deltas
    against the previously-known set.  Useful after dropping a new
    store onto a long-running proxy without waiting for the periodic
    refresh.
    """
    global _known_stores
    current = _scan_stores()
    added   = sorted(current - _known_stores)
    removed = sorted(_known_stores - current)
    if added:
        log.info("[rescan] (manual) added: %s", added)
    if removed:
        log.info("[rescan] (manual) removed: %s", removed)
    _known_stores = current
    return JSONResponse({
        "stores":  sorted(current),
        "added":   added,
        "removed": removed,
    })


# ── Per-store session endpoints ───────────────────────────────────────────────

@app.post("/{store_name}/session/close")
async def close_session(store_name: str, request: Request) -> JSONResponse:
    """
    Explicitly close the caller's session for *store_name* and mint a passport.
    Accepts an optional JSON body {"queries": [...]} with xarray selection
    steps recorded client-side by datapassport_zarr.open_zarr().
    Returns {"passport_pid": "hdl:11676/...", "passport_url": "https://..."}.
    If the session has no chunks (nothing was read), returns an empty result.
    """
    if _resolve_store(store_name) is None:
        return JSONResponse({"error": "store not found"}, status_code=404)

    ip = _client_ip(request)
    s  = session.pop(ip, store_name)
    if s is None or not s.chunks:
        return JSONResponse({"passport_pid": "", "passport_url": "", "chunks": 0})

    try:
        body = await request.json()
        s.queries = body.get("queries", [])
    except Exception:
        pass

    pid, passport_jsonld, saved_path = await _mint_passport(s)

    # Surviving station list comes from the client's last query entry, written
    # by datapassport_zarr after the where()/sel() chain. Falls back to [].
    stations = []
    for entry in reversed(s.queries or []):
        if "surviving_stations" in entry:
            stations = entry["surviving_stations"]
            break

    # Best-effort: per-station source_doi/citation from the on-disk store's
    # combined-view coords (if present).
    station_sources = _resolve_station_sources(s.store, stations) if stations else []

    return JSONResponse({
        "passport_pid":    pid,
        "passport_url":    s.passport_pid and f"https://doi.org/{pid}" or "",
        "chunks":          len(s.chunks),
        "bytes_served":    s.bytes_total,
        "arrays":          sorted(s.arrays),
        "queries":         s.queries,
        "ip_anonymised":   s.ip_anonymised,
        "stations":        stations,
        "station_sources": station_sources,
        # The full ROCrate JSON-LD passport (same as the .jsonld file on disk)
        "passport":        passport_jsonld,
        "passport_path":   saved_path,
    })


def _resolve_station_sources(store_name: str, stations: list[str]) -> list[dict]:
    """For each station id, look up source_doi + citation in the on-disk
    combined-view coords; return [{station, source_doi, citation}, ...]."""
    if not stations:
        return []
    store = _resolve_store(store_name)
    if store is None:
        return []

    # Known combined-view group locations (fast — no rglob walk).
    candidate_groups = ["co2", "ch4", "n2o", "co",
                        "_combined/fluxnet_dd", "_combined/fluxnet_mm",
                        "_combined/fluxnet_ww", "_combined/fluxnet_yy"]

    try:
        import zarr
    except ImportError:
        return []

    out: list[dict] = []
    wanted = set(stations)
    seen_stations: set = set()

    for grp_path in candidate_groups:
        grp_dir = store / grp_path.replace("/", "/")
        if not (grp_dir / ".zgroup").exists():
            continue
        try:
            g = zarr.open_group(str(store), mode="r", path=grp_path)
        except Exception:
            continue
        if "station" not in g or "source_doi" not in g:
            continue

        sids = list(g["station"][:])
        sdoi = list(g["source_doi"][:])
        cite = list(g["citation"][:]) if "citation" in g else [""] * len(sids)
        for sid, doi, cit in zip(sids, sdoi, cite):
            sid_str = sid.decode() if isinstance(sid, (bytes, bytearray)) else str(sid)
            if sid_str in wanted and sid_str not in seen_stations:
                out.append({
                    "station":    sid_str,
                    "source_doi": doi.decode() if isinstance(doi, (bytes, bytearray)) else str(doi),
                    "citation":   cit.decode() if isinstance(cit, (bytes, bytearray)) else str(cit),
                })
                seen_stations.add(sid_str)
    return out


@app.get("/{store_name}/session/passport")
async def get_passport(store_name: str, request: Request) -> JSONResponse:
    """
    Return the passport PID for the caller's current or most-recently-closed
    session for *store_name*.
    """
    if _resolve_store(store_name) is None:
        return JSONResponse({"error": "store not found"}, status_code=404)

    ip = _client_ip(request)
    s  = session.get_or_create(ip, store_name)
    return JSONResponse({
        "passport_pid": s.passport_pid,
        "session_open": bool(s.chunks),
        "chunks":       len(s.chunks),
    })


# ── zarr store key serving ────────────────────────────────────────────────────

@app.get("/{store_name}/")
async def store_root(store_name: str, request: Request) -> Response:
    """Serve root .zgroup for a store, or fsspec directory listing."""
    store = _resolve_store(store_name)
    if store is None:
        return JSONResponse({"error": "store not found"}, status_code=404)

    if "list" in request.query_params:
        entries = [p.name for p in store.iterdir()]
        return JSONResponse(content=entries)

    zgroup = store / ".zgroup"
    if zgroup.exists():
        return JSONResponse(content=json.loads(zgroup.read_bytes()))
    return JSONResponse(content={"zarr_format": 2})


@app.get("/{store_name}/{key:path}")
async def serve_zarr_key(store_name: str, key: str, request: Request) -> Response:
    """
    Serve any zarr store key: metadata files or chunk arrays.
    Chunk data is recorded in the session; metadata is served transparently.
    """
    store = _resolve_store(store_name)
    if store is None:
        return PlainTextResponse("Not found", status_code=404)

    target = (store / key).resolve()
    if not str(target).startswith(str(store)):
        return PlainTextResponse("Forbidden", status_code=403)

    if not target.exists():
        return PlainTextResponse("Not found", status_code=404)

    if "list" in request.query_params:
        if target.is_dir():
            entries = [p.name for p in target.iterdir()]
            return JSONResponse(content=entries)
        return PlainTextResponse("Not found", status_code=404)

    if target.is_dir():
        return PlainTextResponse("Not found", status_code=404)

    data = target.read_bytes()
    ip   = _client_ip(request)

    if target.name in (".zmetadata", ".zgroup", ".zattrs", ".zarray"):
        return Response(content=data, media_type="application/json")

    # Everything else is a chunk — track it
    session.record(ip, store_name, key, data)
    headers = {}
    if not request.headers.get("X-DataPassport-Client"):
        headers["X-DataPassport-Warning"] = (
            "Install datapassport_zarr and use datapassport_zarr.open_zarr() "
            "to receive your data passport PID automatically."
        )
    return Response(content=data, media_type="application/octet-stream", headers=headers)
