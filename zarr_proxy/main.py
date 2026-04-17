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
"""
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


# ── Store resolution ──────────────────────────────────────────────────────────

def _resolve_store(store_name: str) -> pathlib.Path | None:
    """
    Return the resolved store path if *store_name* is a valid zarr store
    inside STORE_DIR, or None if it doesn't exist / is a path traversal attempt.
    Requires a .zgroup or .zmetadata marker to reject non-zarr directories.
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

async def _mint_passport(s: session.Session) -> str:
    """
    Run the full passport pipeline for *s* and return the Handle PID (or "").
    Stores the PID on s.passport_pid so GET /{store}/session/passport can retrieve it.
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
    return pid


async def _on_session_close(s: session.Session) -> None:
    await _mint_passport(s)


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    session.register_on_close(_on_session_close)
    session.start_reaper()
    stores = [p.name for p in STORE_DIR.iterdir()
              if p.is_dir() and ((p / ".zgroup").exists() or (p / ".zmetadata").exists())]
    log.info("zarr_proxy serving %d store(s) from %s: %s", len(stores), STORE_DIR, stores)
    yield


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
    """List available zarr stores."""
    stores = [p.name for p in STORE_DIR.iterdir()
              if p.is_dir() and ((p / ".zgroup").exists() or (p / ".zmetadata").exists())]
    return JSONResponse({"stores": sorted(stores)})


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

    pid = await _mint_passport(s)
    return JSONResponse({
        "passport_pid":  pid,
        "passport_url":  s.passport_pid and f"https://doi.org/{pid}" or "",
        "chunks":        len(s.chunks),
        "bytes_served":  s.bytes_total,
        "arrays":        sorted(s.arrays),
        "queries":       s.queries,
    })


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
