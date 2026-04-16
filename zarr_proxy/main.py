"""
zarr_proxy — FastAPI zarr v2 HTTP store with data passport generation.

Exposes icos-fluxnet.zarr as a standard zarr v2 HTTP store:
  GET /{key}   → serve .zgroup / .zattrs / .zarray / chunk files

Clients connect with:
  xr.open_zarr("http://localhost:8000/")

Every chunk response is recorded into a session keyed by client IP.
When a session goes idle (SESSION_TIMEOUT_SEC), a passport is minted,
a Handle PID is created, the passport is uploaded to ICOS CP, and a
Matomo tracking event is fired.
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

STORE = pathlib.Path(config.ZARR_STORE_PATH).resolve()


# ── Passport pipeline ────────────────────────────────────────────────────────

async def _mint_passport(s: session.Session) -> str:
    """
    Run the full passport pipeline for *s* and return the Handle PID (or "").
    Stores the PID on s.passport_pid so GET /session/passport can retrieve it.
    """
    import hashlib
    from . import cp_client

    log.info(
        "[session] closing  ip=%-16s  groups=%d  arrays=%d  chunks=%d  bytes=%d",
        s.ip, len(s.groups), len(s.arrays), len(s.chunks), s.bytes_total,
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
        # Update Handle to resolve to the real CP landing page
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
    log.info("zarr_proxy serving %s", STORE)
    yield


app = FastAPI(title="zarr-passport-proxy", lifespan=lifespan)


def _client_ip(request: Request) -> str:
    """Return the real client IP, honouring X-Forwarded-For if behind a proxy."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@app.post("/session/close")
async def close_session(request: Request) -> JSONResponse:
    """
    Explicitly close the caller's session and synchronously mint a passport.
    Returns {"passport_pid": "hdl:11676/...", "passport_url": "https://..."}.
    If the session has no chunks (nothing was read), returns an empty result.
    """
    ip = _client_ip(request)
    s  = session.pop(ip)
    if s is None or not s.chunks:
        return JSONResponse({"passport_pid": "", "passport_url": "", "chunks": 0})
    pid = await _mint_passport(s)
    return JSONResponse({
        "passport_pid":  pid,
        "passport_url":  s.passport_pid and f"https://doi.org/{pid}" or "",
        "chunks":        len(s.chunks),
        "bytes_served":  s.bytes_total,
        "arrays":        sorted(s.arrays),
    })


@app.get("/session/passport")
async def get_passport(request: Request) -> JSONResponse:
    """
    Return the passport PID for the caller's current or most-recently-closed
    session.  Useful if the client forgot to call /session/close, or wants
    to poll after the idle timeout fires.
    """
    ip = _client_ip(request)
    s  = session.get_or_create(ip)
    return JSONResponse({
        "passport_pid": s.passport_pid,
        "session_open": bool(s.chunks),
        "chunks":       len(s.chunks),
    })


@app.get("/")
async def root(request: Request) -> Response:
    """
    Serve root .zgroup, or handle fsspec directory listing request.
    fsspec HTTPFileSystem sends GET /?list= to enumerate contents.
    """
    if "list" in request.query_params:
        entries = [p.name for p in STORE.iterdir()]
        return JSONResponse(content=entries)
    zgroup = STORE / ".zgroup"
    if zgroup.exists():
        return JSONResponse(content=json.loads(zgroup.read_bytes()))
    return JSONResponse(content={"zarr_format": 2})


@app.get("/{key:path}")
async def serve_zarr_key(key: str, request: Request) -> Response:
    """
    Serve any zarr store key: metadata files or chunk arrays.
    Chunk data is recorded in the session; metadata is served transparently.
    """
    # Sanitise key to prevent path traversal
    target = (STORE / key).resolve()
    if not str(target).startswith(str(STORE)):
        return PlainTextResponse("Forbidden", status_code=403)

    if not target.exists():
        return PlainTextResponse("Not found", status_code=404)

    # fsspec directory listing: GET /some/path?list=
    if "list" in request.query_params:
        if target.is_dir():
            entries = [p.name for p in target.iterdir()]
            return JSONResponse(content=entries)
        return PlainTextResponse("Not found", status_code=404)

    # Never try to read a directory as a file
    if target.is_dir():
        return PlainTextResponse("Not found", status_code=404)

    data = target.read_bytes()
    ip   = _client_ip(request)

    name = target.name
    if name in (".zmetadata", ".zgroup", ".zattrs", ".zarray"):
        return Response(content=data, media_type="application/json")

    # Everything else is a chunk — track it
    session.record(ip, key, data)
    return Response(content=data, media_type="application/octet-stream")
