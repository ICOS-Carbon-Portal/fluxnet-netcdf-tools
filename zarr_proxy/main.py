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


# ── Passport pipeline (called when a session closes) ─────────────────────────

async def _on_session_close(s: session.Session) -> None:
    log.info(
        "[session] closed  ip=%-16s  groups=%d  arrays=%d  chunks=%d  bytes=%d",
        s.ip, len(s.groups), len(s.arrays), len(s.chunks), s.bytes_total,
    )

    # 1. Build passport (without Handle PID / CP URL yet)
    p, sha256 = passport.build(s)

    # 2. Mint Handle PID (target = CP landing page; use placeholder until CP URL is known)
    pid = handle_client.mint(target_url=f"https://data.icos-cp.eu/passport/{sha256[:16]}")

    # 3. Upload to ICOS CP (updates target URL in passport first)
    from . import cp_client
    if pid:
        p["@graph"][1]["@id"]  = pid
        p["@graph"][1]["url"]  = ""     # will be filled by CP response
    cp_url = cp_client.upload(p, sha256)
    if cp_url:
        p["@graph"][1]["url"] = cp_url
        # Update Handle to point to the real landing page
        if pid:
            handle_client.mint(target_url=cp_url)   # mints a second handle; TODO: update instead

    # 4. Recompute passportSha256 now that pid + cp_url are final
    import hashlib
    p["@graph"][1]["passportSha256"] = None
    final_bytes = json.dumps(p, sort_keys=True, separators=(",", ":")).encode()
    final_sha   = hashlib.sha256(final_bytes).hexdigest()
    p["@graph"][1]["passportSha256"] = final_sha

    # 5. Save to disk
    path = passport.save(p, final_sha)
    log.info("[passport] saved  %s  pid=%s", path, pid or "(none)")

    # 6. Matomo
    matomo_client.track(s, passport_pid=pid)


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
