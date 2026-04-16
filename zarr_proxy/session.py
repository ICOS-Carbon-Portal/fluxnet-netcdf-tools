"""
Session tracker: groups zarr chunk requests by (IP, time-window) into logical
access sessions, then triggers passport minting when a session goes idle.
"""
import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from . import config


@dataclass
class ChunkRecord:
    key: str          # e.g. "SE-Svb/NEE/0.0.0"
    size: int         # bytes served
    sha256: str       # hex digest of chunk bytes


@dataclass
class Session:
    ip: str
    store: str                   # store name, e.g. "icos-fluxnet.zarr"
    started_at:  float = field(default_factory=time.time)
    last_seen:   float = field(default_factory=time.time)
    groups:      set[str]        = field(default_factory=set)
    arrays:      set[str]        = field(default_factory=set)
    chunks:      list[ChunkRecord] = field(default_factory=list)
    bytes_total: int = 0
    passport_pid: str = ""        # filled after minting; enables GET /session/passport
    queries: list[dict] = field(default_factory=list)  # xarray selection steps from client

    def touch(self) -> None:
        self.last_seen = time.time()

    def record_chunk(self, key: str, data: bytes) -> None:
        group, *rest = key.split("/")
        # group = top-level station (e.g. "SE-Svb") or "SE-Svb/fluxnet_dd"
        parts = key.split("/")
        if len(parts) >= 2:
            # array name is the second-to-last component before chunk indices
            # chunk indices look like digits separated by dots: "0.0.0"
            # Walk back to find the first non-chunk component
            arr_idx = len(parts) - 1
            while arr_idx > 0 and _is_chunk_key(parts[arr_idx]):
                arr_idx -= 1
            array_name = parts[arr_idx]
            grp_path   = "/".join(parts[:arr_idx])
            self.groups.add(grp_path)
            self.arrays.add(array_name)

        sha = hashlib.sha256(data).hexdigest()
        self.chunks.append(ChunkRecord(key=key, size=len(data), sha256=sha))
        self.bytes_total += len(data)
        self.touch()

    @property
    def ip_anonymised(self) -> str:
        """Return /24 for IPv4, /48 for IPv6."""
        if ":" in self.ip:
            parts = self.ip.split(":")
            return ":".join(parts[:3]) + "::/48"
        parts = self.ip.split(".")
        return ".".join(parts[:3]) + ".0/24"


def _is_chunk_key(s: str) -> bool:
    """True if s looks like a zarr chunk index: all digits and dots."""
    return bool(s) and all(c.isdigit() or c == "." for c in s)


# ── Session registry ──────────────────────────────────────────────────────────

# (ip, store) → Session
_sessions: dict[tuple[str, str], Session] = {}
_on_close_callbacks: list[Callable[[Session], Awaitable[None]]] = []


def register_on_close(cb: Callable[[Session], Awaitable[None]]) -> None:
    _on_close_callbacks.append(cb)


def get_or_create(ip: str, store: str) -> Session:
    key = (ip, store)
    s = _sessions.get(key)
    if s is None:
        s = Session(ip=ip, store=store)
        _sessions[key] = s
    return s


def record(ip: str, store: str, key: str, data: bytes) -> None:
    get_or_create(ip, store).record_chunk(key, data)


def pop(ip: str, store: str) -> Session | None:
    """Remove and return the active session for *(ip, store)*, or None."""
    return _sessions.pop((ip, store), None)


async def _reaper_loop() -> None:
    """Background task: close idle sessions and fire callbacks."""
    while True:
        await asyncio.sleep(10)
        now = time.time()
        expired = [
            ip for ip, s in list(_sessions.items())
            if now - s.last_seen > config.SESSION_TIMEOUT_SEC
        ]
        for ip in expired:
            session = _sessions.pop(ip, None)
            if session and session.chunks:
                for cb in _on_close_callbacks:
                    try:
                        await cb(session)
                    except Exception as exc:
                        print(f"[session] on_close callback failed: {exc}")


_reaper_task: asyncio.Task | None = None


def start_reaper() -> None:
    global _reaper_task
    _reaper_task = asyncio.create_task(_reaper_loop())
