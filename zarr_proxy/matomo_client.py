"""
Fire a server-side Matomo tracking event for a completed session.
Uses the Matomo Tracking HTTP API with token_auth to override client IP.
"""
import json
import urllib.error
import urllib.parse
import urllib.request

from . import config
from .session import Session


def track(session: Session, passport_pid: str = "") -> None:
    """Send one Matomo event per completed session. Silently skips if not configured."""
    if not all([config.MATOMO_URL, config.MATOMO_SITE_ID, config.MATOMO_TOKEN]):
        return

    stations  = sorted({g.split("/")[0] for g in session.groups if g})
    variables = sorted(session.arrays)

    params = {
        "idsite":       config.MATOMO_SITE_ID,
        "rec":          "1",
        "send_image":   "0",
        "action_name":  f"zarr-access/{','.join(stations)}/{','.join(variables[:10])}",
        "url":          f"https://data.icos-cp.eu/zarr/{'/'.join(stations)}",
        "cip":          session.ip,         # Matomo anonymises per its own settings
        "token_auth":   config.MATOMO_TOKEN,
        "_cvar": json.dumps({
            "1": ["stations",     ",".join(stations)],
            "2": ["variables",    ",".join(variables[:20])],
            "3": ["n_chunks",     str(len(session.chunks))],
            "4": ["bytes_served", str(session.bytes_total)],
            "5": ["passport_pid", passport_pid],
        }),
    }

    url = f"{config.MATOMO_URL.rstrip('/')}/matomo.php?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=10):
            pass
    except Exception as exc:
        print(f"[matomo] Tracking call failed: {exc}")
