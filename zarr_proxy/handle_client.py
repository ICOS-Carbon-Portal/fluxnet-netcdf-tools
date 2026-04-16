"""
Mint a Handle PID via the EPIC REST API.

Endpoint: PUT {HANDLE_ENDPOINT}/{PREFIX}/{suffix}
Auth:     Bearer token
Returns:  full handle string "PREFIX/suffix" or empty string on failure.
"""
import json
import urllib.error
import urllib.request
import uuid

from . import config


def mint(target_url: str) -> str:
    """
    Create a new Handle pointing to *target_url*.
    Returns the handle string (e.g. "11676/abc123") or "" on failure.
    """
    if not config.HANDLE_TOKEN or not config.HANDLE_ENDPOINT:
        print("[handle] HANDLE_TOKEN or HANDLE_ENDPOINT not configured — skipping PID minting")
        return ""

    suffix   = str(uuid.uuid4())
    handle   = f"{config.HANDLE_PREFIX}/{suffix}"
    endpoint = f"{config.HANDLE_ENDPOINT}/{config.HANDLE_PREFIX}/{suffix}"

    payload = json.dumps([
        {
            "type":  "URL",
            "parsed_data": target_url,
        }
    ]).encode()

    req = urllib.request.Request(
        endpoint,
        data=payload,
        method="PUT",
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {config.HANDLE_TOKEN}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 201):
                return f"hdl:{handle}"
    except urllib.error.HTTPError as exc:
        print(f"[handle] HTTP {exc.code} minting handle: {exc.read().decode()[:200]}")
    except Exception as exc:
        print(f"[handle] Error minting handle: {exc}")

    return ""
