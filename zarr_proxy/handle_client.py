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


def _handle_request(method: str, url: str, payload: bytes) -> bool:
    req = urllib.request.Request(
        url, data=payload, method=method,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {config.HANDLE_TOKEN}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status in (200, 201, 204)
    except urllib.error.HTTPError as exc:
        print(f"[handle] HTTP {exc.code} {method} {url}: {exc.read().decode()[:200]}")
    except Exception as exc:
        print(f"[handle] Error {method} {url}: {exc}")
    return False


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

    ok = _handle_request("PUT", endpoint, payload)
    return f"hdl:{handle}" if ok else ""


def update(pid: str, target_url: str) -> bool:
    """
    Update an existing Handle to point to a new URL (PATCH semantics via PUT).
    *pid* may include the 'hdl:' prefix or not.
    """
    if not config.HANDLE_TOKEN or not config.HANDLE_ENDPOINT:
        return False

    handle   = pid.removeprefix("hdl:")
    endpoint = f"{config.HANDLE_ENDPOINT}/{handle}"
    payload  = json.dumps([{"type": "URL", "parsed_data": target_url}]).encode()
    return _handle_request("PUT", endpoint, payload)
