"""
Upload a data passport to the ICOS Carbon Portal metadata service.

Flow:
  1. POST credentials to CP auth endpoint → receive cookie
  2. POST JSON-LD metadata package to /upload → receive landing page URL
"""
import json
import urllib.error
import urllib.parse
import urllib.request

from . import config


def _get_cookie() -> str:
    """Authenticate and return the CPauth cookie string."""
    data = urllib.parse.urlencode({
        "mail":     config.CP_USERNAME,
        "password": config.CP_PASSWORD,
    }).encode()
    req = urllib.request.Request(
        config.CP_AUTH_URL,
        data=data,
        method="POST",
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            # Cookie is in Set-Cookie header
            cookie = resp.headers.get("Set-Cookie", "")
            # Extract just the name=value part (before first semicolon)
            return cookie.split(";")[0].strip()
    except Exception as exc:
        print(f"[cp] Auth failed: {exc}")
        return ""


def upload(passport: dict, passport_sha256: str) -> str:
    """
    Upload the passport as a new CP metadata object.
    Returns the landing page URL on success, or "" on failure.

    The CP /upload endpoint expects a metadata package JSON with:
      submitterId, hashSum, fileName, objectSpecification, specificInfo
    The actual passport JSON-LD is the data object uploaded separately;
    here we upload the passport itself as the data object with a stub
    specificInfo until the DataPassport object type is finalised with CP.
    """
    if not all([config.CP_USERNAME, config.CP_PASSWORD,
                config.CP_SUBMITTER_ID, config.CP_OBJ_SPEC_URL]):
        print("[cp] CP credentials or object spec not configured — skipping CP upload")
        return ""

    cookie = _get_cookie()
    if not cookie:
        return ""

    passport_bytes = json.dumps(passport, separators=(",", ":")).encode()

    meta_package = {
        "submitterId":       config.CP_SUBMITTER_ID,
        "hashSum":           passport_sha256,
        "fileName":          f"passport_{passport_sha256[:16]}.jsonld",
        "objectSpecification": config.CP_OBJ_SPEC_URL,
        "specificInfo":      {},   # datatype-specific fields — TBD
        "references": {
            "duplicateFilenameAllowed": True,
        },
    }

    # Step 1: register metadata
    meta_req = urllib.request.Request(
        config.CP_META_UPLOAD,
        data=json.dumps(meta_package).encode(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Cookie":        cookie,
        },
    )
    try:
        with urllib.request.urlopen(meta_req, timeout=30) as resp:
            landing_url = resp.read().decode().strip().strip('"')
            print(f"[cp] Registered passport: {landing_url}")
            return landing_url
    except urllib.error.HTTPError as exc:
        print(f"[cp] HTTP {exc.code} uploading passport: {exc.read().decode()[:200]}")
    except Exception as exc:
        print(f"[cp] Error uploading passport: {exc}")

    return ""
