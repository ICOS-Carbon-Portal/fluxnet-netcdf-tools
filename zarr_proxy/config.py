"""
Configuration for the zarr data passport proxy.
All sensitive values should be set via environment variables in production.
"""
import os

# ── Zarr store ────────────────────────────────────────────────────────────────
ZARR_STORE_PATH     = os.getenv("ZARR_STORE_PATH", "icos-fluxnet.zarr")

# ── Session tracking ──────────────────────────────────────────────────────────
SESSION_TIMEOUT_SEC = int(os.getenv("SESSION_TIMEOUT_SEC", "300"))

# ── EPIC Handle ───────────────────────────────────────────────────────────────
HANDLE_PREFIX       = os.getenv("HANDLE_PREFIX",   "11676")
HANDLE_ENDPOINT     = os.getenv("HANDLE_ENDPOINT", "https://epic5.storage.surfsara.nl/api/handles")
HANDLE_TOKEN        = os.getenv("HANDLE_TOKEN",    "")

# ── ICOS Carbon Portal ────────────────────────────────────────────────────────
CP_META_UPLOAD      = os.getenv("CP_META_UPLOAD",  "https://meta.icos-cp.eu/upload")
CP_AUTH_URL         = os.getenv("CP_AUTH_URL",     "https://cpauth.icos-cp.eu/password/login")
CP_USERNAME         = os.getenv("CP_USERNAME",     "")
CP_PASSWORD         = os.getenv("CP_PASSWORD",     "")
CP_SUBMITTER_ID     = os.getenv("CP_SUBMITTER_ID", "")
CP_OBJ_SPEC_URL     = os.getenv("CP_OBJ_SPEC_URL", "")   # new DataPassport type URL — TBD

# ── Matomo ────────────────────────────────────────────────────────────────────
MATOMO_URL          = os.getenv("MATOMO_URL",      "")
MATOMO_SITE_ID      = os.getenv("MATOMO_SITE_ID",  "")
MATOMO_TOKEN        = os.getenv("MATOMO_TOKEN",    "")

# ── Passport storage ──────────────────────────────────────────────────────────
PASSPORT_DIR        = os.getenv("PASSPORT_DIR",    "passports")
