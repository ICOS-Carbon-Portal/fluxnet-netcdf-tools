"""
Builds a ROCrate-extended JSON-LD data passport for a completed session,
writes it to disk, and returns the passport dict (without passportSha256
so the caller can insert the Handle PID and CP landing page URL first).
"""
import hashlib
import json
import pathlib
import uuid
from datetime import datetime, timezone

import zarr

from . import config
from .session import Session


def _ts_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _station_metadata(store_path: str, group: str) -> dict:
    """Pull a handful of attrs from the station group's .zattrs."""
    try:
        z = zarr.open_group(store_path, mode="r")
        top = group.split("/")[0]
        if top not in z:
            return {}
        attrs = dict(z[top].attrs)
        prov  = json.loads(attrs.get("_provenance", "{}"))
        return {
            "station":   top,
            "source_doi": prov.get("source_doi", ""),
            "citation":   prov.get("citation",   ""),
        }
    except Exception:
        return {}


def build(session: Session) -> dict:
    """
    Build the ROCrate JSON-LD passport dict for *session*.
    passportSha256 is computed last and embedded.
    The caller fills in @id, url, and passportSha256 after minting the Handle PID.
    """
    passport_id = f"urn:uuid:{uuid.uuid4()}"

    # Deduplicate per-variable checksum manifest
    array_manifest: dict[str, dict] = {}
    for chunk in session.chunks:
        parts = chunk.key.split("/")
        # array path = everything except trailing chunk index
        arr_idx = len(parts) - 1
        while arr_idx > 0 and all(c.isdigit() or c == "." for c in parts[arr_idx]):
            arr_idx -= 1
        arr_path = "/".join(parts[:arr_idx + 1])
        if arr_path not in array_manifest:
            array_manifest[arr_path] = {
                "@id":        arr_path,
                "name":       parts[arr_idx],
                "zarr_path":  arr_path,
                "chunks":     [],
                "sizeInBytes": 0,
            }
        array_manifest[arr_path]["chunks"].append(chunk.sha256)
        array_manifest[arr_path]["sizeInBytes"] += chunk.size

    # Per-variable aggregate checksum = SHA-256 of sorted chunk digests
    has_parts = []
    for entry in array_manifest.values():
        agg = hashlib.sha256(
            "\n".join(sorted(entry["chunks"])).encode()
        ).hexdigest()
        has_parts.append({
            "@id":           entry["@id"],
            "name":          entry["name"],
            "zarr_path":     entry["zarr_path"],
            "sha256":        agg,
            "sizeInBytes":   entry["sizeInBytes"],
            "chunkCount":    len(entry["chunks"]),
        })

    # Station-level metadata (best-effort)
    store_path = str(pathlib.Path(config.ZARR_STORE_DIR) / session.store)
    station_meta = {}
    for g in sorted(session.groups):
        m = _station_metadata(store_path, g)
        if m:
            station_meta = m
            break

    passport: dict = {
        "@context": [
            "https://w3id.org/ro/crate/1.1/context",
            {"icos": "https://meta.icos-cp.eu/ontology/cpmeta/"},
        ],
        "@graph": [
            {
                "@id":              "./",
                "@type":            "Dataset",
                "hasPart":          [{"@id": p["@id"]} for p in has_parts],
            },
            {
                "@id":              passport_id,
                "@type":            ["Dataset", "icos:DataPassport"],
                "name":             (
                    f"Data access passport — "
                    f"{', '.join(sorted(session.arrays)[:5])} "
                    f"({_ts_iso(session.started_at)[:10]})"
                ),
                "url":              cp_url,
                "dateAccessed":     _ts_iso(session.started_at),
                "sessionStart":     _ts_iso(session.started_at),
                "sessionEnd":       _ts_iso(session.last_seen),
                "agent": {
                    "@type":        "Agent",
                    "ipAnonymised": session.ip_anonymised,
                },
                "accessedGroups":   sorted(session.groups),
                "accessedArrays":   sorted(session.arrays),
                "query":            session.queries or [],
                "hasPart":          has_parts,
                "totalBytesServed": session.bytes_total,
                "totalChunks":      len(session.chunks),
                "isPartOf":         {"@id": station_meta.get("source_doi", "")},
                "citation":         station_meta.get("citation", ""),
                "passportSha256":   None,   # placeholder — filled below
            },
        ],
    }

    # Compute passportSha256 over the passport with the field set to null
    passport_bytes = json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    passport_sha256 = hashlib.sha256(passport_bytes).hexdigest()
    passport["@graph"][1]["passportSha256"] = passport_sha256

    return passport, passport_sha256


def save(passport: dict, passport_sha256: str) -> pathlib.Path:
    """Write the passport to PASSPORT_DIR/{sha256[:16]}.jsonld and return the path."""
    outdir = pathlib.Path(config.PASSPORT_DIR)
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"{passport_sha256[:16]}.jsonld"
    path.write_text(json.dumps(passport, indent=2), encoding="utf-8")
    return path
