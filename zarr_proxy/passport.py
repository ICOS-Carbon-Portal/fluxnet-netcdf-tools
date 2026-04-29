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


def _station_entities(store_path: str, group_path: str, surviving_stations: list[str]) -> tuple[list[dict], list[dict]]:
    """For a combined-view group, build per-station and per-DataObject ROCrate
    entities for each station in *surviving_stations*. Reads 1-D coords:
        station, lat, lon, country, site_name, source_doi, citation,
        station_url (optional), intake_height (optional), dataset_name (optional).

    Returns (station_entities, dataobject_entities) — each a list of dicts
    suitable to insert into the passport @graph.
    """
    if not surviving_stations:
        return [], []
    try:
        g = zarr.open_group(store_path, mode="r", path=group_path)
    except Exception:
        return [], []
    if "station" not in g:
        return [], []

    def _arr(name):
        if name not in g:
            return None
        try:
            return list(g[name][:])
        except Exception:
            return None

    sids = _arr("station") or []
    sids = [s.decode() if isinstance(s, (bytes, bytearray)) else str(s) for s in sids]
    if not sids:
        return [], []

    coords = {
        "lat":            _arr("lat") or [],
        "lon":            _arr("lon") or [],
        "country":        _arr("country") or [],
        "site_name":      _arr("site_name") or _arr("station_name") or [],
        "source_doi":     _arr("source_doi") or [],
        "citation":       _arr("citation") or [],
        "station_url":    _arr("station_url") or [],
        "intake_height":  _arr("intake_height") or [],
        "dataset_name":   _arr("dataset_name") or [],
    }

    def _at(name, idx, default=None):
        arr = coords.get(name) or []
        if idx >= len(arr):
            return default
        v = arr[idx]
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", errors="replace")
        return v

    wanted = set(surviving_stations)
    stations_out: list[dict] = []
    dataobjects_out: list[dict] = []
    seen_pids: set[str] = set()

    for i, sid in enumerate(sids):
        if sid not in wanted:
            continue

        lat = _at("lat", i, None)
        lon = _at("lon", i, None)
        intake = _at("intake_height", i, None)
        url = _at("station_url", i, "")
        pid = _at("source_doi", i, "")

        st_node: dict = {
            "@id":        f"#station/{sid}",
            "@type":      "icos:Station",
            "identifier": sid,
            "name":       _at("site_name", i, sid),
            "url":        url,
        }
        if isinstance(lat, float) and isinstance(lon, float):
            st_node["geo"] = {"@type": "GeoCoordinates",
                              "latitude": lat, "longitude": lon}
        if isinstance(intake, float):
            st_node["intakeHeight"] = intake
        if (cc := _at("country", i, "")):
            st_node["addressCountry"] = cc
        if pid:
            st_node["isBasedOn"] = {"@id": pid}
        stations_out.append(st_node)

        # One DataObject per source PID (deduplicated across stations).
        if pid and pid not in seen_pids:
            seen_pids.add(pid)
            do_node: dict = {
                "@id":      pid,
                "@type":    ["Dataset", "icos:DataObject"],
                "url":      pid,
            }
            if (cit := _at("citation", i, "")):
                do_node["citation"] = cit
            if (fname := _at("dataset_name", i, "")):
                do_node["name"] = fname
            dataobjects_out.append(do_node)

    return stations_out, dataobjects_out


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

    # Surviving stations from the client query log (for combined-view groups).
    surviving_stations: list[str] = []
    for entry in reversed(session.queries or []):
        if "surviving_stations" in entry:
            surviving_stations = list(entry["surviving_stations"])
            break

    # Build Station + DataObject entities by reading the on-disk combined-view
    # group's 1-D coords. The group path is taken from the first session group
    # that has a `station` coord; falls back to no entities.
    stations_entities: list[dict] = []
    dataobjects_entities: list[dict] = []
    if surviving_stations:
        for g in sorted(session.groups):
            stations_entities, dataobjects_entities = _station_entities(
                store_path, g, surviving_stations,
            )
            if stations_entities:
                break

    # Aggregate "hasPart" for the passport node = arrays + stations + dataobjects
    passport_has_parts = (
        has_parts
        + [{"@id": s["@id"]} for s in stations_entities]
        + [{"@id": d["@id"]} for d in dataobjects_entities]
    )

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
                "url":              "",
                "dateAccessed":     _ts_iso(session.started_at),
                "sessionStart":     _ts_iso(session.started_at),
                "sessionEnd":       _ts_iso(session.last_seen),
                "agent": {
                    "@type":        "Agent",
                    "ipAnonymised": session.ip_anonymised,
                },
                "accessedGroups":   sorted(session.groups),
                "accessedArrays":   sorted(session.arrays),
                "stations":         surviving_stations,
                "query":            session.queries or [],
                "hasPart":          passport_has_parts,
                "totalBytesServed": session.bytes_total,
                "totalChunks":      len(session.chunks),
                "isPartOf":         {"@id": station_meta.get("source_doi", "")},
                "citation":         station_meta.get("citation", ""),
                "passportSha256":   None,   # placeholder — filled below
            },
            *stations_entities,
            *dataobjects_entities,
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
