"""
Pre-deployment tests for zarr_proxy.

Covers:
  - Session accumulation and IP anonymisation
  - Passport building: chunk manifest, per-variable SHA-256, self-describing passportSha256
  - FastAPI endpoints: root listing, store key serving, session/close, session/passport
  - Path traversal protection
  - _resolve_store rejects non-zarr directories
  - X-DataPassport-Warning header on unrecognised clients
  - Query deduplication in _TrackedArray (sel/isel replaces bare __getitem__ entry)
  - render_passport produces valid HTML from a passport JSON-LD

External services (Handle, CP, Matomo) are not called — all integration points are patched.
"""
import hashlib
import json
import pathlib
import time

import pytest

# ── session ───────────────────────────────────────────────────────────────────

from zarr_proxy.session import (
    ChunkRecord, Session, _is_chunk_key,
    get_or_create, pop, record,
    _sessions,
)


def _make_chunk(key: str, payload: bytes = b"x" * 128) -> tuple[str, bytes]:
    return key, payload


def teardown_function():
    _sessions.clear()


# -- _is_chunk_key -------------------------------------------------------------

def test_is_chunk_key_simple():
    assert _is_chunk_key("0")
    assert _is_chunk_key("0.0.0")
    assert _is_chunk_key("12.34")

def test_is_chunk_key_rejects_names():
    assert not _is_chunk_key("NEE")
    assert not _is_chunk_key("SE-Svb")
    assert not _is_chunk_key("")
    assert not _is_chunk_key(".zarray")


# -- Session.record_chunk ------------------------------------------------------

def test_record_chunk_extracts_group_and_array():
    s = Session(ip="1.2.3.4", store="test.zarr")
    s.record_chunk("SE-Svb/NEE/0.0.0", b"data")
    assert "SE-Svb" in s.groups
    assert "NEE" in s.arrays

def test_record_chunk_subgroup():
    s = Session(ip="1.2.3.4", store="test.zarr")
    s.record_chunk("SE-Svb/fluxnet_dd/GPP/0.0", b"data")
    assert "SE-Svb/fluxnet_dd" in s.groups
    assert "GPP" in s.arrays

def test_record_chunk_accumulates_bytes():
    s = Session(ip="1.2.3.4", store="test.zarr")
    s.record_chunk("SE-Svb/NEE/0", b"a" * 100)
    s.record_chunk("SE-Svb/NEE/1", b"b" * 200)
    assert s.bytes_total == 300
    assert len(s.chunks) == 2

def test_record_chunk_sha256():
    data = b"hello"
    s = Session(ip="1.2.3.4", store="test.zarr")
    s.record_chunk("SE-Svb/NEE/0", data)
    assert s.chunks[0].sha256 == hashlib.sha256(data).hexdigest()


# -- Session.ip_anonymised -----------------------------------------------------

def test_ip_anonymised_ipv4():
    s = Session(ip="192.168.1.42", store="x")
    assert s.ip_anonymised == "192.168.1.0/24"

def test_ip_anonymised_ipv6():
    s = Session(ip="2001:db8:85a3::8a2e:370:7334", store="x")
    assert s.ip_anonymised == "2001:db8:85a3::/48"


# -- Session registry ----------------------------------------------------------

def test_get_or_create_new():
    s = get_or_create("10.0.0.1", "a.zarr")
    assert s.ip == "10.0.0.1"
    assert s.store == "a.zarr"

def test_get_or_create_same_ip_different_store():
    s1 = get_or_create("10.0.0.1", "a.zarr")
    s2 = get_or_create("10.0.0.1", "b.zarr")
    assert s1 is not s2

def test_get_or_create_returns_same():
    s1 = get_or_create("10.0.0.2", "a.zarr")
    s2 = get_or_create("10.0.0.2", "a.zarr")
    assert s1 is s2

def test_pop_removes_session():
    get_or_create("10.0.0.3", "a.zarr")
    s = pop("10.0.0.3", "a.zarr")
    assert s is not None
    assert pop("10.0.0.3", "a.zarr") is None

def test_record_creates_session():
    record("10.0.0.4", "a.zarr", "SE-Svb/NEE/0", b"chunk")
    s = pop("10.0.0.4", "a.zarr")
    assert s is not None
    assert len(s.chunks) == 1


# ── passport ──────────────────────────────────────────────────────────────────

from zarr_proxy.passport import build, save


def _make_session(chunks: list[tuple[str, bytes]] | None = None) -> Session:
    s = Session(ip="1.2.3.4", store="test.zarr")
    for key, data in (chunks or [("SE-Svb/NEE/0.0", b"a" * 64)]):
        s.record_chunk(key, data)
    return s


def test_build_returns_dict_and_sha256():
    s = _make_session()
    passport, sha256 = build(s)
    assert isinstance(passport, dict)
    assert isinstance(sha256, str)
    assert len(sha256) == 64


def test_build_graph_structure():
    s = _make_session()
    passport, _ = build(s)
    graph = passport["@graph"]
    assert any(n["@id"] == "./" for n in graph)
    pp = next(n for n in graph if isinstance(n.get("@type"), list))
    assert "icos:DataPassport" in pp["@type"]


def test_build_has_parts_aggregated():
    data_a = b"chunk_a" * 10
    data_b = b"chunk_b" * 10
    s = _make_session([
        ("SE-Svb/NEE/0.0", data_a),
        ("SE-Svb/NEE/0.1", data_b),
    ])
    passport, _ = build(s)
    pp = next(n for n in passport["@graph"] if isinstance(n.get("@type"), list))
    parts = {p["zarr_path"]: p for p in pp["hasPart"]}
    assert "SE-Svb/NEE" in parts
    nee = parts["SE-Svb/NEE"]
    assert nee["chunkCount"] == 2
    assert nee["sizeInBytes"] == len(data_a) + len(data_b)
    # aggregate SHA-256 = sha256 of sorted chunk digests
    d_a = hashlib.sha256(data_a).hexdigest()
    d_b = hashlib.sha256(data_b).hexdigest()
    expected = hashlib.sha256("\n".join(sorted([d_a, d_b])).encode()).hexdigest()
    assert nee["sha256"] == expected


def test_build_passport_sha256_self_consistent():
    """passportSha256 must equal SHA-256 of the passport with that field set to null."""
    s = _make_session()
    passport, sha256 = build(s)
    pp = passport["@graph"][1]
    assert pp["passportSha256"] == sha256
    # verify: set to null, recompute
    pp["passportSha256"] = None
    recomputed = hashlib.sha256(
        json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert recomputed == sha256


def test_build_accessed_arrays_sorted():
    s = _make_session([
        ("SE-Svb/NEE/0", b"a"),
        ("SE-Svb/GPP/0", b"b"),
        ("SE-Svb/TA/0",  b"c"),
    ])
    passport, _ = build(s)
    pp = passport["@graph"][1]
    assert pp["accessedArrays"] == sorted(pp["accessedArrays"])


def test_build_total_bytes_and_chunks():
    chunks = [("SE-Svb/NEE/0", b"x" * 100), ("SE-Svb/NEE/1", b"y" * 200)]
    s = _make_session(chunks)
    passport, _ = build(s)
    pp = passport["@graph"][1]
    assert pp["totalBytesServed"] == 300
    assert pp["totalChunks"] == 2


def test_build_query_embedded():
    s = _make_session()
    s.queries = [{"variable": "NEE", "group": "SE-Svb", "isel": {"time": 0}}]
    passport, _ = build(s)
    pp = passport["@graph"][1]
    assert pp["query"] == s.queries


def test_save_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr("zarr_proxy.passport.config.PASSPORT_DIR", str(tmp_path))
    s = _make_session()
    passport, sha256 = build(s)
    path = save(passport, sha256)
    assert path.exists()
    assert path.name == f"{sha256[:16]}.jsonld"
    doc = json.loads(path.read_text())
    assert doc["@graph"][1]["passportSha256"] == sha256


# ── FastAPI endpoints ─────────────────────────────────────────────────────────

from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient


@pytest.fixture()
def zarr_store(tmp_path):
    """Create a minimal zarr v2 store with one array."""
    store = tmp_path / "test.zarr"
    store.mkdir()
    (store / ".zgroup").write_text('{"zarr_format":2}')
    (store / ".zmetadata").write_text(json.dumps({
        "metadata": {
            ".zgroup": {"zarr_format": 2},
            "SE-Svb/.zgroup": {"zarr_format": 2},
            "SE-Svb/NEE/.zarray": {
                "zarr_format": 2, "dtype": "<f4",
                "shape": [10], "chunks": [5],
                "compressor": None, "filters": None,
                "fill_value": 0, "order": "C",
            },
        },
        "zarr_consolidated_format": 1,
    }))
    grp = store / "SE-Svb"
    grp.mkdir()
    (grp / ".zgroup").write_text('{"zarr_format":2}')
    arr = grp / "NEE"
    arr.mkdir()
    (arr / ".zarray").write_text('{"zarr_format":2}')
    chunk = b"\x00" * 20
    (arr / "0").write_bytes(chunk)
    (arr / "1").write_bytes(chunk)
    return store, tmp_path


@pytest.fixture()
def client(zarr_store, monkeypatch):
    store, store_dir = zarr_store
    monkeypatch.setenv("ZARR_STORE_DIR", str(store_dir))
    # Patch heavy integrations so they don't make network calls
    monkeypatch.setattr("zarr_proxy.main.passport.build",
                        lambda s: ({"@graph": [{}, {"@id": "urn:uuid:test",
                                                    "@type": ["Dataset", "icos:DataPassport"],
                                                    "passportSha256": "abc"}]}, "abc"))
    monkeypatch.setattr("zarr_proxy.main.passport.save", lambda p, s: pathlib.Path("/tmp/x"))
    monkeypatch.setattr("zarr_proxy.main.handle_client.mint", lambda **kw: "")
    monkeypatch.setattr("zarr_proxy.main.handle_client.update", lambda *a, **kw: False)
    monkeypatch.setattr("zarr_proxy.main.matomo_client.track", lambda *a, **kw: None)

    # Patch cp_client inside _mint_passport's local import
    import zarr_proxy.cp_client as cp
    monkeypatch.setattr(cp, "upload", lambda *a, **kw: "")

    # Reload config with patched env
    import zarr_proxy.config as cfg
    import importlib
    importlib.reload(cfg)

    from zarr_proxy import main as proxy_main
    importlib.reload(proxy_main)

    from zarr_proxy.main import app
    _sessions.clear()
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c, store.name
    _sessions.clear()


# -- GET / --------------------------------------------------------------------

def test_root_lists_stores(client):
    c, store_name = client
    resp = c.get("/")
    assert resp.status_code == 200
    assert store_name in resp.json()["stores"]


def test_root_excludes_non_zarr_dirs(client, zarr_store, monkeypatch):
    c, store_name = client
    _, store_dir = zarr_store
    # Create a plain directory without .zgroup
    (store_dir / "not-a-store").mkdir()
    resp = c.get("/")
    assert "not-a-store" not in resp.json()["stores"]


# -- GET /{store_name}/ -------------------------------------------------------

def test_store_root_serves_zgroup(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/")
    assert resp.status_code == 200
    assert resp.json()["zarr_format"] == 2


def test_store_root_list(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/", params={"list": ""})
    assert resp.status_code == 200
    entries = resp.json()
    assert isinstance(entries, list)
    assert "SE-Svb" in entries


def test_unknown_store_returns_404(client):
    c, _ = client
    resp = c.get("/nonexistent.zarr/")
    assert resp.status_code == 404
    assert "error" in resp.json()


# -- GET /{store_name}/{key:path} ---------------------------------------------

def test_serve_metadata(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/.zmetadata")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")


def test_serve_chunk(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/SE-Svb/NEE/0")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/octet-stream"


def test_chunk_tracked_in_session(client):
    c, store_name = client
    c.get(f"/{store_name}/SE-Svb/NEE/0")
    s = _sessions.get(("testclient", store_name))
    assert s is not None
    assert len(s.chunks) == 1


def test_metadata_not_tracked(client):
    c, store_name = client
    c.get(f"/{store_name}/.zmetadata")
    s = _sessions.get(("testclient", store_name))
    assert s is None or len(s.chunks) == 0


def test_missing_key_returns_404(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/SE-Svb/MISSING/0")
    assert resp.status_code == 404


def test_path_traversal_rejected(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/../../../etc/passwd")
    assert resp.status_code in (403, 404)


def test_directory_key_returns_404(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/SE-Svb")
    assert resp.status_code == 404


# -- X-DataPassport-Warning header --------------------------------------------

def test_warning_header_without_client(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/SE-Svb/NEE/0")
    assert "X-DataPassport-Warning" in resp.headers


def test_no_warning_header_with_client(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/SE-Svb/NEE/0",
                 headers={"X-DataPassport-Client": "datapassport_zarr"})
    assert "X-DataPassport-Warning" not in resp.headers


# -- POST /{store_name}/session/close -----------------------------------------

def test_session_close_no_chunks(client):
    c, store_name = client
    resp = c.post(f"/{store_name}/session/close")
    assert resp.status_code == 200
    assert resp.json()["chunks"] == 0
    assert resp.json()["passport_pid"] == ""


def test_session_close_after_chunks(client):
    c, store_name = client
    c.get(f"/{store_name}/SE-Svb/NEE/0")
    c.get(f"/{store_name}/SE-Svb/NEE/1")
    resp = c.post(f"/{store_name}/session/close",
                  json={"queries": [{"variable": "NEE", "group": "SE-Svb"}]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["chunks"] == 2
    assert body["bytes_served"] > 0
    assert "NEE" in body["arrays"]


def test_session_close_idempotent(client):
    c, store_name = client
    c.get(f"/{store_name}/SE-Svb/NEE/0")
    c.post(f"/{store_name}/session/close")
    # Second close — session already popped, should return zero-chunks result
    resp = c.post(f"/{store_name}/session/close")
    assert resp.json()["chunks"] == 0


def test_session_close_unknown_store(client):
    c, _ = client
    resp = c.post("/nonexistent.zarr/session/close")
    assert resp.status_code == 404
    assert "error" in resp.json()


# -- GET /{store_name}/session/passport ---------------------------------------

def test_session_passport_empty(client):
    c, store_name = client
    resp = c.get(f"/{store_name}/session/passport")
    assert resp.status_code == 200
    body = resp.json()
    assert "passport_pid" in body
    assert "session_open" in body
    assert "chunks" in body


def test_session_passport_after_chunks(client):
    c, store_name = client
    c.get(f"/{store_name}/SE-Svb/NEE/0")
    resp = c.get(f"/{store_name}/session/passport")
    assert resp.json()["session_open"] is True
    assert resp.json()["chunks"] == 1


# ── query deduplication (_TrackedArray) ──────────────────────────────────────

import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from unittest.mock import MagicMock
import xarray as xr
import numpy as np

from datapassport_zarr import DataPassportDataset, _TrackedArray


def _mock_dataset(variables=("NEE",)):
    data = {v: xr.DataArray(np.zeros(5), dims=["time"]) for v in variables}
    return xr.Dataset(data)


def test_getitem_appends_bare_entry():
    ds = DataPassportDataset(_mock_dataset(), proxy_url="http://x", group="SE-Svb",
                             save_passport=False, verbose=False)
    _ = ds["NEE"]
    assert len(ds._queries) == 1
    assert "sel" not in ds._queries[0]
    assert "isel" not in ds._queries[0]


def test_isel_replaces_bare_getitem():
    ds = DataPassportDataset(_mock_dataset(), proxy_url="http://x", group="SE-Svb",
                             save_passport=False, verbose=False)
    ds["NEE"].isel(time=0)
    assert len(ds._queries) == 1
    assert "isel" in ds._queries[0]


def test_sel_replaces_bare_getitem():
    ds = DataPassportDataset(_mock_dataset(), proxy_url="http://x", group="SE-Svb",
                             save_passport=False, verbose=False)
    ds["NEE"].sel(time=2)
    assert len(ds._queries) == 1
    assert "sel" in ds._queries[0]


def test_two_variables_not_merged():
    ds = DataPassportDataset(_mock_dataset(("NEE", "GPP")), proxy_url="http://x",
                             group="SE-Svb", save_passport=False, verbose=False)
    ds["NEE"].isel(time=0)
    ds["GPP"].isel(time=1)
    assert len(ds._queries) == 2


def test_double_isel_both_recorded():
    """Chained isel calls should each produce their own entry."""
    ds = DataPassportDataset(_mock_dataset(), proxy_url="http://x", group="SE-Svb",
                             save_passport=False, verbose=False)
    ds["NEE"].isel(time=slice(0, 3)).isel(time=0)
    assert len(ds._queries) == 2


def test_bare_getitem_kept_without_sel():
    """If user just does ds["NEE"].values with no sel/isel, bare entry stays."""
    ds = DataPassportDataset(_mock_dataset(("NEE", "GPP")), proxy_url="http://x",
                             group="SE-Svb", save_passport=False, verbose=False)
    ds["NEE"]
    ds["GPP"].isel(time=0)  # different variable — should NOT replace NEE entry
    assert len(ds._queries) == 2
    assert "isel" not in ds._queries[0]   # NEE bare entry unchanged
    assert "isel" in ds._queries[1]       # GPP isel


# ── render_passport ───────────────────────────────────────────────────────────

from zarr_proxy.render_passport import render


def test_render_produces_html(tmp_path):
    src = pathlib.Path(__file__).parent.parent / "zarr_proxy" / "landing_page_example.jsonld"
    html = render(src)
    assert html.strip().startswith("<!DOCTYPE html>")
    assert "hdl:11676" in html
    assert "Passport SHA-256" in html     # integrity section heading present
    assert "SE-Svb" in html


def test_render_escapes_html(tmp_path):
    """Malicious content in passport fields must be escaped."""
    doc = json.loads(
        (pathlib.Path(__file__).parent.parent / "zarr_proxy" / "landing_page_example.jsonld")
        .read_text()
    )
    pp = next(n for n in doc["@graph"] if isinstance(n.get("@type"), list))
    pp["citation"] = '<script>alert("xss")</script>'
    path = tmp_path / "malicious.jsonld"
    path.write_text(json.dumps(doc))
    html = render(path)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_render_query_section_omitted_when_empty(tmp_path):
    doc = json.loads(
        (pathlib.Path(__file__).parent.parent / "zarr_proxy" / "landing_page_example.jsonld")
        .read_text()
    )
    pp = next(n for n in doc["@graph"] if isinstance(n.get("@type"), list))
    pp["query"] = []
    path = tmp_path / "no_query.jsonld"
    path.write_text(json.dumps(doc))
    html = render(path)
    assert "Query chain" not in html
