"""
render_passport.py — generate an HTML landing page from a passport .jsonld file.

Usage:
    python render_passport.py passport.jsonld            # writes passport.html
    python render_passport.py passport.jsonld -o out.html
"""
import argparse
import json
import pathlib
import sys
from html import escape


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_bytes(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f} MB ({n:,} bytes)"
    if n >= 1_000:
        return f"{n / 1_000:.1f} KB ({n:,} bytes)"
    return f"{n:,} bytes"


def _fmt_query_step(step: dict) -> str:
    var   = escape(step.get("variable", "?"))
    group = escape(step.get("group", ""))
    parts = [f'<span class="q-kw">ds</span>[<span class="q-val">"{var}"</span>]']
    if group:
        parts.append(f'&nbsp;·&nbsp; group <span class="mono">{group}</span>')

    for op in ("sel", "isel"):
        if op in step:
            indexers = step[op]
            args = []
            for k, v in indexers.items():
                if isinstance(v, dict) and set(v) == {"start", "stop", "step"}:
                    start = v["start"] if v["start"] is not None else ""
                    stop  = v["stop"]  if v["stop"]  is not None else ""
                    val   = f'slice({escape(str(start))}, {escape(str(stop))})'
                else:
                    val = f'"{escape(str(v))}"' if isinstance(v, str) else escape(str(v))
                args.append(f'{escape(k)}=<span class="q-val">{val}</span>')
            parts.append(f'&nbsp;<span class="q-kw">.{op}</span>('
                         + ", ".join(args) + ")")

    if not any(op in step for op in ("sel", "isel")):
        parts.append('&nbsp;<span style="color:#999; font-size:0.78rem;">'
                     '(no further indexing — full array fetched)</span>')

    return "".join(parts)


CSS = """
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 15px; color: #1a1a2e; background: #f5f7fa; padding: 2rem 1rem;
    }
    .container { max-width: 860px; margin: 0 auto; }
    .header {
      background: #00558b; color: #fff; border-radius: 8px 8px 0 0;
      padding: 1.5rem 2rem; display: flex; align-items: center; gap: 1rem;
    }
    .header h1 { font-size: 1.2rem; font-weight: 600; line-height: 1.4; }
    .header .subtitle { font-size: 0.85rem; opacity: 0.8; margin-top: 0.2rem; }
    .pid-bar {
      background: #003f6b; color: #7ecfff; padding: 0.6rem 2rem;
      font-size: 0.85rem; font-family: monospace;
      display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap;
    }
    .pid-bar a { color: #7ecfff; }
    .pid-label { opacity: 0.7; font-family: inherit; }
    .card {
      background: #fff; border: 1px solid #dde3ec; border-top: none; padding: 1.5rem 2rem;
    }
    .card + .card { border-top: 1px solid #dde3ec; }
    .card:last-child { border-radius: 0 0 8px 8px; }
    .section-title {
      font-size: 0.72rem; font-weight: 700; letter-spacing: 0.08em;
      text-transform: uppercase; color: #00558b; margin-bottom: 0.9rem;
    }
    .kv { display: grid; grid-template-columns: 180px 1fr; row-gap: 0.45rem; }
    .kv dt { color: #555; font-size: 0.88rem; }
    .kv dd { font-size: 0.88rem; word-break: break-word; }
    .kv dd a { color: #00558b; }
    .chips { display: flex; flex-wrap: wrap; gap: 0.4rem; }
    .chip {
      display: inline-block; background: #e8f4ff; color: #00558b;
      border: 1px solid #b0d8f5; border-radius: 4px;
      padding: 0.2rem 0.55rem; font-size: 0.8rem; font-family: monospace;
    }
    .query-list { list-style: none; }
    .query-list li {
      background: #f8fafc; border: 1px solid #dde3ec; border-radius: 4px;
      padding: 0.55rem 0.8rem; margin-bottom: 0.4rem;
      font-family: monospace; font-size: 0.82rem; color: #2d3a4a;
    }
    .query-list .q-num { display: inline-block; width: 1.6rem; color: #999; }
    .q-kw { color: #00558b; font-weight: 600; }
    .q-val { color: #c0392b; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    thead th {
      text-align: left; padding: 0.5rem 0.7rem; background: #f0f4f8;
      border-bottom: 2px solid #dde3ec; font-weight: 600; color: #444;
    }
    tbody tr:nth-child(even) td { background: #fafbfd; }
    tbody td { padding: 0.45rem 0.7rem; border-bottom: 1px solid #edf0f5; }
    .mono { font-family: monospace; }
    .integrity {
      background: #f0f4f8; border: 1px solid #dde3ec; border-radius: 6px;
      padding: 1rem 1.2rem;
    }
    .integrity .hash {
      font-family: monospace; font-size: 0.82rem;
      word-break: break-all; color: #2d3a4a; margin-top: 0.4rem;
    }
    .integrity .verify-note { font-size: 0.78rem; color: #777; margin-top: 0.5rem; }
    code {
      background: #e8ecf2; padding: 0.05rem 0.3rem; border-radius: 3px;
      font-family: monospace; font-size: 0.85em;
    }
    .footer { margin-top: 1.5rem; text-align: center; font-size: 0.78rem; color: #999; }
    .footer a { color: #00558b; }
    .dl-btn {
      background: #00558b; color: #fff; padding: 0.5rem 1.1rem;
      border-radius: 5px; text-decoration: none; font-size: 0.88rem; white-space: nowrap;
    }
"""

ICON_SVG = """<svg width="36" height="36" viewBox="0 0 36 36" fill="none"
     xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <circle cx="18" cy="18" r="18" fill="#ffffff22"/>
  <path d="M10 26 L18 10 L26 26 Z" stroke="#fff" stroke-width="2"
        fill="none" stroke-linejoin="round"/>
  <circle cx="18" cy="21" r="2" fill="#7ecfff"/>
</svg>"""


# ── main renderer ─────────────────────────────────────────────────────────────

def render(passport_path: pathlib.Path) -> str:
    doc   = json.loads(passport_path.read_text(encoding="utf-8"))
    graph = {node["@id"]: node for node in doc["@graph"]}

    # Find the DataPassport node (not the root "./" node)
    pp = next(
        n for n in doc["@graph"]
        if isinstance(n.get("@type"), list) and "icos:DataPassport" in n["@type"]
    )

    pid        = pp["@id"]
    handle_url = f"https://hdl.handle.net/{pid.removeprefix('hdl:')}"
    name       = escape(pp.get("name", "Data Access Passport"))
    date       = escape(pp.get("sessionStart", "")[:10])
    cp_url     = escape(pp.get("url", ""))

    agent      = pp.get("agent", {})
    ip_anon    = escape(agent.get("ipAnonymised", ""))

    source_doi = pp.get("isPartOf", {}).get("@id", "")
    citation   = escape(pp.get("citation", ""))
    session_s  = escape(pp.get("sessionStart", ""))
    session_e  = escape(pp.get("sessionEnd", ""))
    total_bytes = pp.get("totalBytesServed", 0)
    total_chunks = pp.get("totalChunks", 0)
    sha256     = escape(pp.get("passportSha256", ""))

    arrays  = pp.get("accessedArrays", [])
    groups  = pp.get("accessedGroups", [])
    queries = pp.get("query", [])
    parts   = pp.get("hasPart", [])

    # ── chips ─────────────────────────────────────────────────────────────────
    array_chips = "".join(f'<span class="chip">{escape(a)}</span>' for a in arrays)
    group_chips = "".join(f'<span class="chip">{escape(g)}</span>' for g in groups)

    # ── query list ────────────────────────────────────────────────────────────
    query_items = "".join(
        f'<li><span class="q-num">{i+1}</span> {_fmt_query_step(q)}</li>'
        for i, q in enumerate(queries)
    )
    query_section = (
        f'<div class="card"><p class="section-title">Query chain (recorded client-side)</p>'
        f'<ul class="query-list">{query_items}</ul></div>'
        if queries else ""
    )

    # ── per-variable table ────────────────────────────────────────────────────
    rows = "".join(
        f"<tr>"
        f'<td class="mono">{escape(p.get("zarr_path", p.get("@id", "")))}</td>'
        f'<td>{p.get("chunkCount", "")}</td>'
        f'<td>{p.get("sizeInBytes", 0):,}</td>'
        f'<td class="mono" style="font-size:0.78rem;color:#666;">'
        f'{escape(p.get("sha256", "")[:16])}…{escape(p.get("sha256", "")[-8:])}</td>'
        f"</tr>"
        for p in parts
    )

    # ── source DOI link ───────────────────────────────────────────────────────
    if source_doi:
        doi_link = f'<a href="{escape(source_doi)}">{escape(source_doi)}</a>'
    else:
        doi_link = "—"

    # ── download link ─────────────────────────────────────────────────────────
    jsonld_filename = passport_path.with_suffix(".jsonld").name

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{name}</title>
  <style>{CSS}</style>
</head>
<body>
<div class="container">

  <div class="header">
    {ICON_SVG}
    <div>
      <h1>{name}</h1>
      <div class="subtitle">ICOS Carbon Portal · Fluxnet data delivery record · {date}</div>
    </div>
  </div>

  <div class="pid-bar">
    <span class="pid-label">Handle PID</span>
    <a href="{escape(handle_url)}">{escape(pid)}</a>
  </div>

  <div class="card">
    <p class="section-title">Session summary</p>
    <dl class="kv">
      <dt>Source dataset</dt><dd>{doi_link}</dd>
      <dt>Citation</dt><dd>{citation}</dd>
      <dt>Session start</dt><dd>{session_s}</dd>
      <dt>Session end</dt><dd>{session_e}</dd>
      <dt>Client (anonymised)</dt><dd class="mono">{ip_anon}</dd>
      <dt>Chunks delivered</dt><dd>{total_chunks:,}</dd>
      <dt>Bytes delivered</dt><dd>{_fmt_bytes(total_bytes)}</dd>
    </dl>
  </div>

  <div class="card">
    <p class="section-title">Variables accessed</p>
    <div class="chips">{array_chips}</div>
    <p style="margin-top:1.1rem;" class="section-title">Groups accessed</p>
    <div class="chips">{group_chips}</div>
  </div>

  {query_section}

  <div class="card">
    <p class="section-title">Delivered data — per-variable checksums</p>
    <table>
      <thead>
        <tr>
          <th>Variable (zarr path)</th><th>Chunks</th>
          <th>Bytes</th><th>SHA-256 (aggregate)</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <p style="font-size:0.78rem;color:#777;margin-top:0.6rem;">
      Aggregate SHA-256 = SHA-256 of the sorted list of individual chunk digests.
    </p>
  </div>

  <div class="card">
    <p class="section-title">Passport integrity</p>
    <div class="integrity">
      <div>Passport SHA-256</div>
      <div class="hash">{sha256}</div>
      <div class="verify-note">
        Computed over the canonical JSON-LD (<code>sort_keys=True</code>, no whitespace)
        with <code>"passportSha256": null</code>. To verify: set that field to
        <code>null</code>, re-serialise with the same settings, and compare the
        SHA-256 to the value above.
      </div>
    </div>
  </div>

  <div class="card" style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;">
    <div>
      <p class="section-title" style="margin-bottom:0.2rem;">Machine-readable passport</p>
      <p style="font-size:0.85rem;color:#555;">
        Full ROCrate JSON-LD document — cite this PID in your methods section.
      </p>
    </div>
    <a href="{jsonld_filename}" class="dl-btn" style="margin-left:auto;">
      Download .jsonld
    </a>
  </div>

</div>

<div class="footer">
  <a href="https://www.icos-cp.eu/">ICOS Carbon Portal</a> ·
  Passport generated {session_e} ·
  <a href="https://w3id.org/ro/crate/1.1">ROCrate 1.1</a>
</div>
</body>
</html>"""


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Render a passport .jsonld as HTML")
    ap.add_argument("jsonld", type=pathlib.Path, help="Input .jsonld file")
    ap.add_argument("-o", "--output", type=pathlib.Path, default=None,
                    help="Output .html file (default: same stem as input)")
    args = ap.parse_args()

    out = args.output or args.jsonld.with_suffix(".html")
    html = render(args.jsonld)
    out.write_text(html, encoding="utf-8")
    print(f"Written: {out}")


if __name__ == "__main__":
    main()
