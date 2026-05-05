"""Inline the fixture + LIVE BEP evidence into zone-rules-editor.html.

Pulls every dogs-field-group BEP row for the 8 anchor fids and embeds them
as evidence cards. This makes the mock UI reflect actual production data
shape (including json_explode rows that have only structured fields and
no prose, and is_canonical winners highlighted).
"""
import json
import os
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

ANCHORS = [6202, 9716, 8339, 9717, 3407, 8673, 8356, 8740]

fixture = json.loads((ROOT / "tests" / "zone_rules_anchors.json").read_text(encoding="utf-8"))


def quote_from_row(r):
    """Compose the displayable quote from whichever prose fields are populated.
    json_explode / unified_v1 rows often have only structured allowed/leash and
    no prose — for those we emit a synthetic placeholder so the moderator still
    sees the source contributing to consensus."""
    parts = []
    if r["notes"]:
        parts.append(r["notes"])
    if r["zone_desc"]:
        if parts:
            parts.append("")
        parts.append(f"[zones] {r['zone_desc']}")
    if r["designated"]:
        parts.append(f"[designated dog zones] {r['designated']}")
    if r["prohibited"]:
        parts.append(f"[prohibited areas] {r['prohibited']}")
    if not parts:
        # Structured-only row: synthesize a label from the claims
        bits = []
        if r["allowed"] is not None:
            bits.append(f"allowed={r['allowed']}")
        if r["leash"] is not None:
            bits.append(f"leash={r['leash']}")
        return "(no prose; structured claim only — " + ", ".join(bits) + ")"
    return "\n\n".join(parts)


with psycopg2.connect(**PG) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            select g.fid, e.source, e.confidence::float as confidence, e.is_canonical,
                   e.claimed_values->>'allowed' as allowed,
                   e.claimed_values->>'leash_required' as leash,
                   e.claimed_values->>'notes' as notes,
                   e.claimed_values->>'zone_description' as zone_desc,
                   e.claimed_values->>'designated_dog_zones' as designated,
                   e.claimed_values->>'prohibited_areas' as prohibited,
                   e.source_url
              from public.beach_enrichment_provenance e
              join public.beaches_gold g on g.fid = e.gold_fid
             where g.fid = any(%s)
               and e.field_group = 'dogs'
             order by g.fid, e.is_canonical desc, e.confidence desc, e.source
            """,
            (ANCHORS,),
        )
        rows = cur.fetchall()

evidence = {}
for r in rows:
    fid = r["fid"]
    evidence.setdefault(fid, []).append({
        "source": r["source"],
        "confidence": float(r["confidence"]) if r["confidence"] is not None else 0.5,
        "is_canonical": bool(r["is_canonical"]),
        "allowed": r["allowed"],
        "leash": r["leash"],
        "quote": quote_from_row(r),
        "source_url": r["source_url"],
    })

print(f"BEP evidence rows pulled:")
for fid in ANCHORS:
    print(f"  fid={fid:>5}  rows={len(evidence.get(fid, []))}")

fixture_js = json.dumps(fixture, indent=2)
evidence_js = json.dumps(evidence, indent=2)

path = ROOT / "admin" / "zone-rules-editor.html"
html = path.read_text(encoding="utf-8")

# Replace the FIXTURE/EVIDENCE constants. The placeholders are only present
# on first run; on re-runs we replace the previously-inlined value instead.
import re
# Use callable replacement so JSON's \u escapes don't trigger re's
# replacement-string parser.
html = re.sub(
    r"const FIXTURE = (?:/\* %FIXTURE% \*/|\{[\s\S]*?\n\});",
    lambda _m: f"const FIXTURE = {fixture_js};",
    html,
    count=1,
)
html = re.sub(
    r"const EVIDENCE = (?:/\* %EVIDENCE% \*/|\{[\s\S]*?\n\});",
    lambda _m: f"const EVIDENCE = {evidence_js};",
    html,
    count=1,
)
path.write_text(html, encoding="utf-8")

print(f"inlined fixture ({len(fixture_js)} chars) + evidence ({len(evidence_js)} chars)")
print(f"total file size: {len(html)} chars")
