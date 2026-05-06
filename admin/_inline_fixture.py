"""Inline the fixture + LIVE BEP evidence into zone-rules-editor.html.

Pulls every dogs-field-group BEP row for the 8 anchor fids and embeds them
as evidence cards. Also appends a section-heavy sample of production
text_repass_v1 outputs so the mock can browse what the LLM actually
produced across the 440-beach repass.
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

# Section-heavy production samples — top N text_repass_v1 rows by section count
# (mix of multi-region and many-section beaches). Pulled live below.
PRODUCTION_SAMPLE_LIMIT = 25
PRODUCTION_SAMPLE_MIN_SECTIONS = 6

# Force-include these fids in the production-samples picker regardless of
# section count. Useful for following a specific cluster (Huntington Beach,
# Coronado complex, Point Reyes carve-outs) end-to-end across the mock.
FORCE_INCLUDE_FIDS = [
    8901,   # Huntington City Beach
    8453,   # Huntington State Beach
]

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

print(f"BEP evidence rows pulled (anchors):")
for fid in ANCHORS:
    print(f"  fid={fid:>5}  rows={len(evidence.get(fid, []))}")

# ============================================================================
# Production samples — section-heavy text_repass_v1 outputs
# ============================================================================
print(f"\nPulling section-heavy production samples (text_repass_v1)...")

with psycopg2.connect(**PG) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            with z as (
              select bep.gold_fid as fid, g.name, g.county_name,
                     bep.claimed_values->'zone_rules' as zr,
                     bep.confidence::float as confidence,
                     jsonb_array_length(bep.claimed_values->'zone_rules'->'regions') as n_regions,
                     (select count(*)
                        from jsonb_array_elements(bep.claimed_values->'zone_rules'->'regions') r,
                             lateral jsonb_each(coalesce(r->'sections','{}'::jsonb))) as n_sections
                from public.beach_enrichment_provenance bep
                join public.beaches_gold g on g.fid = bep.gold_fid
               where bep.source = 'text_repass_v1' and bep.field_group = 'dogs'
                 and bep.gold_fid != all(%s)
            )
            select fid, name, county_name, zr, n_regions, n_sections, confidence
              from z
             where n_sections >= %s or fid = any(%s)
             order by
               case when fid = any(%s) then 0 else 1 end,
               n_regions desc, n_sections desc
             limit %s
            """,
            (ANCHORS, PRODUCTION_SAMPLE_MIN_SECTIONS, FORCE_INCLUDE_FIDS,
             FORCE_INCLUDE_FIDS, PRODUCTION_SAMPLE_LIMIT + len(FORCE_INCLUDE_FIDS)),
        )
        prod_rows = [dict(r) for r in cur.fetchall()]

# Pull evidence for each production sample
for r in prod_rows:
    fid = r["fid"]
    if fid in evidence:
        continue  # already loaded as an anchor
    with psycopg2.connect(**PG) as conn:
        _b, _rows = None, []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                select source, confidence::float as confidence, is_canonical,
                       claimed_values->>'allowed' as allowed,
                       claimed_values->>'leash_required' as leash,
                       claimed_values->>'notes' as notes,
                       claimed_values->>'zone_description' as zone_desc,
                       claimed_values->>'designated_dog_zones' as designated,
                       claimed_values->>'prohibited_areas' as prohibited,
                       source_url
                  from public.beach_enrichment_provenance
                 where gold_fid = %s and field_group = 'dogs'
                 order by is_canonical desc, confidence desc, source
                """,
                (fid,),
            )
            for row in cur.fetchall():
                evidence.setdefault(fid, []).append({
                    "source": row["source"],
                    "confidence": float(row["confidence"]) if row["confidence"] is not None else 0.5,
                    "is_canonical": bool(row["is_canonical"]),
                    "allowed": row["allowed"],
                    "leash": row["leash"],
                    "quote": quote_from_row(row),
                    "source_url": row["source_url"],
                })

# Append production samples to the fixture as additional anchors
# with a single variant 'text_repass_v1'
for r in prod_rows:
    fixture["anchors"].append({
        "fid": r["fid"],
        "name": r["name"],
        "what_it_tests": (
            f"Production text_repass_v1 sample · {r['county_name']} County · "
            f"{r['n_regions']} region(s), {r['n_sections']} section(s) · "
            f"conf {r['confidence']:.2f}"
        ),
        "is_production_sample": True,
        "variants": {
            "text_repass_v1": r["zr"],
        },
    })

print(f"  added {len(prod_rows)} production samples; total anchors now {len(fixture['anchors'])}")

fixture_js = json.dumps(fixture, indent=2, default=str)
evidence_js = json.dumps(evidence, indent=2, default=str)

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
