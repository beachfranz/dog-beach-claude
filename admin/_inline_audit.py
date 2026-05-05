"""Inline audit data into admin/dogs-allowed-audit.html.

Two modes:

  default (no flag): joins beach_policy_gold_set (truth) with beach_dog_policy
  (consumer surface), computes the audit verdict, pulls BEP evidence +
  text_repass_v1 zone_rules. The original gold-set audit view.

  --sample: loads a curated 19-beach representative sample covering the
  zone_rules spec's structural surface (single-zone, multi-region, full-
  season, partial-season, sibling-test, NPS prohibits, CDPR developed-areas).
  No verdict comparison — just a browseable sample post-seasons-at-top
  schema change.

Verdict logic (reductive — yes/mixed/seasonal/restricted are yes-family):
  false_allow  : truth='no'        and actual in yes-family
  false_deny   : truth in yes-family and actual='no'
  match        : truth == actual
  match_loose  : both yes-family but not exact-equal
  missing      : actual is null
  sample       : (--sample mode only) no truth comparison, just a structural showcase

Re-run after audit-impacting migrations or curator edits.
"""
import argparse
import json
import os
import re
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

YES_FAMILY = {"yes", "mixed", "seasonal", "restricted"}


def verdict(truth, actual):
    if actual is None:
        return "missing"
    if truth == actual:
        return "match"
    if truth in YES_FAMILY and actual in YES_FAMILY:
        return "match_loose"
    if truth == "no" and actual in YES_FAMILY:
        return "false_allow"
    if truth in YES_FAMILY and actual == "no":
        return "false_deny"
    return "match_loose"  # seasonal vs other oddities — treat as loose


def quote_from_row(r):
    parts = []
    if r["notes"]:
        parts.append(r["notes"])
    if r["zone_desc"]:
        parts.append(f"[zones] {r['zone_desc']}")
    if r["designated"]:
        parts.append(f"[designated] {r['designated']}")
    if r["prohibited"]:
        parts.append(f"[prohibited] {r['prohibited']}")
    if not parts:
        return f"(structured-only: allowed={r['allowed']}, leash={r['leash']})"
    return " ".join(parts)


ap = argparse.ArgumentParser()
ap.add_argument("--sample", action="store_true",
                help="Load the 19-beach seasons-at-top representative sample "
                     "instead of the gold-set audit.")
args = ap.parse_args()

# Curated 19-beach representative sample for the seasons-at-top schema.
# Coverage: single-zone, multi-region, full-season, partial-season,
# sibling-test, NPS-prohibits, CDPR-developed-areas. See spec memo.
SAMPLE_FIDS = [
    # First 19 (structural coverage)
    6202,  # Coronado Dog Beach (single zone, off-leash)
    6622,  # Carlsbad Lagoon Dog Beach (jurisdiction)
    6070,  # Limantour Beach (Point Reyes carve-out)
    8232,  # Salinas River State Beach (wetland trails)
    8261,  # South Salmon Creek Beach (plover candidate)
    8262,  # North Salmon Creek Beach (plover candidate)
    8265,  # Manhattan Beach (city, mixed)
    8339,  # Little Corona Del Mar (citywide time-window)
    8356,  # Mission Beach (citywide time + amenities)
    8560,  # Del Mar Dog Beach (full season + time)
    8673,  # Carpinteria State Beach (campground, CDPR)
    8715,  # Coronado Beach (sibling of Dog Beach)
    8740,  # Wildcat Beach (NPS prohibits)
    9716,  # Fiesta Island (time + Least Tern seasonal)
    9717,  # Huntington Beach Dog Beach (multi-section, bluff)
    3310,  # Montara State Beach (CDPR)
    3407,  # Garrapata State Beach (multi-region)
    3671,  # Leo Carillo State Beach (LA state beach)
    7593,  # Smith River Beach (plover potential)
    # 5 more added 2026-05-05 — explicit seasonal patterns
    5929,  # Main Beach (Laguna) — peak season Jun 15-Sep 10, time-window prohibition
    8276,  # Carmel Beach (Sonoma) — plover-context source text (siblings test)
    8358,  # Dog Beach (San Diego, Mission Bay area) — seasonal nesting
    8536,  # Tennessee Beach (GGNRA, Marin) — NPS off-leash zones
    8992,  # Del Mar Beach (sibling of Del Mar Dog Beach) — peak-season prohibition
    # 9 more added 2026-05-05 — top leaf-count beaches (structural richness)
    8338,  # Treasure Island Beach (OC) — 14 leaves, multi-region
    6001,  # Danburg Beach (Mono) — 12 leaves, 3 regions Tahoe area
    8260,  # Baker Beach (SF) — 12 leaves, 2 regions GGNRA
    8552,  # Riviera Beach (SD) — 12 leaves single region (many sections)
    8865,  # Limantour Beach (Marin dupe) — 11 leaves
    8759,  # Drakes Beach (Marin) — 11 leaves Point Reyes
    684,   # Avalis Beach (Marin) — 11 leaves
    9156,  # Amarillo Beach (LA) — 11 leaves
    8486,  # Limantour Beach (Marin dupe) — 11 leaves
]


with psycopg2.connect(**PG) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if args.sample:
            cur.execute(
                """
                select bg.fid,
                       null::text as truth, null::text as verified_by,
                       null::text as verified_at, null::text as gold_url,
                       null::text as gold_notes,
                       bg.name, bg.county_name as county, bg.state,
                       coalesce(
                         (select bep.claimed_values->>'governing_body_name'
                            from public.beach_enrichment_provenance bep
                           where bep.gold_fid = bg.fid
                             and bep.field_group = 'governance'
                             and bep.is_canonical
                             and bep.claimed_values->>'governing_body_name' is not null
                           limit 1),
                         (select cu.mng_agncy from public.cpad_units cu
                           where cu.unit_id = bg.cpad_unit_id),
                         (select op.canonical_name from public.operators op
                           where op.jurisdiction_id = bg.c1_jurisdiction_id
                             and op.level='city' limit 1),
                         (select op.canonical_name from public.operators op
                           where op.county_geoid = bg.county_geoid
                             and op.level='county' limit 1)
                       ) as operator,
                       b.dogs_allowed as actual,
                       b.leash_policy, b.off_leash_flag,
                       b.dogs_prohibited_start::text as dogs_prohibited_start,
                       b.dogs_prohibited_end::text   as dogs_prohibited_end,
                       b.dogs_allowed_areas, b.access_rule, b.notes as consumer_notes,
                       b.source as consumer_source,
                       b.consensus_confidence::float as consensus_confidence,
                       b.disagreement_flag,
                       tr.claimed_values->'zone_rules' as zone_rules
                  from public.beaches_gold bg
                  left join public.beach_dog_policy b on b.arena_group_id = bg.fid
                  left join public.beach_enrichment_provenance tr
                    on tr.gold_fid = bg.fid
                   and tr.field_group = 'dogs'
                   and tr.source = 'text_repass_v1'
                 where bg.fid = any(%s)
                 order by bg.name
                """,
                (SAMPLE_FIDS,),
            )
        else:
            cur.execute(
                """
                select g.fid,
                       g.verified_value::text as truth,
                       g.verified_by, g.verified_at::text as verified_at,
                       g.source_url as gold_url, g.notes as gold_notes,
                       bg.name, bg.county_name as county, bg.state,
                       coalesce(
                         (select bep.claimed_values->>'governing_body_name'
                            from public.beach_enrichment_provenance bep
                           where bep.gold_fid = g.fid
                             and bep.field_group = 'governance'
                             and bep.is_canonical
                             and bep.claimed_values->>'governing_body_name' is not null
                           limit 1),
                         (select cu.mng_agncy from public.cpad_units cu
                           where cu.unit_id = bg.cpad_unit_id),
                         (select op.canonical_name from public.operators op
                           where op.jurisdiction_id = bg.c1_jurisdiction_id
                             and op.level='city' limit 1),
                         (select op.canonical_name from public.operators op
                           where op.county_geoid = bg.county_geoid
                             and op.level='county' limit 1)
                       ) as operator,
                       b.dogs_allowed as actual,
                       b.leash_policy, b.off_leash_flag,
                       b.dogs_prohibited_start::text as dogs_prohibited_start,
                       b.dogs_prohibited_end::text   as dogs_prohibited_end,
                       b.dogs_allowed_areas, b.access_rule, b.notes as consumer_notes,
                       b.source as consumer_source,
                       b.consensus_confidence::float as consensus_confidence,
                       b.disagreement_flag,
                       tr.claimed_values->'zone_rules' as zone_rules
                  from public.beach_policy_gold_set g
                  left join public.beaches_gold bg on bg.fid = g.fid
                  left join public.beach_dog_policy b on b.arena_group_id = g.fid
                  left join public.beach_enrichment_provenance tr
                    on tr.gold_fid = g.fid
                   and tr.field_group = 'dogs'
                   and tr.source = 'text_repass_v1'
                 where g.field_name = 'dogs_allowed'
                 order by g.fid
                """
            )
        rows = [dict(r) for r in cur.fetchall()]

        # 2. Evidence per fid
        fids = [r["fid"] for r in rows]
        cur.execute(
            """
            select gold_fid, source, confidence::float as confidence, is_canonical,
                   claimed_values->>'allowed' as allowed,
                   claimed_values->>'leash_required' as leash,
                   claimed_values->>'notes' as notes,
                   claimed_values->>'zone_description' as zone_desc,
                   claimed_values->>'designated_dog_zones' as designated,
                   claimed_values->>'prohibited_areas' as prohibited,
                   source_url
              from public.beach_enrichment_provenance
             where field_group = 'dogs' and gold_fid = any(%s)
             order by gold_fid, is_canonical desc, confidence desc
            """,
            (fids,),
        )
        evidence_by_fid = {}
        for r in cur.fetchall():
            fid = r["gold_fid"]
            evidence_by_fid.setdefault(fid, []).append({
                "source": r["source"],
                "confidence": float(r["confidence"]) if r["confidence"] is not None else 0.5,
                "is_canonical": bool(r["is_canonical"]),
                "allowed": r["allowed"],
                "leash": r["leash"],
                "quote": quote_from_row(r),
                "source_url": r["source_url"],
            })

# 3. Build the AUDIT_DATA payload
beaches = []
for r in rows:
    fid = r["fid"]
    truth = r["truth"]
    actual = r["actual"]
    beach = {
        "fid": fid,
        "name": r["name"] or "(no gold beach)",
        "county": r["county"] or "",
        "state": r["state"] or "",
        "operator": r["operator"],
        "verdict": "sample" if args.sample else verdict(truth, actual),
        "gold": {
            "verified_value": truth,
            "verified_by": r["verified_by"],
            "verified_at": r["verified_at"],
            "source_url": r["gold_url"],
            "notes": r["gold_notes"],
        },
        "consumer": None if actual is None else {
            "dogs_allowed": actual,
            "leash_policy": r["leash_policy"],
            "off_leash_flag": r["off_leash_flag"],
            "dogs_prohibited_start": r["dogs_prohibited_start"],
            "dogs_prohibited_end": r["dogs_prohibited_end"],
            "dogs_allowed_areas": r["dogs_allowed_areas"],
            "access_rule": r["access_rule"],
            "notes": r["consumer_notes"],
            "source": r["consumer_source"],
            "consensus_confidence": r["consensus_confidence"],
            "disagreement_flag": r["disagreement_flag"],
        },
        "zone_rules": r["zone_rules"],
        "evidence": evidence_by_fid.get(fid, []),
    }
    beaches.append(beach)

# Stats
stats = {}
for b in beaches:
    stats[b["verdict"]] = stats.get(b["verdict"], 0) + 1

audit_data = {
    "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
    "beaches": beaches,
    "stats": stats,
}
audit_js = json.dumps(audit_data, indent=2, default=str, ensure_ascii=False)

print(f"Audit beaches: {len(beaches)}")
for v, n in sorted(stats.items(), key=lambda x: -x[1]):
    print(f"  {v:<14} {n}")

path = ROOT / "admin" / "dogs-allowed-audit.html"
html = path.read_text(encoding="utf-8")
html = re.sub(
    r"const AUDIT_DATA = (?:/\* %AUDIT_DATA% \*/|\{[\s\S]*?\n\});",
    lambda _m: f"const AUDIT_DATA = {audit_js};",
    html,
    count=1,
)
path.write_text(html, encoding="utf-8")
print(f"\ninlined audit data ({len(audit_js)} chars) -> {path}")
