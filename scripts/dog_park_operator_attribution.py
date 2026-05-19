"""Operate Phase 3 Pass A — dog park operator attribution discovery.

For each MVP+ (CA/OR/WA) dog park, applies a decision tree to determine
the most likely operator + confidence class. Output is a classification
JSONL + a summary report. NO migration written; this is discovery only.

Decision tree (in priority order):
  1. operator tag set         → direct attribution; normalize against existing
                                operator (singular) + operators (plural) tables
  2. operator_type IN (private/community/business/private_non_profit/military)
                              → NON-municipal; defer to per-operator research
  3. operator_type IN (government/public) + addr:city set
                              → high-confidence city attribution
  4. addr:city set alone      → medium-confidence city attribution
  5. name contains "County" or "Regional" pattern
                              → likely county attribution
  6. spatial join → city polygon
                              → low-confidence city attribution (fallback)
  7. spatial join → county polygon only (no city match)
                              → county-managed unincorporated area

Output:
  tmp/dog_park_attribution_<ts>.jsonl  (per-park classification)
  tmp/dog_park_attribution_<ts>.md     (summary report)

Per Franz "audit the tags jsonb + build pass A" 2026-05-18.
"""

from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import argparse
import datetime
import json
import os
import re
import urllib.parse
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")


def _connect_pg():
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def _enc(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError(f"not JSON serializable: {type(o).__name__}")


# OSM operator_type → high-level classification
NON_MUNICIPAL_TYPES = {
    "private", "community", "business", "private_non_profit", "military"
}
MUNICIPAL_TYPES = {
    "government", "public", "public/government", "government_facility-public"
}

COUNTY_PATTERN  = re.compile(r"\bcounty\b", re.I)
REGIONAL_PATTERN = re.compile(r"\bregional\b", re.I)


def classify_park(park: dict) -> dict:
    """Apply the decision tree. Returns classification dict."""
    tags = park.get("tags") or {}
    op_tag = (park.get("operator") or "").strip()
    op_type = (park.get("operator_type") or "").strip().lower()
    name = park.get("name") or tags.get("name") or ""
    addr_city = tags.get("addr:city")
    addr_state = tags.get("addr:state")
    website = tags.get("website") or tags.get("contact:website")

    classification = "unknown"
    attribution = None
    confidence = "low"
    reason = ""

    # Rule 1: explicit operator tag
    if op_tag:
        classification = "direct_operator_tag"
        attribution = op_tag
        confidence = "high"
        reason = f"OSM operator='{op_tag}'"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 2: non-municipal operator_type → defer
    if op_type in NON_MUNICIPAL_TYPES:
        classification = "non_municipal_defer"
        attribution = None
        confidence = "high"
        reason = f"operator_type={op_type!r} indicates non-government entity (HOA / business / nonprofit / military); needs per-operator research"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 3: government + addr:city → high confidence city
    if op_type in MUNICIPAL_TYPES and addr_city:
        classification = "city_high_confidence"
        attribution = f"City of {addr_city} Parks & Recreation"
        confidence = "high"
        reason = f"operator_type={op_type!r} + addr:city={addr_city!r}"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 4: addr:city alone → medium-confidence city
    if addr_city:
        classification = "city_medium_confidence"
        attribution = f"City of {addr_city} Parks & Recreation"
        confidence = "medium"
        reason = f"addr:city={addr_city!r} (no operator_type)"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 5: name contains "County" → county attribution
    if COUNTY_PATTERN.search(name) and park.get("county_name"):
        classification = "county_name_pattern"
        attribution = f"{park['county_name']} County Parks"
        confidence = "medium"
        reason = f"name={name!r} contains 'County' + spatial county={park['county_name']!r}"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 6: spatial city polygon match → low-confidence city
    if park.get("jurisdiction_name"):
        classification = "spatial_city_fallback"
        attribution = f"City of {park['jurisdiction_name']} Parks & Recreation"
        confidence = "low"
        reason = f"spatial city={park['jurisdiction_name']!r}; no OSM operator/addr signals"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Rule 7: county-only (unincorporated)
    if park.get("county_name"):
        classification = "spatial_county_unincorporated"
        attribution = f"{park['county_name']} County Parks"
        confidence = "low"
        reason = f"spatial county={park['county_name']!r}; no city polygon match (unincorporated)"
        return _build(park, classification, attribution, confidence, reason,
                      op_type=op_type, addr_city=addr_city, website=website,
                      county_via_pip=park.get("county_name"),
                      jurisdiction_via_pip=park.get("jurisdiction_name"))

    # Else: unknown
    return _build(park, "unknown", None, "low",
                  "no OSM tags + no spatial match; needs per-park research",
                  op_type=op_type, addr_city=addr_city, website=website,
                  county_via_pip=park.get("county_name"),
                  jurisdiction_via_pip=park.get("jurisdiction_name"))


def _build(park, classification, attribution, confidence, reason, **extras):
    return {
        "osm_dog_park_id": park["id"],
        "name": park.get("name"),
        "state": park.get("state_code"),
        "classification": classification,
        "inferred_operator_name": attribution,
        "confidence": confidence,
        "reason": reason,
        **extras,
    }


def main():
    ap = argparse.ArgumentParser(description="Pass A — dog park operator attribution")
    ap.add_argument("--states", default="CA,OR,WA", help="MVP+ default")
    args = ap.parse_args()

    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    state_fp_map = {"CA": "06", "OR": "41", "WA": "53"}
    state_fps = [state_fp_map[s] for s in states if s in state_fp_map]

    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            # Pull every MVP+ dog park + its spatial county + spatial city polygon
            cur.execute("""
                SELECT
                    dp.id, dp.name, dp.operator, dp.operator_type, dp.tags,
                    CASE c.state_fp WHEN '06' THEN 'CA' WHEN '41' THEN 'OR' WHEN '53' THEN 'WA' END AS state_code,
                    c.name AS county_name,
                    j.name AS jurisdiction_name
                FROM public.osm_dog_parks dp
                JOIN public.counties c ON ST_Intersects(dp.geom, c.geom)
                LEFT JOIN public.jurisdictions j
                  ON j.place_type LIKE 'C%%'
                  AND ST_Intersects(dp.geom, j.geom)
                WHERE c.state_fp = ANY(%s)
                ORDER BY dp.id
            """, (state_fps,))
            cols = [c[0] for c in cur.description]
            parks = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    # Some parks may join with multiple jurisdictions (overlapping); dedupe by id
    seen = {}
    for p in parks:
        if p["id"] not in seen:
            seen[p["id"]] = p
    parks = list(seen.values())
    print(f"Total MVP+ dog parks: {len(parks)}")

    classifications = [classify_park(p) for p in parks]

    # Tally
    tally: dict[str, int] = {}
    conf_tally: dict[str, int] = {}
    for c in classifications:
        tally[c["classification"]] = tally.get(c["classification"], 0) + 1
        conf_tally[c["confidence"]] = conf_tally.get(c["confidence"], 0) + 1

    # Distinct attributions
    distinct_ops = sorted({c["inferred_operator_name"] for c in classifications if c.get("inferred_operator_name")})

    # Output
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp = ROOT / "tmp"
    tmp.mkdir(exist_ok=True, parents=True)

    jsonl_path = tmp / f"dog_park_attribution_{ts}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for c in classifications:
            f.write(json.dumps(c, default=_enc) + "\n")
    print(f"\nJSONL: {jsonl_path}")

    print("\n=== CLASSIFICATIONS ===")
    for k in sorted(tally):
        print(f"  {k:<32} {tally[k]}")
    print("\n=== CONFIDENCE ===")
    for k in sorted(conf_tally):
        print(f"  {k:<8} {conf_tally[k]}")
    print(f"\nDistinct inferred operators: {len(distinct_ops)}")

    # Markdown summary
    md = [
        f"# Dog Park Operator Attribution — Pass A",
        f"",
        f"Generated 2026-05-18 by `scripts/dog_park_operator_attribution.py`.",
        f"States: {','.join(states)}",
        f"Total MVP+ dog parks: {len(parks)}",
        f"Distinct inferred operators: {len(distinct_ops)}",
        f"",
        f"## Classification breakdown",
        f"",
        f"| Classification | Count | Confidence | Meaning |",
        f"|---|---:|---|---|",
    ]
    explain = {
        "direct_operator_tag": ("high", "OSM `operator` tag set directly"),
        "non_municipal_defer": ("high", "OSM `operator_type` flags private/community/business → defer to per-operator research"),
        "city_high_confidence": ("high", "OSM operator_type=government + addr:city set"),
        "city_medium_confidence": ("medium", "OSM addr:city set alone"),
        "county_name_pattern": ("medium", "park `name` matches `\\bCounty\\b` regex + spatial county"),
        "spatial_city_fallback": ("low", "spatial city polygon match (no OSM signals)"),
        "spatial_county_unincorporated": ("low", "spatial county only — unincorporated area"),
        "unknown": ("low", "no signals available"),
    }
    for k in sorted(tally, key=lambda x: -tally[x]):
        conf, mean = explain.get(k, ("?", ""))
        md.append(f"| `{k}` | {tally[k]} | {conf} | {mean} |")

    md.extend([
        f"",
        f"## Distinct inferred operators ({len(distinct_ops)})",
        f"",
    ])
    for op in distinct_ops:
        n = sum(1 for c in classifications if c["inferred_operator_name"] == op)
        md.append(f"- `{op}` — {n} park(s)")

    # Non-municipal defer entries (for per-operator follow-up)
    nm = [c for c in classifications if c["classification"] == "non_municipal_defer"]
    if nm:
        md.append(f"\n## Non-municipal defer ({len(nm)})")
        md.append("")
        md.append("| osm_id | name | state | reason |")
        md.append("|---:|---|---|---|")
        for c in nm[:50]:
            md.append(f"| {c['osm_dog_park_id']} | {(c.get('name') or '')[:40]} | {c['state']} | {c['reason'][:80]} |")
        if len(nm) > 50:
            md.append(f"\n*(... {len(nm)-50} more)*")

    md_path = tmp / f"dog_park_attribution_{ts}.md"
    md_path.write_text("\n".join(md), encoding="utf-8")
    print(f"Markdown: {md_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
