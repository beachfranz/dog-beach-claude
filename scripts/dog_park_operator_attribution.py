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
  tmp/dog_park_attribution_<ts>.md     (summary report + audit section)

Role since 2026-10-13: CROSS-CHECK, not the source of truth. The pipeline
(walker + ingester) keys on COALESCE(canonical_operator_id,
inferred_operator_id); canonical_operator_id is the polygon cascade in
_resolve_dog_park_operator. This script's tag-based answer is resolved to
an operators.id with a normalized + fuzzy (pg_trgm) match scoped to the
park's state (it used to need an exact canonical_name match, which left
~83% of parks unresolved), and the report lists every park where the tag
answer and the polygon answer disagree — those go to human review.

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
        "fid": park.get("fid"),
        "osm_dog_park_id": park["id"],
        "name": park.get("name"),
        "state": park.get("state_code"),
        "classification": classification,
        "inferred_operator_name": attribution,
        "confidence": confidence,
        "reason": reason,
        "canonical_operator_id": park.get("canonical_operator_id"),
        **extras,
    }


# Suffixes the decision tree (and raw OSM operator tags) add that canonical
# operator names don't carry: "City of Portland Parks & Recreation" →
# "City of Portland"; "Portland Parks & Recreation" → "Portland".
_OP_SUFFIX_RX = re.compile(
    r"\s+(parks?\s*(&|and)\s*rec(reation)?|parks?\s*(&|and)\s*open\s*space|"
    r"parks?\s*department|parks?\s*dept\.?|parks?)\s*$", re.I)
_FUZZY_MIN_SIMILARITY = 0.7


def _operator_name_candidates(name: str) -> list[str]:
    base = _OP_SUFFIX_RX.sub("", name.strip()).strip()
    cands = [name.strip(), base]
    low = base.lower()
    if not (low.startswith("city of") or low.startswith("town of")
            or "county" in low or "district" in low):
        cands += [f"City of {base}", f"Town of {base}"]
    seen, out = set(), []
    for c in cands:
        if c and c.lower() not in seen:
            seen.add(c.lower()); out.append(c)
    return out


def resolve_operator_ids(cur, classifications) -> None:
    """Set c['inferred_operator_id'] for each classification: exact
    (case-insensitive) match on any normalized candidate name first, then
    the best pg_trgm match >= _FUZZY_MIN_SIMILARITY. Scoped to the park's
    state, canonical + active operators only."""
    cache: dict[tuple, int | None] = {}
    for c in classifications:
        name, state = c.get("inferred_operator_name"), c.get("state")
        c["inferred_operator_id"] = None
        if not name or not state:
            continue
        key = (name.lower(), state)
        if key not in cache:
            cands = [x.lower() for x in _operator_name_candidates(name)]
            cur.execute("""
                SELECT id FROM public.operators
                 WHERE is_canonical AND is_active AND state_code = %s
                   AND lower(canonical_name) = ANY(%s)
                 ORDER BY id LIMIT 1
            """, (state, cands))
            r = cur.fetchone()
            if r is None:
                cur.execute("""
                    SELECT id, max(similarity(lower(canonical_name), cand)) AS sim
                      FROM public.operators, unnest(%s::text[]) AS cand
                     WHERE is_canonical AND is_active AND state_code = %s
                     GROUP BY id
                    HAVING max(similarity(lower(canonical_name), cand)) >= %s
                     ORDER BY sim DESC, id LIMIT 1
                """, (cands, state, _FUZZY_MIN_SIMILARITY))
                r = cur.fetchone()
            cache[key] = r[0] if r else None
        c["inferred_operator_id"] = cache[key]


def main():
    ap = argparse.ArgumentParser(description="Pass A — dog park operator attribution")
    ap.add_argument("--states", default="CA,OR,WA",
                    help="MVP+ default; pass 'ALL' for the full US set")
    ap.add_argument("--write-to-gold", action="store_true",
                    help="UPDATE dog_parks_gold.inferred_operator_id + attribution_method "
                         "+ attribution_confidence in addition to JSONL/MD")
    args = ap.parse_args()

    if args.states.upper() == "ALL":
        states = None
    else:
        states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            # Pull every dog park + spatial county + spatial city polygon.
            # Reads from dog_parks_gold (curated) so we can write back via fid.
            # LATERAL + ORDER BY so a park near two cities / on a county
            # line gets a deterministic answer (was: first row of a
            # multi-match join, arbitrary).
            sql = """
                SELECT
                    g.fid                            AS fid,
                    g.osm_id                         AS id,
                    g.name,
                    g.tags->>'operator'              AS operator,
                    COALESCE(g.tags->>'operator:type', g.tags->>'operator_type') AS operator_type,
                    g.tags,
                    g.state                          AS state_code,
                    g.canonical_operator_id,
                    c.name                           AS county_name,
                    j.name                           AS jurisdiction_name
                FROM public.dog_parks_gold g
                LEFT JOIN LATERAL (
                    SELECT c.name FROM public.counties c
                     WHERE ST_Contains(c.geom, ST_PointOnSurface(g.geom))
                     LIMIT 1
                ) c ON true
                -- 200m DWithin for jurisdictions (TIGER places) per
                -- [[pip-for-places-uses-200m]] — coastal-city polygons
                -- stop at high-water line; waterfront parks just outside.
                LEFT JOIN LATERAL (
                    SELECT j.name FROM public.jurisdictions j
                     WHERE j.place_type LIKE 'C%%'
                       AND ST_DWithin(g.geom, j.geom, 0.0018)
                     ORDER BY ST_Distance(g.geom, j.geom), ST_Area(j.geom)
                     LIMIT 1
                ) j ON true
                WHERE g.is_active = true
            """
            params: tuple = ()
            if states:
                sql += " AND g.state = ANY(%s)"
                params = (states,)
            sql += " ORDER BY g.fid"
            cur.execute(sql, params)
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
    scope_label = "ALL US" if states is None else ",".join(states)
    print(f"Total dog parks ({scope_label}): {len(parks)}")

    classifications = [classify_park(p) for p in parks]

    # Resolve tag-based names → operators.id, then audit against the
    # polygon cascade (canonical_operator_id).
    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            resolve_operator_ids(cur, classifications)
            op_ids = sorted({x for c in classifications
                             for x in (c.get("inferred_operator_id"),
                                       c.get("canonical_operator_id")) if x})
            cur.execute("SELECT id, canonical_name FROM public.operators WHERE id = ANY(%s)",
                        (op_ids,))
            op_names = dict(cur.fetchall())
    finally:
        conn.close()
    disagreements = [c for c in classifications
                     if c.get("inferred_operator_id") and c.get("canonical_operator_id")
                     and c["inferred_operator_id"] != c["canonical_operator_id"]]
    n_resolved = sum(1 for c in classifications if c.get("inferred_operator_id"))
    print(f"Tag attribution resolved to operators.id: {n_resolved}/{len(classifications)}")
    print(f"Disagree with polygon operator (canonical_operator_id): {len(disagreements)}")

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
        f"States: {scope_label}",
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

    md.append(f"\n## Audit: tag operator vs polygon operator ({len(disagreements)} disagree)")
    md.append("")
    md.append(f"Tag answer resolved to an operator for {n_resolved}/{len(classifications)} parks. "
              "Rows below have both answers and they differ — review these "
              "(often a county or district park inside a city).")
    md.append("")
    md.append("| fid | name | state | tag operator | polygon operator |")
    md.append("|---:|---|---|---|---|")
    for c in disagreements:
        md.append(f"| {c['fid']} | {(c.get('name') or '')[:40]} | {c['state']} | "
                  f"{op_names.get(c['inferred_operator_id'], c['inferred_operator_id'])} | "
                  f"{op_names.get(c['canonical_operator_id'], c['canonical_operator_id'])} |")

    md_path = tmp / f"dog_park_attribution_{ts}.md"
    md_path.write_text("\n".join(md), encoding="utf-8")
    print(f"Markdown: {md_path}")

    if args.write_to_gold:
        _write_to_gold(classifications)

    return 0


def _write_to_gold(classifications):
    """UPDATE dog_parks_gold.inferred_operator_id + attribution_method +
    attribution_confidence for every classified park.

    Uses c['inferred_operator_id'] from resolve_operator_ids() (normalized
    + fuzzy, state-scoped). Unresolved names leave inferred_operator_id
    NULL; attribution_method + attribution_confidence still get recorded.
    """
    if not classifications:
        return
    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            updates = 0
            with_op_id = 0
            for c in classifications:
                if c.get("fid") is None:
                    continue
                op_id = c.get("inferred_operator_id")
                if op_id is not None:
                    with_op_id += 1
                cur.execute(
                    """
                    UPDATE public.dog_parks_gold
                       SET inferred_operator_id   = %s,
                           attribution_method     = %s,
                           attribution_confidence = %s
                     WHERE fid = %s
                    """,
                    (op_id, c["classification"], c["confidence"], c["fid"]),
                )
                updates += 1
        conn.commit()
        print(f"\n=== WRITE-TO-GOLD ===")
        print(f"  dog_parks_gold rows updated: {updates}")
        print(f"  resolved to existing operator.id: {with_op_id}")
        print(f"  attribution_method/_confidence only (no FK match): {updates - with_op_id}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
