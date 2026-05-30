"""Clone codified beach_policy_source + _temporal rows to gap beaches.

When a city has some beaches with operative sand bps + temporal rules
already wired and OTHER beaches in the same city geographically lacking
those rows, the gap is almost always just "this beach was added after
the city codify ran." The ordinance applies citywide — copy it.

For each gap beach:
  1. Find the smallest jurisdiction that contains it
  2. Find a sibling beach in the same jurisdiction that already has
     operative sand bps rows
  3. For each (policy_source_id, section, rule) the sibling has,
     INSERT an equivalent bps row for the gap beach (region_name NULL —
     whole-beach scope only)
  4. Copy beach_policy_source_temporal rows tied to those bps rows
  5. Call _promote_zone_rules_for_fid(fid) to regenerate zone_rules

Usage:
  python scripts/one_off/clone_codify_to_gap_beaches.py --dry-run
  python scripts/one_off/clone_codify_to_gap_beaches.py
  python scripts/one_off/clone_codify_to_gap_beaches.py --fid 6190
"""

from __future__ import annotations
import sys
import os
import argparse
import traceback

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
import psycopg2.extras
from psycopg2.extras import Json


DEFAULT_STATES = ("CA", "OR", "WA", "MD", "UT", "VA", "MA", "NH")


def find_gap_beaches(cur, only_fid: int | None, states: tuple[str, ...] = DEFAULT_STATES) -> list[tuple[int, str]]:
    """Active scored beaches with no operative sand bps row in the given states."""
    if only_fid is not None:
        cur.execute(
            "SELECT fid, COALESCE(display_name_override, name) AS name "
            "  FROM beaches_gold WHERE fid = %s AND is_active",
            (only_fid,),
        )
        return [(r["fid"], r["name"]) for r in cur.fetchall()]
    cur.execute("""
        SELECT bg.fid, COALESCE(bg.display_name_override, bg.name) AS name
          FROM beaches_gold bg
         WHERE bg.state = ANY(%s) AND bg.is_active
           AND bg.scoring_tier IN ('daily','hourly')
           AND NOT EXISTS (
             SELECT 1 FROM beach_policy_source bps
              WHERE bps.beach_fid = bg.fid
                AND bps.operative_status = 'operative'
                AND bps.section = 'sand'
           )
         ORDER BY bg.fid;
    """, (list(states),))
    return [(r["fid"], r["name"]) for r in cur.fetchall()]


def smallest_jurisdiction(cur, fid: int) -> str | None:
    cur.execute("""
        SELECT j.name FROM jurisdictions j JOIN beaches_gold bg ON ST_Intersects(j.geom, bg.geom)
         WHERE bg.fid = %s ORDER BY ST_Area(j.geom) ASC LIMIT 1
    """, (fid,))
    r = cur.fetchone()
    return r["name"] if r else None


def nearest_cdpr_unit(cur, fid: int) -> str | None:
    """For offshore/unincorporated beaches, find the nearest CDPR-managed
    CPAD unit within 200m. Returns the unit_name or None."""
    cur.execute("""
        SELECT cu.unit_name
          FROM cpad_units cu
          JOIN beaches_gold bg ON bg.fid = %s
         WHERE ST_DWithin(bg.geom::geography, cu.geom::geography, 200)
           AND (cu.mng_agncy ILIKE '%%Department of Parks%%'
                OR cu.mng_agncy ILIKE '%%CDPR%%'
                OR cu.mng_agncy ILIKE '%%State Parks%%')
         ORDER BY ST_Distance(bg.geom::geography, cu.geom::geography) ASC
         LIMIT 1
    """, (fid,))
    r = cur.fetchone()
    return r["unit_name"] if r else None


def find_template_sibling(cur, jurisdiction: str, target_fid: int) -> int | None:
    """Pick a sibling beach in the same jurisdiction with operative sand bps.

    Prefers the sibling with the most operative bps rows (most complete codify).
    """
    cur.execute("""
        SELECT bps.beach_fid, COUNT(*) AS bps_count
          FROM beach_policy_source bps
          JOIN beaches_gold sibling ON sibling.fid = bps.beach_fid
         WHERE bps.operative_status = 'operative'
           AND bps.beach_fid <> %s
           AND EXISTS (
             SELECT 1 FROM jurisdictions j
              WHERE j.name = %s AND ST_Intersects(j.geom, sibling.geom)
           )
         GROUP BY bps.beach_fid
         ORDER BY bps_count DESC, bps.beach_fid ASC
         LIMIT 1;
    """, (target_fid, jurisdiction))
    r = cur.fetchone()
    return r["beach_fid"] if r else None


def find_template_sibling_cdpr(cur, cdpr_unit: str, target_fid: int) -> int | None:
    """Same as find_template_sibling but matches by CPAD unit name instead
    of jurisdiction. Used for state-park beaches in unincorporated coast."""
    cur.execute("""
        SELECT bps.beach_fid, COUNT(*) AS bps_count
          FROM beach_policy_source bps
          JOIN beaches_gold sibling ON sibling.fid = bps.beach_fid
         WHERE bps.operative_status = 'operative'
           AND bps.beach_fid <> %s
           AND EXISTS (
             SELECT 1 FROM cpad_units cu
              WHERE cu.unit_name = %s
                AND ST_DWithin(sibling.geom::geography, cu.geom::geography, 200)
           )
         GROUP BY bps.beach_fid
         ORDER BY bps_count DESC, bps.beach_fid ASC
         LIMIT 1;
    """, (target_fid, cdpr_unit))
    r = cur.fetchone()
    return r["beach_fid"] if r else None


def clone_codify(cur, template_fid: int, gap_fid: int) -> tuple[int, int]:
    """Copy operative bps + temporal rows from template_fid to gap_fid.

    Returns (n_bps_inserted, n_temporal_inserted).
    """
    cur.execute("""
        SELECT id, policy_source_id, section, rule, rule_modifier,
               evidence_verbatim, evidence_url,
               operative_status, status_basis_id, status_note,
               region_anchor, authority_tier, exemption_type, is_global_note
          FROM beach_policy_source
         WHERE beach_fid = %s
           AND operative_status = 'operative'
           AND region_name IS NULL
         ORDER BY id
    """, (template_fid,))
    template_rows = cur.fetchall()
    if not template_rows:
        return (0, 0)

    n_bps = 0
    n_temporal = 0
    for tr in template_rows:
        # Idempotent: if equivalent row already exists, UPDATE it to operative
        # status with refreshed evidence (some sources land non-operative rows
        # from prior codify runs that need to be flipped on). If not, INSERT.
        cur.execute("""
            SELECT id, operative_status FROM beach_policy_source
             WHERE beach_fid = %s
               AND policy_source_id = %s
               AND section = %s
               AND rule = %s
               AND region_name IS NULL
             LIMIT 1
        """, (gap_fid, tr["policy_source_id"], tr["section"], tr["rule"]))
        existing = cur.fetchone()
        if existing:
            cur.execute("""
                UPDATE beach_policy_source
                   SET operative_status = %s,
                       evidence_verbatim = %s,
                       evidence_url = %s,
                       status_basis_id = %s,
                       status_note = %s,
                       authority_tier = %s,
                       last_verified = now()
                 WHERE id = %s
            """, (
                tr["operative_status"], tr["evidence_verbatim"], tr["evidence_url"],
                tr["status_basis_id"], tr["status_note"], tr["authority_tier"],
                existing["id"],
            ))
            new_bps_id = existing["id"]
        else:
            cur.execute("""
                INSERT INTO beach_policy_source (
                    beach_fid, policy_source_id, section, rule, rule_modifier,
                    evidence_verbatim, evidence_url,
                    operative_status, status_basis_id, status_note,
                    region_anchor, authority_tier, exemption_type, is_global_note,
                    extracted_at, last_verified, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    now(), now(), now()
                ) RETURNING id
            """, (
                gap_fid, tr["policy_source_id"], tr["section"], tr["rule"],
                Json(tr["rule_modifier"]) if tr["rule_modifier"] is not None else None,
                tr["evidence_verbatim"], tr["evidence_url"],
                tr["operative_status"], tr["status_basis_id"], tr["status_note"],
                tr["region_anchor"], tr["authority_tier"], tr["exemption_type"], tr["is_global_note"],
            ))
            new_bps_id = cur.fetchone()["id"]
            n_bps += 1

        # Copy temporal rows tied to the template bps row, retargeted to the
        # gap beach + new bps id.
        cur.execute("""
            SELECT section, exception_rule, window_kind,
                   effective_from_md, effective_to_md,
                   anchor_start, anchor_end,
                   daily_start, daily_end,
                   season_label, extracted_via, extractor_confidence, notes
              FROM beach_policy_source_temporal
             WHERE bps_id = %s
        """, (tr["id"],))
        temporal_template = cur.fetchall()
        if not temporal_template:
            continue
        # Idempotent: clear any pre-existing temporal rows for the cloned bps
        cur.execute(
            "DELETE FROM beach_policy_source_temporal "
            "WHERE bps_id = %s AND section = %s",
            (new_bps_id, tr["section"]),
        )
        for t in temporal_template:
            cur.execute("""
                INSERT INTO beach_policy_source_temporal (
                    bps_id, beach_fid, policy_source_id, section,
                    exception_rule, window_kind,
                    effective_from_md, effective_to_md,
                    anchor_start, anchor_end,
                    daily_start, daily_end,
                    season_label, extracted_via, extractor_confidence, notes,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    now(), now()
                )
            """, (
                new_bps_id, gap_fid, tr["policy_source_id"], t["section"],
                t["exception_rule"], t["window_kind"],
                t["effective_from_md"], t["effective_to_md"],
                t["anchor_start"], t["anchor_end"],
                t["daily_start"], t["daily_end"],
                t["season_label"],
                f"cloned_from_fid_{template_fid}",
                t["extractor_confidence"],
                t["notes"],
            ))
            n_temporal += 1
    return n_bps, n_temporal


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fid", type=int, default=None)
    ap.add_argument("--states", default=",".join(DEFAULT_STATES),
                    help="Comma-separated state codes; default = all MVP+ states.")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    states = tuple(s.strip().upper() for s in args.states.split(",") if s.strip())
    beaches = find_gap_beaches(cur, args.fid, states=states)
    print(f"Gap scope: {len(beaches)} beaches")
    if args.limit:
        beaches = beaches[:args.limit]
        print(f"  limited to first {len(beaches)}")

    n_cloned = 0
    n_unhandled = 0
    n_failed = 0
    unhandled_by_juri: dict[str, list[tuple[int, str]]] = {}

    for fid, name in beaches:
        juri = smallest_jurisdiction(cur, fid)
        template = None
        match_kind = None
        match_label = None
        if juri:
            template = find_template_sibling(cur, juri, fid)
            if template is not None:
                match_kind = "juri"
                match_label = juri
        if template is None:
            # Fall back to CDPR unit match for offshore / unincorporated coast.
            cdpr = nearest_cdpr_unit(cur, fid)
            if cdpr:
                template = find_template_sibling_cdpr(cur, cdpr, fid)
                if template is not None:
                    match_kind = "cdpr"
                    match_label = cdpr
        if template is None:
            label = juri or "(unincorporated/offshore)"
            unhandled_by_juri.setdefault(label, []).append((fid, name))
            n_unhandled += 1
            continue
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:45]:<45}  {match_kind}={match_label[:25]:<25}  template_fid={template}")
            continue
        try:
            n_bps, n_t = clone_codify(cur, template, fid)
            cur.execute("SELECT public._promote_zone_rules_for_fid(%s) AS r", (fid,))
            conn.commit()
            n_cloned += 1
            print(f"  OK   fid={fid:<6} {name[:45]:<45}  {match_kind}={match_label[:25]:<25}  bps+={n_bps} temp+={n_t}")
        except Exception as e:
            conn.rollback()
            n_failed += 1
            print(f"  FAIL fid={fid:<6} {name[:45]:<45}  {match_kind}={match_label[:25] if match_label else None}")
            traceback.print_exc()

    print(f"\nDone. cloned={n_cloned} unhandled={n_unhandled} failed={n_failed}")
    if unhandled_by_juri:
        print("\nUnhandled (need research codify):")
        for juri, lst in sorted(unhandled_by_juri.items(), key=lambda kv: -len(kv[1])):
            print(f"  {juri} ({len(lst)} beaches):")
            for fid, name in lst:
                print(f"    fid {fid:<6} {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
