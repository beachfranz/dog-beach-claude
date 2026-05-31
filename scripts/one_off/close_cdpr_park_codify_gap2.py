"""Close codify gap for 5 CDPR park beaches that need NEW per-park ps rows.

Companion to close_cdpr_park_codify_gap.py — that script handled 8 beaches
where an existing per-park policy_source row already existed. This script
handles the remaining 5 by creating new policy_source rows first, then
writing BPS rows.

Verbatim rules pulled from parks.ca.gov per-park pages on 2026-05-31.

  fid 8479 Emma Wood SB           page_id=604  not_allowed (dogs not on beach)
  fid 3349 Gray Whale Cove SB     page_id=528  not_allowed (dogs not on beach)
  fid 5961 New Brighton SB        page_id=542  on_leash    (Yes)
  fid 8579 San Gregorio SB        page_id=529  not_allowed (prohibited at all times)
  fid 6223 North Beach @ Marshall page_id=484  on_leash    (river beach, leashed)
            Gold Discovery SHP

Three of five are major consumer-surface flips (Emma Wood, Gray Whale Cove,
San Gregorio) — beaches that were inferred as dog-friendly but are
actually prohibited per CDPR. Expect scoring_tier auto-demotion on those
(tg_after_change_refresh_scoring_tier flips daily -> none when
dogs_allowed=no).

Usage:
  python scripts/one_off/close_cdpr_park_codify_gap2.py --dry-run
  python scripts/one_off/close_cdpr_park_codify_gap2.py
  python scripts/one_off/close_cdpr_park_codify_gap2.py --fid 8479
"""

from __future__ import annotations
import sys
import os
import argparse

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.common.db import connect
import psycopg2.extras


CDPR_AGENCY_ID = 234


PARK_DEFS = {
    # ───────────────────────────── Emma Wood SB ─────────────────────────
    8479: {
        "page_id": 604,
        "park_name": "Emma Wood State Beach",
        "citation": "California State Parks dog policy — Emma Wood State Beach",
        "full_text": (
            'CA DPR — Emma Wood State Beach dog policy (verbatim from '
            'parks.ca.gov/?page_id=604 "Are dogs Allowed?" section): '
            '"Yes: Dogs allowed in campground and day-use area. Dogs not '
            'allowed on beach." Camping section adds: "Leashed dogs are '
            'permitted in the campground only, and are prohibited on the '
            'beach." [Refreshed 2026-05-31.]'
        ),
        "sand_rule": "not_allowed",
        "water_rule": "not_allowed",
        "verbatim_beach": (
            "Emma Wood State Beach per CDPR's per-park dog policy: dogs "
            "are PROHIBITED on the beach. Leashed dogs are permitted in "
            "the campground and day-use area only."
        ),
    },
    # ───────────────────────────── Gray Whale Cove SB ────────────────────
    3349: {
        "page_id": 528,
        "park_name": "Gray Whale Cove State Beach",
        "citation": "California State Parks dog policy — Gray Whale Cove State Beach",
        "full_text": (
            'CA DPR — Gray Whale Cove State Beach dog policy (verbatim '
            'from parks.ca.gov/?page_id=528 "Are dogs Allowed?" section): '
            '"No." Basic Park Information adds: "DOGS are not allowed on '
            'the beach. Dogs are allowed on the trails east of Highway 1 '
            'and they must be confined to a leash no longer than six feet '
            'at all times." [Refreshed 2026-05-31.]'
        ),
        "sand_rule": "not_allowed",
        "water_rule": "not_allowed",
        "verbatim_beach": (
            "Gray Whale Cove State Beach per CDPR's per-park dog policy: "
            "dogs are PROHIBITED on the beach. Dogs are permitted on "
            "trails east of Highway 1 with a maximum 6-foot leash."
        ),
    },
    # ───────────────────────────── New Brighton SB ───────────────────────
    5961: {
        "page_id": 542,
        "park_name": "New Brighton State Beach",
        "citation": "California State Parks dog policy — New Brighton State Beach",
        "full_text": (
            'CA DPR — New Brighton State Beach dog policy (verbatim from '
            'parks.ca.gov/?page_id=542 "Are dogs Allowed?" section): '
            '"Yes." Detail: "DOGS must always be on a leash no longer '
            'than six feet and attended by humans." [Refreshed 2026-05-31.]'
        ),
        "sand_rule": "on_leash",
        "water_rule": "on_leash",
        "verbatim_beach": (
            "New Brighton State Beach per CDPR's per-park dog policy: "
            "dogs allowed on a leash no longer than six feet and "
            "attended by humans at all times."
        ),
    },
    # ───────────────────────────── San Gregorio SB ───────────────────────
    8579: {
        "page_id": 529,
        "park_name": "San Gregorio State Beach",
        "citation": "California State Parks dog policy — San Gregorio State Beach",
        "full_text": (
            'CA DPR — San Gregorio State Beach dog policy (verbatim from '
            'parks.ca.gov/?page_id=529 "Are dogs Allowed?" section): '
            '"No." Basic Park Information states: "DOGS are prohibited '
            'on the beach at all times." [Refreshed 2026-05-31.]'
        ),
        "sand_rule": "not_allowed",
        "water_rule": "not_allowed",
        "verbatim_beach": (
            "San Gregorio State Beach per CDPR's per-park dog policy: "
            "dogs are PROHIBITED on the beach at all times."
        ),
    },
    # ───────────────────── North Beach @ Marshall Gold Discovery SHP ─────
    6223: {
        "page_id": 484,
        "park_name": "Marshall Gold Discovery State Historic Park",
        "citation": "California State Parks dog policy — Marshall Gold Discovery State Historic Park",
        "full_text": (
            'CA DPR — Marshall Gold Discovery State Historic Park dog '
            'policy (verbatim from parks.ca.gov/?page_id=484 "Are dogs '
            'Allowed?" section): "Yes: Dogs on leash allowed on trails '
            'and in the river. Dogs not allowed in buildings." '
            '[Refreshed 2026-05-31.]'
        ),
        "sand_rule": "on_leash",
        "water_rule": "on_leash",
        "verbatim_beach": (
            "North Beach at Marshall Gold Discovery State Historic Park "
            "(South Fork American River) per CDPR's per-park dog policy: "
            "dogs on leash allowed on trails and in the river. "
            "Standard CA DPR 6-foot leash applies."
        ),
    },
}


def ensure_policy_source(cur, page_id: int, park_name: str,
                         citation: str, full_text: str) -> tuple[int, str]:
    """Find or create the per-park CDPR ps row. Returns (ps_id, 'found'|'inserted')."""
    source_url = f"https://www.parks.ca.gov/?page_id={page_id}"
    cur.execute("""
        SELECT id FROM policy_source
         WHERE source_url = %s OR citation = %s
         LIMIT 1
    """, (source_url, citation))
    existing = cur.fetchone()
    if existing:
        return existing["id"], "found"
    cur.execute("""
        INSERT INTO policy_source (
            subtype, citation, source_url, full_text,
            issuing_agency_id, scope, last_verified, created_at
        ) VALUES (
            'agency_administrative_policy', %s, %s, %s,
            %s, ARRAY['dog_policy']::text[], now(), now()
        ) RETURNING id
    """, (citation, source_url, full_text, CDPR_AGENCY_ID))
    return cur.fetchone()["id"], "inserted"


def upsert_bps_row(cur, fid: int, section: str, rule: str,
                   ps_id: int, url: str, verbatim: str) -> tuple[int, str]:
    cur.execute("""
        SELECT id, operative_status
          FROM beach_policy_source
         WHERE beach_fid = %s AND policy_source_id = %s
           AND section = %s AND region_name IS NULL
         LIMIT 1
    """, (fid, ps_id, section))
    existing = cur.fetchone()
    if existing:
        was_demoted = existing["operative_status"] != "operative"
        cur.execute("""
            UPDATE beach_policy_source
               SET rule = %s, evidence_verbatim = %s, evidence_url = %s,
                   operative_status = 'operative', last_verified = now()
             WHERE id = %s
        """, (rule, verbatim, url, existing["id"]))
        return existing["id"], ("promoted" if was_demoted else "updated")
    cur.execute("""
        INSERT INTO beach_policy_source (
            beach_fid, policy_source_id, section, rule,
            evidence_verbatim, evidence_url,
            operative_status, extracted_at, last_verified, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            'operative', now(), now(), now()
        ) RETURNING id
    """, (fid, ps_id, section, rule, verbatim, url))
    return cur.fetchone()["id"], "inserted"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fid", type=int, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    fids = [args.fid] if args.fid is not None else list(PARK_DEFS.keys())
    cur.execute(
        "SELECT fid, COALESCE(display_name_override, name) AS name "
        "  FROM beaches_gold WHERE fid = ANY(%s) AND is_active "
        "  ORDER BY fid",
        (fids,),
    )
    beaches = [(r["fid"], r["name"]) for r in cur.fetchall()]
    print(f"Scope: {len(beaches)} CDPR-park beaches needing new ps rows")

    n_ok = n_fail = 0
    ps_actions = {"found": 0, "inserted": 0}

    for fid, name in beaches:
        d = PARK_DEFS[fid]
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:30]:<30} -> {d['park_name'][:40]:<40} sand={d['sand_rule']} water={d['water_rule']}")
            continue
        log: list = []
        try:
            ps_id, ps_action = ensure_policy_source(
                cur, d["page_id"], d["park_name"], d["citation"], d["full_text"],
            )
            ps_actions[ps_action] += 1
            log.append(f"      ps_id={ps_id} ({ps_action})")
            url = f"https://www.parks.ca.gov/?page_id={d['page_id']}"
            for section, rule in [("sand", d["sand_rule"]), ("water", d["water_rule"])]:
                bps_id, act = upsert_bps_row(
                    cur, fid, section, rule, ps_id, url, d["verbatim_beach"],
                )
                log.append(f"      {section}: bps_id={bps_id} action={act} rule={rule}")
            cur.execute("SELECT public._promote_zone_rules_for_fid(%s) AS r", (fid,))
            log.append(f"      _promote_zone_rules_for_fid -> {cur.fetchone()['r']}")
            conn.commit()
            n_ok += 1
            print(f"  OK   fid={fid:<6} {name[:30]:<30} -> ps_id={ps_id} ({ps_action}) sand={d['sand_rule']} water={d['water_rule']}")
            if args.verbose:
                for line in log: print(line)
        except Exception:
            conn.rollback()
            n_fail += 1
            import traceback
            print(f"  FAIL fid={fid:<6} {name[:30]:<30}")
            traceback.print_exc()

    print(f"\nDone. processed={n_ok} failed={n_fail}")
    print(f"PS row actions: {ps_actions}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
