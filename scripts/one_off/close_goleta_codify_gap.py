"""Close the codify gap for City of Goleta beaches.

Three beaches in Goleta jurisdiction (Tecolote Canyon, Ellwood, Haskell's) lacked
operative beach_policy_source rows for the city ordinance that governs them.

Backstory:
  * fid 2243 (Tecolote Canyon) — never codified.
  * fid 8946 (Ellwood), fid 8947 (Haskell's) — had bps rows for ps_id 192 (Goleta
    Municipal Code §6.01.020), but the 2026-05-30 codify-city-preemption demote
    migration (20260530_demote_county_code_in_city_beaches.sql) caught them by
    regex on the parenthetical "(County Ordinance Adopted — adopts Santa Barbara
    County Animals & Fowl Code Chapter 7 by reference, per Ord. 19-05)" in the
    citation. The rule is actually city-issued (issuing_agency_id=51 = Goleta),
    so it preempts at the city level even though it adopts county Chapter 7.

Source chain:
  Goleta MC §6.01.020 → SB County Code Chapter 7 (Animals & Fowl) → on_leash.

Goleta does not impose seasonal hour-of-day prohibitions on beach dogs the way
San Diego does, so this script writes BPS rows only — no beach_policy_source_temporal
rows.

Usage:
  python scripts/one_off/close_goleta_codify_gap.py --dry-run
  python scripts/one_off/close_goleta_codify_gap.py
  python scripts/one_off/close_goleta_codify_gap.py --fid 2243   (single fid test)
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


PS_GOLETA = 192
URL_GOLETA = "https://qcode.us/codes/goleta/view.php?topic=6-01-020"

GOLETA_FIDS = [2243, 8946, 8947]

VERBATIM_SAND = (
    "Goleta Municipal Code §6.01.020 adopts Santa Barbara County Code Chapter 7 "
    "(Animals & Fowl) by reference. The adopted SB County Code §26-49 requires "
    "dogs to be on a leash not exceeding eight feet on public property; "
    "§26-49.1 lists designated off-leash areas (none of which lie within the "
    "City of Goleta). Beaches within Goleta are accordingly subject to the "
    "on-leash default at all times."
)

VERBATIM_WATER = VERBATIM_SAND


def fetch_beaches(cur, only_fid: int | None) -> list[tuple[int, str]]:
    fids = [only_fid] if only_fid is not None else GOLETA_FIDS
    cur.execute(
        "SELECT fid, COALESCE(display_name_override, name) AS name "
        "  FROM beaches_gold WHERE fid = ANY(%s) AND is_active "
        "  ORDER BY fid",
        (fids,),
    )
    return [(r["fid"], r["name"]) for r in cur.fetchall()]


def upsert_bps_row(cur, fid: int, section: str, rule: str,
                   evidence_verbatim: str) -> tuple[int, str]:
    """INSERT or UPDATE one beach_policy_source row.

    Returns (bps_id, action) where action is one of:
      - 'inserted'   — new row created
      - 'promoted'   — existing inapplicable_jurisdiction row flipped to operative
      - 'updated'    — operative row content updated
    """
    cur.execute("""
        SELECT id, operative_status, rule, evidence_verbatim
          FROM beach_policy_source
         WHERE beach_fid = %s AND policy_source_id = %s
           AND section = %s AND region_name IS NULL
         LIMIT 1
    """, (fid, PS_GOLETA, section))
    existing = cur.fetchone()
    if existing:
        was_demoted = existing["operative_status"] == "inapplicable_jurisdiction"
        cur.execute("""
            UPDATE beach_policy_source
               SET rule = %s,
                   evidence_verbatim = %s,
                   evidence_url = %s,
                   operative_status = 'operative',
                   last_verified = now(),
                   status_note = CASE
                     WHEN status_note IS NULL THEN
                       '2026-05-31 re-promoted: prior codify-city-preemption demote was a false positive; issuing agency is City of Goleta, not SB County. See [[codify-city-preemption-adoption-by-ref]] pin.'
                     ELSE status_note || ' | 2026-05-31 re-promoted: false-positive demote (city ord cites adopted county code).'
                   END
             WHERE id = %s
        """, (rule, evidence_verbatim, URL_GOLETA, existing["id"]))
        return existing["id"], ("promoted" if was_demoted else "updated")
    cur.execute("""
        INSERT INTO beach_policy_source (
            beach_fid, policy_source_id, section, rule,
            evidence_verbatim, evidence_url,
            operative_status, extracted_at, last_verified, created_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            'operative', now(), now(), now()
        )
        RETURNING id
    """, (fid, PS_GOLETA, section, rule, evidence_verbatim, URL_GOLETA))
    return cur.fetchone()["id"], "inserted"


def re_promote_existing_global_rows(cur, fid: int) -> int:
    """Re-promote the section=global metadata rows for ps 192 that were also
    demoted by the regex (adoption_by_reference / conflict_resolution /
    partial_exclusion). They're legit city-code structural rows."""
    cur.execute("""
        UPDATE beach_policy_source
           SET operative_status = 'operative',
               last_verified = now(),
               status_note = COALESCE(status_note, '') || ' | 2026-05-31 re-promoted: false-positive demote (city ord cites adopted county code).'
         WHERE beach_fid = %s
           AND policy_source_id = %s
           AND section = 'global'
           AND operative_status = 'inapplicable_jurisdiction'
        RETURNING id
    """, (fid, PS_GOLETA))
    return len(cur.fetchall())


def apply_to_beach(cur, fid: int, log: list) -> dict:
    counts = {"sand": None, "water": None, "global_repromoted": 0}
    for section, verbatim in [("sand", VERBATIM_SAND), ("water", VERBATIM_WATER)]:
        bps_id, action = upsert_bps_row(cur, fid, section, "on_leash", verbatim)
        counts[section] = action
        log.append(f"      {section}: bps_id={bps_id} action={action}")
    counts["global_repromoted"] = re_promote_existing_global_rows(cur, fid)
    log.append(f"      global rows re-promoted: {counts['global_repromoted']}")
    cur.execute("SELECT public._promote_zone_rules_for_fid(%s) AS r", (fid,))
    log.append(f"      _promote_zone_rules_for_fid → {cur.fetchone()['r']}")
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fid", type=int, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    beaches = fetch_beaches(cur, args.fid)
    print(f"Scope: {len(beaches)} Goleta beaches")

    n_processed = 0
    n_failed = 0
    agg = {"sand_inserted": 0, "sand_promoted": 0, "sand_updated": 0,
           "water_inserted": 0, "water_promoted": 0, "water_updated": 0,
           "global_repromoted": 0}

    for fid, name in beaches:
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:50]:<50}")
            continue
        log: list = []
        try:
            counts = apply_to_beach(cur, fid, log)
            conn.commit()
            n_processed += 1
            agg[f"sand_{counts['sand']}"] += 1
            agg[f"water_{counts['water']}"] += 1
            agg["global_repromoted"] += counts["global_repromoted"]
            print(f"  OK   fid={fid:<6} {name[:50]:<50} sand={counts['sand']} water={counts['water']} global_repromoted={counts['global_repromoted']}")
            if args.verbose:
                for line in log: print(line)
        except Exception:
            conn.rollback()
            n_failed += 1
            import traceback
            print(f"  FAIL fid={fid:<6} {name[:50]:<50}")
            traceback.print_exc()

    print(f"\nDone. processed={n_processed} failed={n_failed}")
    print(f"Action distribution: {agg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
