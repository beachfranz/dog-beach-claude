"""Close the codify gap for the Moss Beach cluster (San Mateo County).

Three scoreable beaches in unincorporated San Mateo County around the Moss
Beach / Pillar Point area had no operative sand beach_policy_source row:

  fid 6182  Seal Cove Beach   (Fitzgerald Marine Reserve, SMC Parks unit)
  fid 5983  Ross Cove Beach   (Pillar Point Bluff, SMC Parks unit)
  fid 2354  Mavericks Beach   (unincorporated SMC; no park-unit overlay)

Source assignment:
  ps 197 — SMC Ordinance Code §3.68.180 (Dogs) within Chapter 3.68
           (County Park and Recreation Area Rules). Specifically governs
           SMC Parks facilities. Use for Seal Cove + Ross Cove (both inside
           SMC Parks-managed units).
  ps 115 — SMC County Code Title 6 — Animals (general animal regulations).
           Use for Mavericks (unincorporated, not inside an SMC Parks unit).

All three: rule=on_leash for sand and water, no temporal windows
(SMC has no seasonal hour-of-day prohibition; standard CA 6-foot leash).

Note: Fitzgerald Marine Reserve is also a State Marine Conservation Area
(SMCA), but MPA status governs TAKE (fishing/collection), not dogs. SMC
Parks rules govern dog access to the upland park.

Usage:
  python scripts/one_off/close_moss_beach_codify_gap.py --dry-run
  python scripts/one_off/close_moss_beach_codify_gap.py
  python scripts/one_off/close_moss_beach_codify_gap.py --fid 6182
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


PS_SMC_PARKS = 197   # SMC §3.68.180 (Dogs) — Ch. 3.68 County Park & Recreation Area Rules
PS_SMC_TITLE6 = 115  # SMC Code Title 6 (Animal Regulations) — countywide

URL_SMC_PARKS = (
    "https://library.municode.com/ca/san_mateo_county/codes/code_of_ordinances"
    "?nodeId=TIT3PUSAMOWE_CH3.68COPAREARRU_3.68.180DO"
)
URL_SMC_TITLE6 = (
    "https://library.municode.com/ca/san_mateo_county/codes/code_of_ordinances"
    "?nodeId=TIT6AN"
)


VERBATIM_SMC_PARKS_SEAL = (
    "San Mateo County Ordinance Code §3.68.180 (Dogs) within Chapter 3.68 "
    "(County Park and Recreation Area Rules) requires dogs in SMC Parks "
    "facilities to be on a leash not exceeding six feet at all times, and "
    "prohibits dogs in posted no-dogs areas designated by the Parks Director. "
    "Seal Cove Beach is within Fitzgerald Marine Reserve, an SMC Parks "
    "facility, so §3.68.180 applies on the upland sand. The reserve is also "
    "a State Marine Conservation Area, but MPA status governs take (fishing/"
    "collection), not dog access."
)

VERBATIM_SMC_PARKS_ROSS = (
    "San Mateo County Ordinance Code §3.68.180 (Dogs) within Chapter 3.68 "
    "(County Park and Recreation Area Rules) requires dogs in SMC Parks "
    "facilities to be on a leash not exceeding six feet at all times. Ross "
    "Cove Beach is within the Pillar Point Bluff unit operated by SMC Parks, "
    "so §3.68.180 applies."
)

VERBATIM_SMC_TITLE6_MAVERICKS = (
    "San Mateo County Code Title 6 — Animals (countywide animal regulations) "
    "requires dogs to be on a 6-foot leash on public property in unincorporated "
    "San Mateo County. Mavericks Beach sits within unincorporated SMC outside "
    "any incorporated city polygon and without a CPAD park-unit overlay, so "
    "the countywide rule controls access from shore."
)


def assignment(fid: int) -> dict:
    if fid == 6182:
        return {
            "label": "seal_cove_smc_parks",
            "ps_id": PS_SMC_PARKS,
            "url": URL_SMC_PARKS,
            "rule": "on_leash",
            "verbatim": VERBATIM_SMC_PARKS_SEAL,
        }
    if fid == 5983:
        return {
            "label": "ross_cove_smc_parks",
            "ps_id": PS_SMC_PARKS,
            "url": URL_SMC_PARKS,
            "rule": "on_leash",
            "verbatim": VERBATIM_SMC_PARKS_ROSS,
        }
    if fid == 2354:
        return {
            "label": "mavericks_smc_title6",
            "ps_id": PS_SMC_TITLE6,
            "url": URL_SMC_TITLE6,
            "rule": "on_leash",
            "verbatim": VERBATIM_SMC_TITLE6_MAVERICKS,
        }
    raise ValueError(f"No assignment defined for fid {fid}")


MOSS_FIDS = [6182, 5983, 2354]


def fetch_beaches(cur, only_fid: int | None) -> list[tuple[int, str]]:
    fids = [only_fid] if only_fid is not None else MOSS_FIDS
    cur.execute(
        "SELECT fid, COALESCE(display_name_override, name) AS name "
        "  FROM beaches_gold WHERE fid = ANY(%s) AND is_active "
        "  ORDER BY fid",
        (fids,),
    )
    return [(r["fid"], r["name"]) for r in cur.fetchall()]


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
               SET rule = %s,
                   evidence_verbatim = %s,
                   evidence_url = %s,
                   operative_status = 'operative',
                   last_verified = now()
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


def apply_to_beach(cur, fid: int, bucket: dict, log: list) -> dict:
    counts = {"sand": None, "water": None}
    for section in ("sand", "water"):
        bps_id, action = upsert_bps_row(
            cur, fid, section, bucket["rule"], bucket["ps_id"],
            bucket["url"], bucket["verbatim"],
        )
        counts[section] = action
        log.append(f"      {section}: bps_id={bps_id} action={action}")
    # Trigger does Pass 2 now (per 20260531_refresh_beaches_from_ps_pass2.sql),
    # but call explicitly as belt-and-suspenders matching the SD/Goleta pattern.
    cur.execute("SELECT public._promote_zone_rules_for_fid(%s) AS r", (fid,))
    log.append(f"      _promote_zone_rules_for_fid -> {cur.fetchone()['r']}")
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
    print(f"Scope: {len(beaches)} Moss Beach cluster beaches")

    n_ok = n_fail = 0
    agg = {"sand_inserted": 0, "sand_promoted": 0, "sand_updated": 0,
           "water_inserted": 0, "water_promoted": 0, "water_updated": 0}

    for fid, name in beaches:
        bucket = assignment(fid)
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:40]:<40} -> {bucket['label']} (ps_id={bucket['ps_id']})")
            continue
        log: list = []
        try:
            counts = apply_to_beach(cur, fid, bucket, log)
            conn.commit()
            n_ok += 1
            agg[f"sand_{counts['sand']}"] += 1
            agg[f"water_{counts['water']}"] += 1
            print(f"  OK   fid={fid:<6} {name[:40]:<40} -> {bucket['label']} "
                  f"sand={counts['sand']} water={counts['water']}")
            if args.verbose:
                for line in log: print(line)
        except Exception:
            conn.rollback()
            n_fail += 1
            import traceback
            print(f"  FAIL fid={fid:<6} {name[:40]:<40}")
            traceback.print_exc()

    print(f"\nDone. processed={n_ok} failed={n_fail}")
    print(f"Action distribution: {agg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
