"""Close codify gap for 8 California State Beach / State Park beaches.

Eight scoreable beaches inside CDPR-managed CPAD units lacked an operative
default-region sand beach_policy_source row. Each fid maps to an existing
per-park policy_source row (ps 58/60/64/70/220), so this script just writes
BPS rows with the correct per-park rule + verbatim.

Five other gap beaches (Emma Wood SB, Gray Whale Cove SB, New Brighton SB,
San Gregorio SB, North Beach @ Marshall Gold Discovery SHP) need NEW
per-park policy_source rows created first and are deferred.

Rule flips worth noting (not just citation upgrades):
  fid 8567 Marina State Beach (Fort Ord Dunes SP)
    on_leash -> not_allowed. CDPR explicitly prohibits dogs on the beach
    inside Fort Ord Dunes; existing zone_rules was inferred wrong.
  fid 8445 South Beach (Leo Carrillo SP)
    on_leash -> not_allowed. CDPR explicitly prohibits dogs south of
    Lifeguard Tower 3 at Leo Carrillo; the child South Beach fid
    inherits the parent's region carve-out.

Usage:
  python scripts/one_off/close_cdpr_park_codify_gap.py --dry-run
  python scripts/one_off/close_cdpr_park_codify_gap.py
  python scripts/one_off/close_cdpr_park_codify_gap.py --fid 8567
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


# ─── Per-park policy_source rows (already exist) ─────────────────────
PS_CARMEL_RIVER  = 58
PS_CAYUCOS       = 60
PS_FORT_ORD_DUNES = 64
PS_LEO_CARRILLO  = 70
PS_MT_TAMALPAIS  = 220

URL_PARKS_CA_GOV = "https://www.parks.ca.gov"

URL_CARMEL_RIVER  = f"{URL_PARKS_CA_GOV}/?page_id=567"
URL_CAYUCOS       = f"{URL_PARKS_CA_GOV}/?page_id=596"
URL_FORT_ORD      = f"{URL_PARKS_CA_GOV}/?page_id=580"
URL_LEO_CARRILLO  = f"{URL_PARKS_CA_GOV}/?page_id=616"
URL_MT_TAMALPAIS  = f"{URL_PARKS_CA_GOV}/?page_id=471"


# ─── Per-beach verbatim text ─────────────────────────────────────────

VERBATIM_MONASTERY = (
    "Monastery Beach is part of Carmel River State Beach. Per CDPR's "
    "per-park dog policy: dogs must be on a maximum 6-foot leash at ALL "
    "times and physically under your control. Standard CA DPR leash "
    "requirement applies."
)

VERBATIM_CAYUCOS = (
    "Cayucos State Beach per CDPR's per-park dog policy: dogs are allowed "
    "only on leash in permitted areas (standard CA DPR 6-foot leash "
    "requirement). Unless posted signs say otherwise."
)

VERBATIM_MARINA_NOT_ALLOWED = (
    "Marina State Beach sits inside Fort Ord Dunes State Park. Per CDPR's "
    "per-park dog policy for Fort Ord Dunes: 'Dogs not allowed on spur "
    "trails from there to beach or on the beach.' Dogs are PROHIBITED on "
    "the beach itself; the upland recreation trail through the dunes is "
    "the only dog-permitted area."
)

VERBATIM_LEO_CARRILLO_DEFAULT = (
    "Leo Carrillo State Park / State Beach: dogs on a leash are allowed in "
    "the Park's day-use areas, campground, and North Beach (north of "
    "Lifeguard Tower 3) on a standard 6-foot CDPR leash. Dogs are NOT "
    "allowed on backcountry trails or South Beach (south of Lifeguard "
    "Tower 3). Default region defaults to on_leash because the park has "
    "dog access; the South Beach exclusion is captured at the named "
    "region 'South Beach'."
)

VERBATIM_LEO_NORTH = (
    "North Beach of Leo Carrillo State Park is the dog-permitted section "
    "(north of Lifeguard Tower 3) per CDPR's per-park dog policy. Dogs on "
    "a 6-foot leash are allowed."
)

VERBATIM_LEO_SOUTH_NOT_ALLOWED = (
    "South Beach of Leo Carrillo State Park (south of Lifeguard Tower 3) "
    "is the dog-prohibited section per CDPR's per-park dog policy: 'Dogs "
    "are NOT allowed on backcountry trails or South Beach.'"
)

VERBATIM_MT_TAM_BACKCOUNTRY = (
    "Mount Tamalpais State Park dog policy: 'Dogs allowed only in "
    "developed areas. Dogs not allowed in the following areas: trails, "
    "dirt roads, and backcountry areas.' This beach is accessed via "
    "backcountry trail and is not a developed area; dogs are accordingly "
    "prohibited."
)


# ─── Per-fid assignment ───────────────────────────────────────────────

ASSIGNMENTS: dict[int, dict] = {
    8569: {  # Monastery Beach
        "label": "monastery_carmel_river",
        "ps_id": PS_CARMEL_RIVER, "url": URL_CARMEL_RIVER,
        "rule": "on_leash", "verbatim": VERBATIM_MONASTERY,
    },
    8599: {  # Cayucos State Beach
        "label": "cayucos_sb",
        "ps_id": PS_CAYUCOS, "url": URL_CAYUCOS,
        "rule": "on_leash", "verbatim": VERBATIM_CAYUCOS,
    },
    8567: {  # Marina State Beach (Fort Ord Dunes)
        "label": "marina_fort_ord_no_dogs",
        "ps_id": PS_FORT_ORD_DUNES, "url": URL_FORT_ORD,
        "rule": "not_allowed", "verbatim": VERBATIM_MARINA_NOT_ALLOWED,
    },
    3671: {  # Leo Carrillo State Beach (parent unit)
        "label": "leo_carrillo_parent",
        "ps_id": PS_LEO_CARRILLO, "url": URL_LEO_CARRILLO,
        "rule": "on_leash", "verbatim": VERBATIM_LEO_CARRILLO_DEFAULT,
    },
    8244: {  # North Beach (Leo Carrillo child) - the dog-permitted area
        "label": "leo_carrillo_north",
        "ps_id": PS_LEO_CARRILLO, "url": URL_LEO_CARRILLO,
        "rule": "on_leash", "verbatim": VERBATIM_LEO_NORTH,
    },
    8445: {  # South Beach (Leo Carrillo child) - the dog-prohibited area
        "label": "leo_carrillo_south_no_dogs",
        "ps_id": PS_LEO_CARRILLO, "url": URL_LEO_CARRILLO,
        "rule": "not_allowed", "verbatim": VERBATIM_LEO_SOUTH_NOT_ALLOWED,
    },
    454: {   # Mickey's Beach (Mt Tam backcountry)
        "label": "mickeys_mt_tam_no_dogs",
        "ps_id": PS_MT_TAMALPAIS, "url": URL_MT_TAMALPAIS,
        "rule": "not_allowed", "verbatim": VERBATIM_MT_TAM_BACKCOUNTRY,
    },
    6116: {  # Steep Ravine Beach (Mt Tam backcountry)
        "label": "steep_ravine_mt_tam_no_dogs",
        "ps_id": PS_MT_TAMALPAIS, "url": URL_MT_TAMALPAIS,
        "rule": "not_allowed", "verbatim": VERBATIM_MT_TAM_BACKCOUNTRY,
    },
}


def fetch_beaches(cur, only_fid: int | None) -> list[tuple[int, str]]:
    fids = [only_fid] if only_fid is not None else list(ASSIGNMENTS.keys())
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
        SELECT id, operative_status, rule
          FROM beach_policy_source
         WHERE beach_fid = %s AND policy_source_id = %s
           AND section = %s AND region_name IS NULL
         LIMIT 1
    """, (fid, ps_id, section))
    existing = cur.fetchone()
    if existing:
        was_demoted = existing["operative_status"] != "operative"
        rule_changed = existing["rule"] != rule
        cur.execute("""
            UPDATE beach_policy_source
               SET rule = %s,
                   evidence_verbatim = %s,
                   evidence_url = %s,
                   operative_status = 'operative',
                   last_verified = now()
             WHERE id = %s
        """, (rule, verbatim, url, existing["id"]))
        if was_demoted:
            return existing["id"], "promoted"
        if rule_changed:
            return existing["id"], "rule_flipped"
        return existing["id"], "updated"
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
    print(f"Scope: {len(beaches)} CDPR-park beaches")

    n_ok = n_fail = 0
    agg: dict[str, int] = {}

    for fid, name in beaches:
        bucket = ASSIGNMENTS[fid]
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:32]:<32} -> {bucket['label']:<32} rule={bucket['rule']}")
            continue
        log: list = []
        try:
            counts = apply_to_beach(cur, fid, bucket, log)
            conn.commit()
            n_ok += 1
            for sec, act in counts.items():
                agg[f"{sec}_{act}"] = agg.get(f"{sec}_{act}", 0) + 1
            print(f"  OK   fid={fid:<6} {name[:32]:<32} -> {bucket['label']:<32} sand={counts['sand']} water={counts['water']}")
            if args.verbose:
                for line in log: print(line)
        except Exception:
            conn.rollback()
            n_fail += 1
            import traceback
            print(f"  FAIL fid={fid:<6} {name[:32]:<32}")
            traceback.print_exc()

    print(f"\nDone. processed={n_ok} failed={n_fail}")
    print(f"Action distribution: {agg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
