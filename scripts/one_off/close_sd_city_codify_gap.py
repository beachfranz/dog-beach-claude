"""Close the codify gap for San Diego City beaches.

Writes structured rules into beach_policy_source + beach_policy_source_temporal
(the tables that the zone_rules cascade actually reads, per _zr_inject_from_policy_sources).
After insert, calls _promote_zone_rules_for_fid(fid) to regenerate zone_rules.

Sources:
  ps_id 156 — sandiego.gov/lifeguards/safety/bchreg (verbatim default rule)
  ps_id 338 — SD Muni Code §63.0102 (Animals on Public Property) PDF

Rule buckets:
  default_mb_rule  — Mission Bay area default (most beaches): sand rule=on_leash,
                     two seasonal time_windows (Apr 1–Oct 31 prohibit 9am–6pm,
                     Nov 1–Mar 31 prohibit 9am–4pm).
  dog_beach        — Ocean Beach Dog Beach: sand rule=off_leash_voice_control,
                     no temporal restriction (24/7 off-leash).
  fiesta_island    — Fiesta Island: sand rule=off_leash_voice_control,
                     daily 04:00–22:00.
  closure_area     — Boomer Beach + La Jolla Cove: §63.0102(g)(1) closure,
                     sand rule=not_allowed year-round.

Usage:
  python scripts/one_off/close_sd_city_codify_gap.py --dry-run
  python scripts/one_off/close_sd_city_codify_gap.py
  python scripts/one_off/close_sd_city_codify_gap.py --limit 1   (single-beach test)
  python scripts/one_off/close_sd_city_codify_gap.py --fid 8973  (single specific fid)
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


PS_BCHDOG = 156    # sandiego.gov park-and-recreation dogs/bchdog
PS_MUNI = 338      # SD Muni Code §63.0102 PDF

URL_BCHREG = "https://www.sandiego.gov/lifeguards/safety/bchreg"
URL_BCHDOG = "https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog"
URL_MUNI = "https://docs.sandiego.gov/municode/MuniCodeChapter06/Ch06Art03Division01.pdf"


# ─── Rule definitions ────────────────────────────────────────────────

DEFAULT_VERBATIM_SAND = (
    "From Nov. 1 through March 31, dogs are NOT allowed on the beach, "
    "boardwalk or adjacent parks between 9 a.m. and 4 p.m. From April 1 "
    "through Oct. 31, dogs are NOT allowed on the beach, boardwalk or "
    "adjacent parks between 9 a.m. and 6 p.m. At all other hours, dogs "
    "must be on a leash. Owners are expected to pick up after their dog."
)

DEFAULT_WATER_VERBATIM = DEFAULT_VERBATIM_SAND  # same rule covers water

DEFAULT_TEMPORAL = [
    {
        "exception_rule": "not_allowed",
        "window_kind": "seasonal_and_daily",
        "effective_from_md": "04-01",
        "effective_to_md": "10-31",
        "daily_start": "09:00:00",
        "daily_end": "18:00:00",
        "season_label": (
            "Summer (Apr 1 – Oct 31): dogs prohibited 9am–6pm; "
            "permitted on leash before 9am and after 6pm"
        ),
    },
    {
        "exception_rule": "not_allowed",
        "window_kind": "seasonal_and_daily",
        "effective_from_md": "11-01",
        "effective_to_md": "03-31",
        "daily_start": "09:00:00",
        "daily_end": "16:00:00",
        "season_label": (
            "Winter (Nov 1 – Mar 31): dogs prohibited 9am–4pm; "
            "permitted on leash before 9am and after 4pm"
        ),
    },
]

DOG_BEACH_VERBATIM = (
    "Dog Beach (adjacent to Ocean Beach): dogs allowed. Leash-free exercise "
    "area for dogs at the west end of the San Diego River Floodway."
)

FIESTA_VERBATIM = (
    "Fiesta Island (Mission Bay Park): leash-optional area, including all "
    "shoreline. Open daily 4:00 a.m. to 10:00 p.m. Seasonal California Least "
    "Tern nesting closures apply April 15 – September 15 at designated areas."
)
FIESTA_TEMPORAL = [
    {
        "exception_rule": "off_leash_voice_control",
        "window_kind": "daily",
        "effective_from_md": None,
        "effective_to_md": None,
        "daily_start": "04:00:00",
        "daily_end": "22:00:00",
        "season_label": "Daily 4 a.m. – 10 p.m. (leash-optional)",
    },
]

CLOSURE_VERBATIM = (
    "It is unlawful for any dog at any time to be on the closure area of "
    "Point La Jolla and Boomer Beach, as described in San Diego Municipal "
    "Code section 63.0102(g)(4) and shown in Figure 2."
)
CLOSURE_TEMPORAL = [
    {
        "exception_rule": "not_allowed",
        "window_kind": "daily",
        "effective_from_md": None,
        "effective_to_md": None,
        "daily_start": "00:00:00",
        "daily_end": "23:59:00",
        "season_label": (
            "Year-round closure per SD Muni Code §63.0102(g)(1) — "
            "dogs prohibited at all times"
        ),
    },
]


# ─── Bucket assignment ───────────────────────────────────────────────

def bucket_for(fid: int) -> dict:
    """Return the rule bundle for this beach."""
    if fid == 8358:  # Ocean Beach Dog Beach
        return {
            "label": "dog_beach",
            "ps_id": PS_BCHDOG,
            "evidence_url": URL_BCHREG,
            "sand_rule": "off_leash_voice_control",
            "water_rule": "off_leash_voice_control",
            "sand_verbatim": DOG_BEACH_VERBATIM,
            "water_verbatim": DOG_BEACH_VERBATIM,
            "temporal": [],  # 24/7 off-leash, no restriction
        }
    if fid == 9716:  # Fiesta Island
        return {
            "label": "fiesta_island",
            "ps_id": PS_BCHDOG,
            "evidence_url": URL_BCHDOG,
            "sand_rule": "off_leash_voice_control",
            "water_rule": "off_leash_voice_control",
            "sand_verbatim": FIESTA_VERBATIM,
            "water_verbatim": FIESTA_VERBATIM,
            "temporal": FIESTA_TEMPORAL,
        }
    if fid in (8973, 8348):  # Boomer Beach, La Jolla Cove
        return {
            "label": "closure_area",
            "ps_id": PS_MUNI,
            "evidence_url": URL_MUNI,
            "sand_rule": "not_allowed",
            "water_rule": "not_allowed",
            "sand_verbatim": CLOSURE_VERBATIM,
            "water_verbatim": CLOSURE_VERBATIM,
            "temporal": CLOSURE_TEMPORAL,
        }
    return {
        "label": "default_mb_rule",
        "ps_id": PS_BCHDOG,
        "evidence_url": URL_BCHREG,
        "sand_rule": "on_leash",
        "water_rule": "on_leash",
        "sand_verbatim": DEFAULT_VERBATIM_SAND,
        "water_verbatim": DEFAULT_WATER_VERBATIM,
        "temporal": DEFAULT_TEMPORAL,
    }


SKIP_FIDS = {
    6202,  # Coronado Dog Beach — TIGER says SD City but really Coronado / Naval Amphibious Base.
    8347,  # La Jolla Shores — already fully codified (bps_id 12064).
}

NEIGHBORING_CITIES = (
    "Coronado", "Del Mar", "Solana Beach", "Encinitas",
    "Carlsbad", "Oceanside", "Chula Vista", "Imperial Beach",
)


# ─── Scope query ─────────────────────────────────────────────────────

def fetch_sd_city_beach_fids(cur, only_fid: int | None) -> list[tuple[int, str]]:
    if only_fid is not None:
        cur.execute(
            "SELECT fid, COALESCE(display_name_override, name) AS name "
            "  FROM beaches_gold WHERE fid = %s AND is_active",
            (only_fid,),
        )
        return [(r["fid"], r["name"]) for r in cur.fetchall()]
    cur.execute("""
        WITH sd_city_buf AS (
            SELECT ST_Union(ST_Buffer(j.geom::geography, 50)::geometry) AS g
              FROM jurisdictions j WHERE j.name = 'San Diego'
        ),
        non_sd_cities AS (
            SELECT ST_Union(j.geom) AS g
              FROM jurisdictions j WHERE j.name = ANY(%s)
        )
        SELECT bg.fid, COALESCE(bg.display_name_override, bg.name) AS name
          FROM beaches_gold bg, sd_city_buf, non_sd_cities
         WHERE bg.state = 'CA' AND bg.county_name = 'San Diego' AND bg.is_active
           AND ST_Intersects(bg.geom, sd_city_buf.g)
           AND NOT ST_Intersects(bg.geom, non_sd_cities.g)
         ORDER BY bg.fid;
    """, (list(NEIGHBORING_CITIES),))
    return [(r["fid"], r["name"]) for r in cur.fetchall() if r["fid"] not in SKIP_FIDS]


# ─── Per-beach apply ─────────────────────────────────────────────────

def upsert_bps_row(cur, fid: int, section: str, rule: str, ps_id: int,
                   evidence_url: str, evidence_verbatim: str) -> int:
    """INSERT or UPDATE one beach_policy_source row. Returns bps_id."""
    # The natural key for "same beach, same source, same section, same region" is
    # (beach_fid, policy_source_id, section, region_name). region_name is NULL
    # for whole-beach rules. There's no constraint enforcing it, so we do a
    # manual lookup-then-insert.
    cur.execute("""
        SELECT id FROM beach_policy_source
         WHERE beach_fid = %s AND policy_source_id = %s
           AND section = %s AND region_name IS NULL
         LIMIT 1
    """, (fid, ps_id, section))
    existing = cur.fetchone()
    if existing:
        cur.execute("""
            UPDATE beach_policy_source
               SET rule = %s,
                   evidence_verbatim = %s,
                   evidence_url = %s,
                   operative_status = 'operative',
                   last_verified = now()
             WHERE id = %s
        """, (rule, evidence_verbatim, evidence_url, existing["id"]))
        return existing["id"]
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
    """, (fid, ps_id, section, rule, evidence_verbatim, evidence_url))
    return cur.fetchone()["id"]


def replace_temporal_rows(cur, fid: int, bps_id: int, section: str,
                          ps_id: int, temporal: list[dict]) -> int:
    """Replace beach_policy_source_temporal rows for this bps_id."""
    cur.execute("""
        DELETE FROM beach_policy_source_temporal
         WHERE bps_id = %s AND section = %s
    """, (bps_id, section))
    n = 0
    for t in temporal:
        cur.execute("""
            INSERT INTO beach_policy_source_temporal (
                bps_id, beach_fid, policy_source_id, section,
                exception_rule, window_kind,
                effective_from_md, effective_to_md,
                daily_start, daily_end,
                season_label, extracted_via, extractor_confidence,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s::time, %s::time,
                %s, %s, %s,
                now(), now()
            )
        """, (
            bps_id, fid, ps_id, section,
            t["exception_rule"], t["window_kind"],
            t["effective_from_md"], t["effective_to_md"],
            t["daily_start"], t["daily_end"],
            t["season_label"], "manual_codify_sd_city_2026-05-30", 1.0,
        ))
        n += 1
    return n


def apply_to_beach(cur, fid: int, bucket: dict, log) -> tuple[int, int]:
    """Returns (n_bps_upserts, n_temporal_inserts)."""
    n_bps = 0
    n_temporal = 0
    for section, rule, verbatim in [
        ("sand",  bucket["sand_rule"],  bucket["sand_verbatim"]),
        ("water", bucket["water_rule"], bucket["water_verbatim"]),
    ]:
        bps_id = upsert_bps_row(
            cur, fid, section, rule, bucket["ps_id"],
            bucket["evidence_url"], verbatim,
        )
        n_bps += 1
        n_temporal += replace_temporal_rows(
            cur, fid, bps_id, section, bucket["ps_id"], bucket["temporal"],
        )
        log.append(f"      {section}: bps_id={bps_id} rule={rule} temporal_rows={len(bucket['temporal'])}")
    # Regenerate zone_rules
    cur.execute("SELECT public._promote_zone_rules_for_fid(%s) AS r", (fid,))
    log.append(f"      _promote_zone_rules_for_fid → {cur.fetchone()['r']}")
    return n_bps, n_temporal


# ─── Main ─────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fid", type=int, default=None)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    beaches = fetch_sd_city_beach_fids(cur, args.fid)
    print(f"Scope: {len(beaches)} SD City beaches")
    if args.limit:
        beaches = beaches[:args.limit]
        print(f"  limited to first {len(beaches)}")

    n_processed = 0
    n_failed = 0
    bucket_counts: dict[str, int] = {}

    for fid, name in beaches:
        bucket = bucket_for(fid)
        bucket_counts[bucket["label"]] = bucket_counts.get(bucket["label"], 0) + 1
        if args.dry_run:
            print(f"  DRY  fid={fid:<6} {name[:50]:<50} → {bucket['label']} (ps_id={bucket['ps_id']})")
            continue
        log: list[str] = []
        try:
            apply_to_beach(cur, fid, bucket, log)
            conn.commit()
            n_processed += 1
            print(f"  OK   fid={fid:<6} {name[:50]:<50} → {bucket['label']}")
            if args.verbose:
                for line in log: print(line)
        except Exception as e:
            conn.rollback()
            n_failed += 1
            import traceback
            print(f"  FAIL fid={fid:<6} {name[:50]:<50}")
            traceback.print_exc()

    print(f"\nDone. processed={n_processed} failed={n_failed}")
    print(f"Bucket distribution: {bucket_counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
