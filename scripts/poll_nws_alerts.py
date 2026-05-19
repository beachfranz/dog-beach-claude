"""Poll NWS active alerts; PIP polygons against beaches; upsert into beach_active_alert.

Per docs/water_conditions_advisory_spec.md Phase 1. Phase 2 adds the
translation layer (YAML + LLM wildcard fallback). For v1 the
dog_impact_* columns are left NULL.

Pilot scope: top 30 tier-1 CA beaches by catchment_score.

Run:
  python scripts/poll_nws_alerts.py                      # pilot scope (30 CA)
  python scripts/poll_nws_alerts.py --state CA           # all CA scoreable
  python scripts/poll_nws_alerts.py --all-mvp            # CA + OR + WA
  python scripts/poll_nws_alerts.py --dry-run            # show what would upsert

Cadence: 15min cron (per spec). API is free, no key.
"""
from __future__ import annotations
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
import truststore                          # Win Python 3.14 OS-cert store
truststore.inject_into_ssl()

import argparse
import json
import os
import time
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")

NWS_BASE = "https://api.weather.gov"
USER_AGENT = "DogBeachScout/1.0 (https://dogbeachscout.com; franz@franzfunk.com) nws-alerts-poller"

PILOT_FIDS_QUERY = """
  SELECT fid FROM public.beaches_gold
   WHERE state='CA' AND is_active = true
     AND scoring_tier IN ('daily','hourly')
     AND catchment_score IS NOT NULL
   ORDER BY catchment_score DESC LIMIT 30
"""


def _connect_pg():
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def fetch_active_alerts(area: str = "CA") -> list[dict]:
    """Returns GeoJSON features for active NWS alerts in the area."""
    url = f"{NWS_BASE}/alerts/active"
    params = {"area": area}
    r = requests.get(url, params=params,
                     headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"},
                     timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("features", []) or []


def pip_alert_against_beaches(conn, feature: dict, beach_fids: list[int]) -> list[int]:
    """For one alert feature, return the list of beach_fids whose geom intersects."""
    geom = feature.get("geometry")
    if not geom:
        return []   # zone-coded alerts skipped in v1
    geom_json = json.dumps(geom)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b.fid FROM public.beaches_gold b
             WHERE b.fid = ANY(%s)
               AND b.geom IS NOT NULL
               AND ST_Intersects(b.geom, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
        """, (beach_fids, geom_json))
        return [r[0] for r in cur.fetchall()]


def upsert_alert(conn, beach_fid: int, alert: dict) -> None:
    p = alert.get("properties", {}) or {}
    alert_id = alert.get("id") or p.get("id") or ""
    # NWS sometimes uses 'expires' or 'ends'; prefer ends (final) over expires
    valid_to = p.get("ends") or p.get("expires")
    valid_from = p.get("effective") or p.get("onset") or p.get("sent")
    if not (alert_id and valid_from and valid_to):
        return
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.beach_active_alert (
              beach_fid, alert_id, nws_event_type, severity, certainty, urgency,
              headline, description, instruction,
              valid_from, valid_to, source, fetched_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'nws', now())
            ON CONFLICT (beach_fid, alert_id) DO UPDATE SET
              nws_event_type = EXCLUDED.nws_event_type,
              severity       = EXCLUDED.severity,
              certainty      = EXCLUDED.certainty,
              urgency        = EXCLUDED.urgency,
              headline       = EXCLUDED.headline,
              description    = EXCLUDED.description,
              instruction    = EXCLUDED.instruction,
              valid_from     = EXCLUDED.valid_from,
              valid_to       = EXCLUDED.valid_to,
              fetched_at     = now();
        """, (
            beach_fid, alert_id, p.get("event"),
            p.get("severity"), p.get("certainty"), p.get("urgency"),
            p.get("headline"), p.get("description"), p.get("instruction"),
            valid_from, valid_to,
        ))


def main() -> int:
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--pilot", action="store_true",
                     help="Pilot scope: top 30 tier-1 CA beaches by catchment_score (default)")
    grp.add_argument("--state", help="All scoreable beaches in one state")
    grp.add_argument("--all-mvp", action="store_true", help="All MVP+ (CA + OR + WA)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + PIP, print matches, no writes")
    args = ap.parse_args()

    conn = _connect_pg()
    try:
        # Resolve beach scope
        with conn.cursor() as cur:
            if args.state:
                cur.execute("""
                    SELECT fid FROM public.beaches_gold
                     WHERE state = %s AND is_active = true
                       AND scoring_tier IN ('daily','hourly')
                """, (args.state.upper(),))
                states = [args.state.upper()]
            elif args.all_mvp:
                cur.execute("""
                    SELECT fid FROM public.beaches_gold
                     WHERE state IN ('CA','OR','WA') AND is_active = true
                       AND scoring_tier IN ('daily','hourly')
                """)
                states = ["CA", "OR", "WA"]
            else:
                # default = pilot
                cur.execute(PILOT_FIDS_QUERY)
                states = ["CA"]
            beach_fids = [r[0] for r in cur.fetchall()]
        print(f"Scope: {len(beach_fids)} beaches across {states}", flush=True)
        if not beach_fids:
            print("No beaches in scope.")
            return 0

        n_alerts_total = 0
        n_alerts_matched = 0
        n_upserts = 0
        for state in states:
            alerts = fetch_active_alerts(state)
            print(f"\n=== {state}: {len(alerts)} active NWS alerts ===", flush=True)
            n_alerts_total += len(alerts)
            for a in alerts:
                matched_fids = pip_alert_against_beaches(conn, a, beach_fids)
                if not matched_fids:
                    continue
                n_alerts_matched += 1
                p = a.get("properties", {}) or {}
                evt = p.get("event") or "?"
                sev = p.get("severity") or "?"
                print(f"  [{evt}] sev={sev} → matches {len(matched_fids)} beaches: {matched_fids[:5]}{'...' if len(matched_fids)>5 else ''}",
                      flush=True)
                if not args.dry_run:
                    for fid in matched_fids:
                        upsert_alert(conn, fid, a)
                        n_upserts += 1
            if not args.dry_run:
                conn.commit()
            time.sleep(1.0)  # be polite between state queries

        print(f"\n=== SUMMARY ===")
        print(f"  Active alerts fetched: {n_alerts_total}")
        print(f"  Matched ≥1 in-scope beach: {n_alerts_matched}")
        print(f"  Row upserts: {n_upserts}")
        if args.dry_run:
            print(f"  [dry-run] no writes")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
