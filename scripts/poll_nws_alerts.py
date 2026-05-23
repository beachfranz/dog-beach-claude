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

import argparse
import json
import os
import time

import psycopg2
import psycopg2.extras
import requests

from scripts.common.db import connect

NWS_BASE = "https://api.weather.gov"
USER_AGENT = "DogBeachScout/1.0 (https://dogbeachscout.com; franz@franzfunk.com) nws-alerts-poller"

PILOT_FIDS_QUERY = """
  SELECT fid FROM public.beaches_gold
   WHERE state='CA' AND is_active = true
     AND scoring_tier IN ('daily','hourly')
     AND catchment_score IS NOT NULL
   ORDER BY catchment_score DESC LIMIT 30
"""


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


_ZONE_URL_RE = None  # lazy import target

def _zone_id_and_type_from_url(zone_url: str) -> tuple[str | None, str | None]:
    """Parse 'https://api.weather.gov/zones/forecast/PZZ565' → ('PZZ565','forecast').
    Returns (None, None) if not a recognizable zone URL."""
    global _ZONE_URL_RE
    if _ZONE_URL_RE is None:
        import re
        _ZONE_URL_RE = re.compile(r"/zones/([a-z]+)/([A-Z0-9_]+)(?:\?|$|/)")
    m = _ZONE_URL_RE.search(zone_url or "")
    if not m:
        return None, None
    return m.group(2), m.group(1)


def get_zone_geom_cached(conn, zone_url: str, ttl_days: int = 365) -> str | None:
    """Return zone GeoJSON-string (geometry) for the given zone URL, fetching
    from api.weather.gov on cache miss. Caches into public.nws_zones.

    Zones change ~annually; default TTL is 365 days. Returns None if the
    URL isn't parseable or the API call fails.
    """
    zone_id, zone_type = _zone_id_and_type_from_url(zone_url)
    if not (zone_id and zone_type):
        return None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ST_AsGeoJSON(geom), fetched_at FROM public.nws_zones
             WHERE zone_id = %s
               AND fetched_at > now() - (%s || ' days')::interval
               AND geom IS NOT NULL
        """, (zone_id, str(ttl_days)))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    # Cache miss — fetch
    try:
        r = requests.get(zone_url, headers={"User-Agent": USER_AGENT,
                                            "Accept": "application/geo+json"},
                         timeout=30)
        if r.status_code != 200:
            return None
        feat = r.json()
    except requests.RequestException:
        return None
    geom = feat.get("geometry")
    props = feat.get("properties", {}) or {}
    if not geom:
        # Some zones (rare) lack geometry — record a "tried" sentinel anyway
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.nws_zones (zone_id, zone_type, name, state, raw)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (zone_id) DO UPDATE SET
                  fetched_at = now(), raw = EXCLUDED.raw
            """, (zone_id, zone_type, props.get("name"), props.get("state"),
                  json.dumps(props)))
            conn.commit()
        return None
    geom_json = json.dumps(geom)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.nws_zones (zone_id, zone_type, name, state, geom, raw)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)
            ON CONFLICT (zone_id) DO UPDATE SET
              fetched_at = now(), name = EXCLUDED.name, state = EXCLUDED.state,
              geom = EXCLUDED.geom, raw = EXCLUDED.raw
        """, (zone_id, zone_type, props.get("name"), props.get("state"),
              geom_json, json.dumps(props)))
        conn.commit()
    return geom_json


def pip_alert_against_beaches(conn, feature: dict, beach_fids: list[int]) -> list[int]:
    """For one alert feature, return the list of beach_fids whose geom intersects.

    Tries the alert's inline GeoJSON geometry first. If absent (which is
    common for NWS Beach Hazards Statements / High Surf for coastal waters
    that ship as `affectedZones=[...]` only), falls back to resolving each
    affected zone polygon via the NWS zones API (cached in nws_zones).
    """
    # Path 1: inline geometry
    geom = feature.get("geometry")
    if geom:
        geom_json = json.dumps(geom)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.fid FROM public.beaches_gold b
                 WHERE b.fid = ANY(%s)
                   AND b.geom IS NOT NULL
                   AND ST_Intersects(b.geom, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            """, (beach_fids, geom_json))
            return [r[0] for r in cur.fetchall()]

    # Path 2: zone-coded — resolve each zone polygon and union the matches
    props = feature.get("properties", {}) or {}
    zone_urls = props.get("affectedZones") or []
    if not zone_urls:
        return []
    matched: set[int] = set()
    for zurl in zone_urls:
        zone_geom_json = get_zone_geom_cached(conn, zurl)
        if not zone_geom_json:
            continue
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.fid FROM public.beaches_gold b
                 WHERE b.fid = ANY(%s)
                   AND b.geom IS NOT NULL
                   AND ST_Intersects(b.geom, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            """, (beach_fids, zone_geom_json))
            matched.update(r[0] for r in cur.fetchall())
    return sorted(matched)


def upsert_alert(conn, beach_fid: int, alert: dict) -> None:
    p = alert.get("properties", {}) or {}
    alert_id = alert.get("id") or p.get("id") or ""
    # NWS sometimes uses 'expires' or 'ends'; prefer ends (final) over expires
    valid_to = p.get("ends") or p.get("expires")
    valid_from = p.get("effective") or p.get("onset") or p.get("sent")
    event_type = p.get("event")
    if not (alert_id and valid_from and valid_to and event_type):
        return

    # Map NWS severity (Minor/Moderate/Severe/Extreme/Unknown) → our enum
    nws_sev = (p.get("severity") or "").lower()
    severity = nws_sev if nws_sev in ("minor", "moderate", "severe", "extreme") else "moderate"

    advisory_key = f"nws:{alert_id}"
    # NWS-specific payload preserved in raw_data
    raw = {
        "certainty":   p.get("certainty"),
        "urgency":     p.get("urgency"),
        "headline":    p.get("headline"),
        "description": p.get("description"),
        "instruction": p.get("instruction"),
        "sender":      p.get("sender"),
        "areaDesc":    p.get("areaDesc"),
    }
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.beach_advisory (
              beach_fid, advisory_key, source, event_type, severity,
              valid_from, valid_to, label, icon, raw_data, fetched_at
            ) VALUES (%s, %s, 'nws', %s, %s, %s, %s, %s, '⚠️', %s, now())
            ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
              event_type = EXCLUDED.event_type,
              severity   = EXCLUDED.severity,
              valid_from = EXCLUDED.valid_from,
              valid_to   = EXCLUDED.valid_to,
              label      = EXCLUDED.label,
              raw_data   = EXCLUDED.raw_data,
              fetched_at = now();
        """, (
            beach_fid, advisory_key, event_type, severity,
            valid_from, valid_to, event_type, json.dumps(raw),
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

    conn = connect()
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
