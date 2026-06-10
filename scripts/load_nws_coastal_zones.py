"""Bulk-load NWS coastal forecast zones into public.nws_zones.

Why: refresh-nws-srf lazily warms nws_zones from zones the SRF parser
encounters, but coverage is uneven (HI only has Big Island; NH has 1
zone; WA has 1). NWS publishes the complete forecast zone inventory via
api.weather.gov; this script loads ALL forecast zones for the requested
coastal states up front so beaches_gold.coastal_zone_id binds against the
full set.

After loading, the script:
  1. Resets '__no_zone__' sentinels in beaches_gold for the loaded
     states (the old rebind couldn't match them; now it can).
  2. Fires _rebind_beach_coastal_zones(state) for each state.

Run:
  python scripts/load_nws_coastal_zones.py                  # all coastal states
  python scripts/load_nws_coastal_zones.py CA,OR,WA         # specific states
  python scripts/load_nws_coastal_zones.py HI,NH,MD,WA      # just the gaps

Cadence: one-shot. Re-runs are idempotent (UPSERT on zone_id). Future
state launches don't need to call this — it's a backfill operator tool.
"""
from __future__ import annotations
import sys, os, json, time

import psycopg2
import requests

# Bootstrap repo root so `from scripts.common.db import connect` works
# whether imported or invoked as a script.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect

NWS_BASE = "https://api.weather.gov"
USER_AGENT = ("DogBeachScout/1.0 (https://dogbeachscout.com; "
              "franz@franzfunk.com) coastal-zone-loader")

# Coastal + Great-Lakes states. Great-Lakes states are included on the
# off-chance NWS publishes their lakeshore forecast zones — the API will
# just return empty if not. Inland states omitted.
COASTAL_STATES = ["CA","OR","WA","HI","AK",
                  "MA","RI","CT","NY","NJ","DE","MD","VA","NC","SC",
                  "GA","FL","AL","MS","LA","TX","ME","NH",
                  "MI","WI","MN","IL","IN","OH","PA"]


def _get_with_retry(url: str, params: dict | None = None, retries: int = 4) -> dict | None:
    """GET with backoff. NWS API returns transient 502s under load."""
    delay = 2.0
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params,
                             headers={"User-Agent": USER_AGENT,
                                      "Accept": "application/geo+json"},
                             timeout=45)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if 500 <= r.status_code < 600 and attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return None


def fetch_state_forecast_zones(state: str) -> list[dict]:
    """List forecast zones for a state. Result rows may or may not
    include geometry depending on NWS — for zones missing geometry we
    fetch the per-zone URL below."""
    j = _get_with_retry(f"{NWS_BASE}/zones",
                        {"type": "forecast", "area": state})
    return (j or {}).get("features", []) or []


def fetch_zone_full(zone_url: str) -> dict | None:
    """Fetch a single zone's full GeoJSON when the list response lacks geom."""
    return _get_with_retry(zone_url)


def upsert_zone(cur, feature: dict) -> bool:
    """UPSERT one forecast zone row. Returns True on success."""
    props = feature.get("properties") or {}
    zone_id = props.get("id") or feature.get("id", "").rsplit("/", 1)[-1]
    if not zone_id:
        return False
    geom = feature.get("geometry")
    if not geom:
        # List response without geometry — refetch per-zone URL.
        zone_url = feature.get("id") or f"{NWS_BASE}/zones/forecast/{zone_id}"
        full = fetch_zone_full(zone_url)
        if not full:
            return False
        geom = full.get("geometry")
        if not geom:
            return False
        # Refresh props from the full response (usually richer).
        props = full.get("properties") or props
        feature = full

    name = props.get("name")
    # NWS returns state in a JSON array; take the first element when present.
    state_arr = props.get("state")
    state = (state_arr[0] if isinstance(state_arr, list) and state_arr
             else state_arr if isinstance(state_arr, str) else None)

    cur.execute(
        """
        INSERT INTO public.nws_zones (
            zone_id, zone_type, name, state, geom, fetched_at, raw
        )
        VALUES (
            %s, 'forecast', %s, %s,
            ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
            now(), %s
        )
        ON CONFLICT (zone_id) DO UPDATE SET
            name       = EXCLUDED.name,
            state      = EXCLUDED.state,
            geom       = EXCLUDED.geom,
            fetched_at = EXCLUDED.fetched_at,
            raw        = EXCLUDED.raw
        """,
        (zone_id, name, state, json.dumps(geom), json.dumps(feature)),
    )
    return True


def reset_no_zone_sentinels(cur, state: str) -> int:
    """Clear '__no_zone__' so the next rebind can re-attempt with the
    newly-loaded zones."""
    cur.execute(
        """
        UPDATE public.beaches_gold
           SET coastal_zone_id = NULL
         WHERE state = %s
           AND coastal_zone_id = '__no_zone__'
        """, (state,)
    )
    return cur.rowcount


def rebind_state(cur, state: str) -> int:
    """Fire _rebind_beach_coastal_zones(state) and return rows touched."""
    cur.execute("SELECT public._rebind_beach_coastal_zones(%s)", (state,))
    row = cur.fetchone()
    return int(row[0] if row else 0)


def process_state(conn, state: str) -> dict:
    """Load all forecast zones for a state, reset sentinels, rebind."""
    stats = {"state": state, "zones_loaded": 0, "zones_skipped": 0,
             "sentinels_cleared": 0, "rebound": 0}
    try:
        features = fetch_state_forecast_zones(state)
    except Exception as e:
        stats["error"] = f"list_fetch: {e}"
        return stats

    with conn.cursor() as cur:
        for f in features:
            if upsert_zone(cur, f):
                stats["zones_loaded"] += 1
            else:
                stats["zones_skipped"] += 1
        # Reset sentinels so previously-unbindable beaches get a fresh
        # attempt against the now-loaded zones.
        stats["sentinels_cleared"] = reset_no_zone_sentinels(cur, state)
    conn.commit()

    # Rebind in its own transaction — the function has its own
    # statement_timeout setting.
    with conn.cursor() as cur:
        try:
            stats["rebound"] = rebind_state(cur, state)
        except Exception as e:
            stats["rebind_error"] = str(e).splitlines()[0][:200]
    conn.commit()
    return stats


def main() -> int:
    if len(sys.argv) > 1:
        states = [s.strip().upper() for s in sys.argv[1].split(",") if s.strip()]
    else:
        states = COASTAL_STATES

    with connect() as conn:
        for state in states:
            stats = process_state(conn, state)
            err = stats.get("error") or stats.get("rebind_error")
            line = (f"{state}: loaded={stats['zones_loaded']} "
                    f"skipped={stats['zones_skipped']} "
                    f"sentinels_cleared={stats['sentinels_cleared']} "
                    f"rebound={stats['rebound']}"
                    + (f" ERR={err}" if err else ""))
            print(line)
            # Be polite to NWS — small delay between states.
            time.sleep(0.5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
