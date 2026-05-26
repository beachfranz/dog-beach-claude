"""
ingest_dog_park_discovery_queue.py — Phase B1 inventory ingest.

Consumes pending rows from dog_park_discovery_queue (catalog-walker output)
and creates new dog_parks_gold rows for parks that don't yet exist.

Per the walker analysis 2026-05-25 LATE: catalog walker surfaced 53 famous
CA dog parks (SF=21, Sac=12, LA=9, etc.) that are absent from gold. The
walker captures them with operator_id + catalog_park_name + catalog_park_url.
This script:

  1. For each pending queue row, build a Google Places searchText query
     ("{catalog_park_name}, {operator_city}, CA"); geocode via Google Places.
  2. If hit: INSERT into dog_parks_gold with website=catalog_park_url so
     the existing OSM-shortcut extractor can extract amenities on the next
     run; mark queue row status='ingested', set new_park_fid.
  3. If miss: status='rejected', notes='google_places_no_match'.
  4. The existing tg_compute_weather_grid_cell trigger auto-populates
     weather_grid_lat/lon. tg_dog_parks_gold_default_policy creates the
     dog_park_dog_policy stub row. dog_parks_active_unsourced view picks
     up the new row.

Reuses harvest/geocode.py's google_places_searchtext + derive_city_state.

Usage:
  python scripts/ingest_dog_park_discovery_queue.py --dry-run
  python scripts/ingest_dog_park_discovery_queue.py --apply --limit 5
  python scripts/ingest_dog_park_discovery_queue.py --apply --op-id 78
"""
from __future__ import annotations
import argparse, json, os, sys, time
from pathlib import Path

# Force UTF-8 stdout so unicode arrows / em-dashes don't crash on Windows cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.harvest.geocode import google_places_searchtext, derive_city_state


def fetch_queue(cur, op_id: int | None, limit: int | None) -> list[dict]:
    where = ["status = 'pending'"]
    params: list = []
    if op_id is not None:
        where.append("operator_id = %s")
        params.append(op_id)
    sql = f"""
      SELECT id, operator_id, operator_name, catalog_park_name,
             catalog_park_url, blurb, catalog_source_url, best_similarity
        FROM public.dog_park_discovery_queue
       WHERE {' AND '.join(where)}
       ORDER BY operator_id, id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, params)
    return list(cur.fetchall())


def extract_places_fields(place: dict) -> dict:
    """Pull the consumer-relevant fields from a Google Places search result."""
    loc = place.get("location") or {}
    addr_comp = {c.get("types", [None])[0]: c.get("longText") or c.get("long_name")
                 for c in (place.get("addressComponents") or [])}
    return {
        "lat":             float(loc.get("latitude")) if loc.get("latitude") is not None else None,
        "lng":             float(loc.get("longitude")) if loc.get("longitude") is not None else None,
        "formatted_addr":  place.get("formattedAddress") or place.get("shortFormattedAddress"),
        "address_street":  (
            (addr_comp.get("street_number", "") + " " + addr_comp.get("route", "")).strip()
            or None
        ),
        "address_city":    addr_comp.get("locality") or addr_comp.get("sublocality"),
        "address_state":   addr_comp.get("administrative_area_level_1"),
        "address_zip":     addr_comp.get("postal_code"),
        "address_county":  addr_comp.get("administrative_area_level_2"),
        "google_place_id": place.get("id"),
        "google_data":     place,
    }


def insert_dog_park(cur, queue_row: dict, fields: dict) -> int | None:
    """INSERT into dog_parks_gold. Triggers fire for weather_grid + dog_park_dog_policy."""
    name = queue_row['catalog_park_name']
    source_id = f"discovery_queue_{queue_row['id']}"
    state = (fields.get('address_state') or 'CA')[:2].upper() if fields.get('address_state') else 'CA'

    cur.execute("""
        INSERT INTO public.dog_parks_gold
            (name, source_code, source_id, lat, lon, geom,
             state, county_name,
             address_street, address_city, address_state, address_zip, address_county,
             website,
             inferred_operator_id, attribution_method, attribution_confidence,
             promoted_from, promoted_at,
             is_active, is_scoreable,
             tags)
        VALUES (%s, 'catalog_walker', %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s, %s,
                %s, %s, %s, %s, %s,
                %s,
                %s, 'catalog_walker_match', 'medium',
                'dog_park_discovery_queue_v1', now(),
                true, true,
                %s::jsonb)
        RETURNING fid
    """, (
        name, source_id, fields['lat'], fields['lng'],
        fields['lng'], fields['lat'],                            # geom: lng, lat order
        state, fields.get('address_county'),
        fields.get('address_street'), fields.get('address_city'),
        fields.get('address_state'), fields.get('address_zip'),
        fields.get('address_county'),
        queue_row['catalog_park_url'],
        queue_row['operator_id'],
        json.dumps({"google_place_id": fields.get('google_place_id'),
                    "discovery_source_url": queue_row['catalog_source_url'],
                    "discovery_queue_id": queue_row['id']}),
    ))
    return cur.fetchone()['fid']


def mark_queue_row(cur, queue_id: int, status: str, new_park_fid: int | None,
                   notes: str | None) -> None:
    cur.execute("""
        UPDATE public.dog_park_discovery_queue
           SET status = %s,
               review_notes = COALESCE(%s, review_notes),
               updated_at = now()
         WHERE id = %s
    """, (status, notes, queue_id))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--op-id", type=int, default=None,
                    help="Only process queue rows for this operator_id")
    args = ap.parse_args()

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    rows = fetch_queue(cur, args.op_id, args.limit)
    print(f"discovery queue → ingest: {len(rows)} pending rows; apply={args.apply}")

    summary = {"ingested": 0, "no_match": 0, "no_lat_lng": 0,
               "duplicate": 0, "error": 0}

    for q in rows:
        op_city, op_state = derive_city_state(q['operator_name'] or '')
        query_parts = [q['catalog_park_name']]
        if op_city: query_parts.append(op_city)
        query_parts.append(op_state)
        query = ", ".join(query_parts)
        print(f"\n[queue {q['id']}] op={q['operator_id']} '{q['catalog_park_name'][:40]}'")
        print(f"  query: {query}")

        place = google_places_searchtext(query)
        if not place:
            print(f"  [-] google_places: no match")
            if args.apply:
                mark_queue_row(cur, q['id'], 'rejected', None, 'google_places_no_match')
                conn.commit()
            summary['no_match'] += 1
            continue

        fields = extract_places_fields(place)
        if fields['lat'] is None or fields['lng'] is None:
            print(f"  [-] google_places hit but no lat/lng")
            if args.apply:
                mark_queue_row(cur, q['id'], 'rejected', None, 'google_places_no_lat_lng')
                conn.commit()
            summary['no_lat_lng'] += 1
            continue

        # Quick distance sanity check: is the geocoded location plausibly the
        # operator's city? (Catch SF Alamo Square geocoding to a different
        # state's namesake by mistake.) Use existing dog_parks_gold rows for
        # this operator to check.
        cur.execute("""
            SELECT min(ST_Distance(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                geom::geography)) / 1000.0 AS km
              FROM public.dog_parks_gold
             WHERE inferred_operator_id = %s AND is_active AND geom IS NOT NULL
        """, (fields['lng'], fields['lat'], q['operator_id']))
        nearest = cur.fetchone()
        nearest_km = nearest['km'] if nearest and nearest['km'] is not None else None
        if nearest_km is not None and nearest_km > 50:
            print(f"  [-] geocode {nearest_km:.1f}km from op's nearest park — likely wrong")
            if args.apply:
                mark_queue_row(cur, q['id'], 'rejected', None,
                               f'geocode_too_far:{nearest_km:.1f}km')
                conn.commit()
            summary['no_match'] += 1
            continue

        # Check for existing park at this lat/lng (within ~150m) → duplicate
        cur.execute("""
            SELECT fid, name FROM public.dog_parks_gold
             WHERE is_active AND geom IS NOT NULL
               AND ST_DWithin(geom::geography,
                              ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                              150)
             LIMIT 1
        """, (fields['lng'], fields['lat']))
        dupe = cur.fetchone()
        if dupe:
            print(f"  [dup] within 150m of existing fid={dupe['fid']} '{dupe['name']}'")
            if args.apply:
                mark_queue_row(cur, q['id'], 'duplicate', None,
                               f'within_150m_of_fid_{dupe["fid"]}')
                conn.commit()
            summary['duplicate'] += 1
            continue

        print(f"  [+] geocoded ({fields['lat']:.5f}, {fields['lng']:.5f}) "
              f"{fields.get('address_city') or '?'} nearest_op_park={nearest_km:.1f}km" if nearest_km else "")

        if args.apply:
            try:
                new_fid = insert_dog_park(cur, q, fields)
                mark_queue_row(cur, q['id'], 'ingested', new_fid,
                               f'new_park_fid={new_fid}')
                conn.commit()
                summary['ingested'] += 1
                print(f"  [INGESTED] new dog_parks_gold.fid = {new_fid}")
            except Exception as e:
                conn.rollback()
                print(f"  [!] INSERT error: {e}")
                summary['error'] += 1
        else:
            summary['ingested'] += 1  # would-be ingest in dry-run

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:18} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
