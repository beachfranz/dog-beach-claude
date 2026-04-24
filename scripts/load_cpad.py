"""
load_cpad.py
------------
Loads CPAD_AgencyLevel polygons into the `cpad_units` PostGIS table by
streaming them from the CNRA ArcGIS FeatureServer in paginated batches.

CPAD is ~160k features. Default run fetches everything; pass --coastal
to filter to coastal counties only (~20% of rows).

Usage:
    python scripts/load_cpad.py                   # full CA load
    python scripts/load_cpad.py --coastal         # coastal counties only
    python scripts/load_cpad.py --limit 2000      # smoke-test a single page
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

SUPABASE_URL = "https://ehlzbwtrsxaaukurekau.supabase.co"
ANON_KEY     = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu")
ENDPOINT     = f"{SUPABASE_URL}/functions/v1/admin-load-cpad-batch"

CPAD_BASE    = "https://gis.cnra.ca.gov/arcgis/rest/services/Boundaries/CPAD_AgencyLevel/MapServer/0/query"
CPAD_PAGE    = 2000
DB_BATCH     = 500       # edge function cap; max-out for fewest round-trips
FETCH_WORKERS = 8        # parallel page fetches from CPAD
UPLOAD_WORKERS = 8       # parallel upserts to our DB

# Major coastal CA counties (+Monterey, SLO inland coast). Used with --coastal.
COASTAL_COUNTIES = [
    "Del Norte", "Humboldt", "Mendocino", "Sonoma", "Marin",
    "San Francisco", "San Mateo", "Santa Cruz", "Monterey",
    "San Luis Obispo", "Santa Barbara", "Ventura",
    "Los Angeles", "Orange", "San Diego",
]


def cpad_request(params):
    # POST — long objectIds lists blow the GET URL length limit (414).
    r = requests.post(CPAD_BASE, data=params, timeout=120)
    r.raise_for_status()
    return r.json()


def get_object_ids(where_clause):
    print(f"Fetching object IDs ({where_clause})...")
    data = cpad_request({"where": where_clause, "returnIdsOnly": "true", "f": "json"})
    ids = data.get("objectIds") or []
    print(f"  {len(ids):,} object IDs")
    return ids


def fetch_page(object_ids):
    data = cpad_request({
        "objectIds": ",".join(str(i) for i in object_ids),
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "geojson",
    })
    return data.get("features") or []


def upload_batch(features):
    r = requests.post(
        ENDPOINT,
        headers={
            "Authorization":  f"Bearer {ANON_KEY}",
            "Content-Type":   "application/json",
            "x-admin-secret": ADMIN_SECRET,
        },
        json={"features": features},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


def chunk(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coastal", action="store_true", help="coastal counties only")
    ap.add_argument("--limit",   type=int, default=None, help="max features (smoke test)")
    args = ap.parse_args()

    if args.coastal:
        quoted = ", ".join(f"'{c}'" for c in COASTAL_COUNTIES)
        where = f"COUNTY IN ({quoted})"
    else:
        where = "1=1"

    t0 = time.time()
    object_ids = get_object_ids(where)
    if args.limit:
        object_ids = object_ids[:args.limit]
        print(f"  limiting to first {len(object_ids)} for smoke test")

    # ── Fetch pages from CPAD ────────────────────────────────────────────────
    print(f"\nFetching {len(object_ids):,} features in pages of {CPAD_PAGE}...")
    pages = list(chunk(object_ids, CPAD_PAGE))
    all_features = []
    t_fetch = time.time()
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = {ex.submit(fetch_page, page): i for i, page in enumerate(pages)}
        for f in as_completed(futures):
            i = futures[f]
            try:
                features = f.result()
                all_features.extend(features)
                print(f"  page {i+1}/{len(pages)} — +{len(features)} (total {len(all_features):,})")
            except Exception as e:
                print(f"  page {i+1} FAILED: {e}", file=sys.stderr)
    print(f"  fetch done in {time.time() - t_fetch:.0f}s — {len(all_features):,} features in memory")

    # ── Upload to DB ─────────────────────────────────────────────────────────
    print(f"\nUploading to cpad_units in batches of {DB_BATCH}...")
    t_upload = time.time()
    totals = {"inserted": 0, "updated": 0, "skipped": 0}
    batches = list(chunk(all_features, DB_BATCH))
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as ex:
        futures = {ex.submit(upload_batch, b): i for i, b in enumerate(batches)}
        for f in as_completed(futures):
            i = futures[f]
            try:
                r = f.result()
                for k in totals:
                    totals[k] += r.get(k, 0) or 0
                if (i+1) % 10 == 0 or i == len(batches) - 1:
                    print(f"  batch {i+1}/{len(batches)} — totals: {totals}")
            except Exception as e:
                print(f"  batch {i+1} FAILED: {e}", file=sys.stderr)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s. Totals: {totals}")


if __name__ == "__main__":
    main()
