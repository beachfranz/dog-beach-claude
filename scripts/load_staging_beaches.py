"""
load_staging_beaches.py
-----------------------
Loads a geocoded/flagged beach CSV into beaches_staging via the
ingest-beaches-staging edge function. No service key required locally.

Usage:
    python scripts/load_staging_beaches.py [path/to/csv]

Defaults to: share/Dog_Beaches/ca_beaches_flagged.csv
"""

import csv
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://ehlzbwtrsxaaukurekau.supabase.co"
ANON_KEY     = "sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk"
ENDPOINT     = f"{SUPABASE_URL}/functions/v1/ingest-beaches-staging"
BATCH_SIZE   = 100

DEFAULT_CSV  = Path(__file__).parent.parent / "share" / "Dog_Beaches" / "ca_beaches_flagged.csv"


def coerce_int(val: str):
    try: return int(val)
    except: return None

def coerce_float(val: str):
    try: return float(val)
    except: return None

def clean(val: str):
    return val.strip() or None


def build_record(r: dict) -> dict | None:
    lat = coerce_float(r.get("latitude", ""))
    lon = coerce_float(r.get("longitude", ""))
    if lat is None or lon is None:
        return None
    return {
        "source_fid":             coerce_int(r.get("fid", "")),
        "display_name":           clean(r.get("name", "")) or "Unknown",
        "latitude":               lat,
        "longitude":              lon,
        "formatted_address":      clean(r.get("formatted_address", "")),
        "street_number":          clean(r.get("street_number", "")),
        "route":                  clean(r.get("route", "")),
        "city":                   clean(r.get("city", "")),
        "county":                 clean(r.get("county", "")),
        "state":                  clean(r.get("state", "")),
        "country":                clean(r.get("country", "")),
        "zip":                    clean(r.get("zip", "")),
        "governing_jurisdiction": clean(r.get("governing_jurisdiction", "")),
        "governing_body":         clean(r.get("governing_body", "")),
        "review_status":          clean(r.get("review_status", "")) or "OK",
        "review_notes":           clean(r.get("review_notes", "")),
    }


def post_batch(records: list[dict]) -> tuple[int, list[str]]:
    resp = requests.post(
        ENDPOINT,
        headers={
            "Authorization": f"Bearer {ANON_KEY}",
            "Content-Type":  "application/json",
        },
        json={"records": records},
        timeout=30,
    )
    if not resp.ok:
        return 0, [f"HTTP {resp.status_code}: {resp.text}"]
    data = resp.json()
    return data.get("inserted", 0), data.get("errors", [])


def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV

    print(f"Reading {csv_path}...")
    with open(csv_path, encoding="utf-8") as f:
        raw_rows = list(csv.DictReader(f))
    print(f"  {len(raw_rows):,} rows in file")

    records = []
    skipped = 0
    for r in raw_rows:
        rec = build_record(r)
        if rec:
            records.append(rec)
        else:
            skipped += 1

    print(f"  {len(records):,} valid records, {skipped} skipped (no coords)")
    print(f"  Posting in batches of {BATCH_SIZE}...")

    total_inserted = 0
    total_errors   = []

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        inserted, errors = post_batch(batch)
        total_inserted += inserted
        total_errors   += errors
        print(f"  Batch {i // BATCH_SIZE + 1}: {inserted} inserted")

    print(f"\nDone. {total_inserted} inserted, {len(total_errors)} errors.")
    for e in total_errors:
        print(f"  ERROR: {e}")


if __name__ == "__main__":
    main()
