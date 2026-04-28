"""Load the curated off_leash_dog_beaches geojson into Supabase."""

import json, os, sys
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
KEY          = os.environ["SUPABASE_SERVICE_KEY"]

GEOJSON = Path("C:/Users/beach/Downloads/california_off_leash_dog_beaches.geojson")

def main():
    gj = json.loads(GEOJSON.read_text(encoding="utf-8"))
    rows = []
    for f in gj["features"]:
        p = f["properties"]
        lng, lat = f["geometry"]["coordinates"]
        rows.append({
            "name":               p.get("name"),
            "region":             p.get("region"),
            "city":               p.get("city"),
            "off_leash_legal":    p.get("off_leash_legal"),
            "off_leash_de_facto": p.get("off_leash_de_facto"),
            "enforcement_risk":   p.get("enforcement_risk"),
            "social_norm":        p.get("social_norm"),
            "confidence":         p.get("confidence"),
            "latitude":           lat,
            "longitude":          lng,
        })

    # Truncate so re-runs are clean.
    httpx.post(
        f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",  # may not exist — fallback below
        headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"},
        json={"q": "truncate public.off_leash_dog_beaches restart identity"},
    )

    # Insert via PostgREST in one batch
    r = httpx.post(
        f"{SUPABASE_URL}/rest/v1/off_leash_dog_beaches",
        headers={
            "apikey": KEY,
            "Authorization": f"Bearer {KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=rows, timeout=30,
    )
    if not r.is_success:
        print(f"Insert failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    print(f"Inserted {len(rows)} off-leash dog beach rows.")

    # Now backfill geom from latitude/longitude (faster than building EWKT per row).
    # This requires a SQL call — easiest via supabase db query.
    sql = """
    update public.off_leash_dog_beaches
       set geom = st_setsrid(st_makepoint(longitude, latitude), 4326)
     where geom is null;
    """
    print("Run this in supabase to backfill geom:")
    print(sql)

if __name__ == "__main__":
    main()
