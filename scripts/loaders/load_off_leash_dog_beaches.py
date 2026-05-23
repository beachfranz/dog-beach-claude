"""Load the curated off_leash_dog_beaches geojson into Supabase."""

import json, sys
from pathlib import Path

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.common.supa import supa

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

    # Truncate so re-runs are clean. RPC may not exist — wrap in try/except.
    try:
        supa("/rest/v1/rpc/exec_sql", method="POST",
             body={"q": "truncate public.off_leash_dog_beaches restart identity"})
    except RuntimeError as e:
        print(f"truncate RPC unavailable (continuing): {e}", file=sys.stderr)

    # Insert via PostgREST in one batch
    try:
        supa("/rest/v1/off_leash_dog_beaches", method="POST",
             body=rows, prefer="return=minimal")
    except RuntimeError as e:
        print(f"Insert failed: {e}", file=sys.stderr)
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
