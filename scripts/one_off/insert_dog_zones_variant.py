"""One-off: insert the dog_zones extraction prompt variant.

Activates closed_8zones_v1 (Sonnet 4.6, structured_json, is_canon=true).
"""
import os
import sys
import urllib.parse
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

PROMPT = """For each of the EIGHT zone types below, decide the most permissive dog status the source explicitly supports, with evidence.

ZONES (closed list):
  - sand          (the actual beach itself: sand or shingle area)
  - trails        (multi-use trails, hiking trails, bike paths through the park)
  - picnic_area   (designated picnic / day-use lawn / table areas)
  - parking_lot   (parking lots, the road in/out)
  - campground    (overnight camping zones)
  - dunes         (dune systems, dune preserves, vegetated sand)
  - lagoon        (wetland/lagoon/estuary edges and trails through them)
  - boardwalk     (boardwalk, promenade, paved path along the beach)

STATUS for each zone (closed list):
  - off_leash         - explicit text says dogs can be off-leash here
  - on_leash          - explicit text says dogs allowed here on leash
  - prohibited        - explicit text says no dogs / dogs not allowed
  - unclear           - zone is mentioned but the dog rule is ambiguous
  - not_applicable    - zone is not mentioned, or this beach has no such zone

Be conservative: only mark off_leash if the source explicitly says off-leash, leash-free, or unleashed for that zone. Do NOT infer off-leash from "dogs allowed" without leash specifics.

Return ONLY a JSON object of this exact shape (no prose, no markdown fencing):

{
  "zones": {
    "sand":        {"status": "...", "evidence": "verbatim quote from source or null"},
    "trails":      {"status": "...", "evidence": "..."},
    "picnic_area": {"status": "...", "evidence": "..."},
    "parking_lot": {"status": "...", "evidence": "..."},
    "campground":  {"status": "...", "evidence": "..."},
    "dunes":       {"status": "...", "evidence": "..."},
    "lagoon":      {"status": "...", "evidence": "..."},
    "boardwalk":   {"status": "...", "evidence": "..."}
  }
}"""


def main():
    conn = psycopg2.connect(**PG)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO public.extraction_prompt_variants
        (field_name, variant_key, prompt_template, expected_shape,
         target_model, active, is_canon, notes)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
      ON CONFLICT (field_name, variant_key) DO UPDATE SET
        prompt_template = EXCLUDED.prompt_template,
        expected_shape  = EXCLUDED.expected_shape,
        target_model    = EXCLUDED.target_model,
        active          = EXCLUDED.active,
        is_canon        = EXCLUDED.is_canon,
        notes           = EXCLUDED.notes
      RETURNING id, field_name, variant_key, expected_shape, target_model, active, is_canon;
    """, (
        "dog_zones",
        "closed_8zones_v1",
        PROMPT,
        "structured_json",
        "claude-sonnet-4-6",
        True,
        True,
        "Per-zone dog policy in 8 closed zones x 5 statuses. Wrapped in zones envelope so parse_response keeps full object. Added 2026-05-01.",
    ))
    print(cur.fetchone())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
