"""Process ONE chunk of active scoreable beaches through the PAD-US
multi-state PIP pipeline. Exits 0 if more remain, 2 if done, 1 on fatal.

Designed per feedback_chunked_subprocess.md — bounded per-chunk
timeout, idempotency via populate_pad_us_containment_gold's
delete-before-insert, restart cleanly on failure.

Orchestrate via bash:
  while true; do
    .venv-pipeline/Scripts/python.exe scripts/one_off/pad_us_chunk.py || break
    [ $? -eq 2 ] && break
  done

Or:
  until ! .venv-pipeline/Scripts/python.exe scripts/one_off/pad_us_chunk.py; do :; done
"""
from __future__ import annotations
import os
import sys
import time
import urllib.parse
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

CHUNK_SIZE = int(os.environ.get("PAD_US_CHUNK_SIZE", "100"))
PER_FID_TIMEOUT_S = 60

# A fid is "processed" once it has a canonical governance row from ANY
# source. Earlier version filtered on "has pad_us polygon_containment row"
# which infinite-looped on beaches OUTSIDE PAD-US coverage (those never
# get a containment row written — function exits cleanly with 0 inserts,
# and the chunk-picker re-selects them next iteration).
#
# Switching to "has canonical governance" means each chunk processes 100
# truly-unattributed beaches per pass: pad_us first (where applicable),
# city/county fallback otherwise.
SQL_PICK_CHUNK = """
  select g.fid, g.state
    from public.beaches_gold g
   where g.is_active and g.is_scoreable
     and not exists (
       select 1 from public.beach_enrichment_provenance b
        where b.gold_fid = g.fid
          and b.field_group = 'governance'
          and b.is_canonical = true
          and b.claimed_values->>'name' is not null
     )
   order by g.state, g.fid
   limit %s
"""


def main() -> int:
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute(f"set statement_timeout = '{PER_FID_TIMEOUT_S}s'")

    cur.execute(SQL_PICK_CHUNK, (CHUNK_SIZE,))
    fids = cur.fetchall()
    if not fids:
        print("All active scoreable beaches have PAD-US containment evidence. Done.")
        return 2

    print(f"chunk: {len(fids)} fids (states: {set(s for _, s in fids)})", flush=True)
    t0 = time.time()
    ok = err = 0
    for fid, state in fids:
        try:
            cur.execute("select populate_pad_us_containment_gold(%s, 100)", (fid,))
            cur.execute("select _resolve_polygon_containment(%s)", (fid,))
            cur.execute("select populate_governance_from_polygon_gold(%s)", (fid,))
            cur.execute("select _resolve_governance_gold(%s)", (fid,))
            ok += 1
            conn.commit()
        except Exception as e:
            err += 1
            print(f"  fail fid={fid} ({state}): {str(e)[:140]}", flush=True)
            conn.rollback()
    elapsed = time.time() - t0
    print(f"chunk done: ok={ok} err={err} in {elapsed:.0f}s "
          f"({len(fids)/elapsed:.1f} fid/s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
