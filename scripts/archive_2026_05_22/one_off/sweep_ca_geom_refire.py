"""Catch-up sweep: enqueue every CA active beach into
beach_geom_change_queue and chew through it via process_geom_change_queue.

Triggered by: Franz, 2026-05-10. After tightening the geom-change trigger
to drop the 50m floor, several months of curator coord moves (152 via
admin-curate-beach-location, 34 via admin-move-beach-point, 13 via
admin-update-beaches-gold, 12 via admin-confirm-geocode-move) had been
silently dropped. Goal: re-fire BEP cascade so polygon containment,
governance, amenity attribution, etc. reflect the current point.

Chunks of 50, retry on transient errors, idempotent (queue de-dupes on
fid). Safe to re-run.
"""
import os, urllib.parse, time
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

CHUNK = 50
MAX_CHUNKS = 40  # 50 * 40 = 2000 cap, plenty for 746

def connect():
    c = psycopg2.connect(**PG)
    c.autocommit = True
    return c

# Step 1: enqueue every CA active fid (on conflict do nothing so
# re-running this script doesn't reset queued_at for in-flight items)
with connect() as c, c.cursor() as cur:
    cur.execute("set statement_timeout = '120s'")
    cur.execute("""
        insert into public.beach_geom_change_queue (fid)
        select fid from public.beaches_gold
         where is_active and state='CA'
        on conflict (fid) do nothing
    """)
    cur.execute("select count(*) from public.beach_geom_change_queue")
    n_q = cur.fetchone()[0]
    print(f'[enqueue] queue size after insert: {n_q}', flush=True)

# Step 2: drain in chunks
total_processed = 0
total_errors = 0
chunk_idx = 0
t0 = time.time()

while chunk_idx < MAX_CHUNKS:
    chunk_idx += 1
    t_chunk = time.time()
    try:
        with connect() as c, c.cursor() as cur:
            cur.execute("set statement_timeout = '600s'")  # 10 min per chunk
            cur.execute(
                "select fids_processed, errors, queue_remaining "
                "from public.process_geom_change_queue(%s)",
                (CHUNK,),
            )
            row = cur.fetchone()
            fp, errs, remaining = int(row[0]), int(row[1]), int(row[2])
    except Exception as e:
        print(f'[chunk {chunk_idx}] EXCEPTION after '
              f'{time.time()-t_chunk:.1f}s: {e}', flush=True)
        # Brief pause then continue — the queue stores attempts/last_error
        time.sleep(3)
        continue

    total_processed += fp
    total_errors += errs
    elapsed = time.time() - t0
    print(f'[chunk {chunk_idx:3d}] processed={fp:3d}  errors={errs:2d}  '
          f'remaining={remaining:4d}  chunk_time={time.time()-t_chunk:5.1f}s  '
          f'total_processed={total_processed:4d}  elapsed={elapsed:6.1f}s',
          flush=True)
    if remaining == 0 and fp == 0:
        print(f'[done] queue drained.', flush=True)
        break

print(f'[final] processed={total_processed}  errors={total_errors}  '
      f'wall={time.time()-t0:.1f}s', flush=True)

# Step 3: report any fids that errored 3+ times (the queue retains them)
with connect() as c, c.cursor() as cur:
    cur.execute("""
        select fid, attempts, left(last_error, 200) from public.beach_geom_change_queue
         where attempts >= 3 order by attempts desc limit 20
    """)
    rows = cur.fetchall()
    if rows:
        print(f'[stuck] {len(rows)} fids with attempts>=3:')
        for r in rows:
            print(f'  fid={r[0]}  attempts={r[1]}  err={r[2]}')
    else:
        print(f'[stuck] none.')
