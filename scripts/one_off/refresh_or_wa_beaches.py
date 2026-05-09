"""refresh_or_wa_beaches.py — invoke daily-beach-refresh for OR+WA scoreable
fids in batches. Bypasses the rotated-secret pg_net trigger by calling the
Edge Function directly with the env's current ADMIN_SECRET.
"""
from __future__ import annotations
import os, time, urllib.parse, json
from pathlib import Path
import httpx
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

URL    = os.environ['SUPABASE_URL'].rstrip('/') + '/functions/v1/daily-beach-refresh'
KEY    = os.environ['SUPABASE_SERVICE_KEY']
SECRET = os.environ['ADMIN_SECRET']
HEADERS = {
    'Authorization':  f'Bearer {KEY}',
    'apikey':         KEY,
    'x-admin-secret': SECRET,
    'Content-Type':   'application/json',
}

BATCH = 25


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                select g.location_id from public.beaches_gold g
                 where g.is_active and g.is_scoreable and g.state in ('OR','WA')
                 order by g.state, g.fid
            """)
            ids = [r['location_id'] for r in cur.fetchall()]
    log(f'Refreshing {len(ids)} OR/WA scoreable beaches in batches of {BATCH}')

    ok, fail = 0, 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        t0 = time.time()
        try:
            r = httpx.post(URL, headers=HEADERS,
                           json={'location_ids': batch, 'tide_window_days': 7},
                           timeout=300.0)
            elapsed = time.time() - t0
            if r.is_success:
                ok += len(batch)
                log(f'  batch {i//BATCH+1}: {len(batch)} ids OK in {elapsed:.0f}s')
            else:
                fail += len(batch)
                log(f'  batch {i//BATCH+1}: HTTP {r.status_code} — {r.text[:200]}')
        except Exception as e:
            fail += len(batch)
            log(f'  batch {i//BATCH+1}: EXC {e}')
        time.sleep(2)  # be polite

    log(f'Done. ok={ok} fail={fail}')


if __name__ == '__main__':
    main()
