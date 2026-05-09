"""bulk_load_tiger_places.py — pre-load TIGER 2024 Places shapefiles for
every state into public.jurisdictions.

Idempotent: skips a state if external_source_status has tiger_places=ok
for that state (or if the shapefile is already present and the state has
>50 places loaded). Status row is recorded after each successful state.

Usage:
  python scripts/one_off/bulk_load_tiger_places.py
  python scripts/one_off/bulk_load_tiger_places.py --states OR,WA
  python scripts/one_off/bulk_load_tiger_places.py --skip-existing
"""
from __future__ import annotations
import argparse, os, subprocess, sys, time, urllib.parse, zipfile
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _external_source_status import record_status

TIGER_DIR = ROOT / 'supabase' / '.temp' / 'tiger'
URL_TEMPLATE = 'https://www2.census.gov/geo/tiger/TIGER2024/PLACE/tl_2024_{fp}_place.zip'

# State code → STATEFP (TIGER FIPS). Excludes territories.
STATE_FIPS = {
    'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09','DE':'10',
    'DC':'11','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17','IN':'18','IA':'19',
    'KS':'20','KY':'21','LA':'22','ME':'23','MD':'24','MA':'25','MI':'26','MN':'27',
    'MS':'28','MO':'29','MT':'30','NE':'31','NV':'32','NH':'33','NJ':'34','NM':'35',
    'NY':'36','NC':'37','ND':'38','OH':'39','OK':'40','OR':'41','PA':'42','RI':'44',
    'SC':'45','SD':'46','TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53',
    'WV':'54','WI':'55','WY':'56',
}


def log(m: str):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def already_loaded(state: str) -> bool:
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""select 1 from public.external_source_status
                            where source='tiger_places' and state=%s and status='ok'""", (state,))
            return cur.fetchone() is not None


def jurisdictions_count(state: str) -> int:
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("select count(*) from public.jurisdictions where state=%s", (state,))
            return cur.fetchone()[0]


def download_and_extract(fp: str) -> Path | None:
    shp = TIGER_DIR / f'tl_2024_{fp}_place.shp'
    if shp.exists():
        return shp.with_suffix('')
    zip_path = TIGER_DIR / f'tl_2024_{fp}_place.zip'
    url = URL_TEMPLATE.format(fp=fp)
    log(f'    downloading {url}')
    rc = subprocess.run(
        ['curl', '--ssl-no-revoke', '-sSL', '-o', str(zip_path), url],
        capture_output=True
    )
    if rc.returncode != 0 or not zip_path.exists() or zip_path.stat().st_size < 10000:
        log(f'    download failed (rc={rc.returncode})')
        return None
    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(TIGER_DIR)
    except zipfile.BadZipFile:
        log(f'    bad zipfile')
        return None
    if not shp.exists():
        log(f'    shapefile missing after extract')
        return None
    return shp.with_suffix('')


def load_one_state(state: str) -> tuple[bool, int, str]:
    fp = STATE_FIPS.get(state)
    if not fp:
        return False, 0, f'unknown state code: {state}'

    shp_base = download_and_extract(fp)
    if shp_base is None:
        return False, 0, 'download/extract failed'

    log(f'    applying load_places_shapefile.py for {state} (FP={fp})')
    rc = subprocess.run(
        [sys.executable, str(ROOT / 'scripts' / 'load_places_shapefile.py'),
         str(shp_base)],
        capture_output=True, text=True, cwd=str(ROOT)
    )
    if rc.returncode != 0:
        return False, 0, f'loader rc={rc.returncode}: {(rc.stderr or "")[-200:]}'

    n = jurisdictions_count(state)
    return True, n, 'ok'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', help='Comma-sep state-code list (default: all)')
    ap.add_argument('--skip-existing', action='store_true',
                    help='Skip if status=ok in external_source_status')
    args = ap.parse_args()

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    else:
        states = sorted(STATE_FIPS.keys())

    log(f'Plan: load TIGER Places for {len(states)} states')
    log(f'States: {", ".join(states)}')

    TIGER_DIR.mkdir(parents=True, exist_ok=True)
    loaded = skipped = failed = 0

    for state in states:
        if args.skip_existing and already_loaded(state):
            log(f'{state}: SKIP (status=ok)')
            skipped += 1
            continue
        existing = jurisdictions_count(state)
        if existing > 50:
            log(f'{state}: SKIP ({existing} jurisdictions already)')
            try:
                record_status(PG, 'tiger_places', state, 'ok', existing,
                              'pre-existing >50 rows')
            except Exception:
                pass
            skipped += 1
            continue

        log(f'{state}: loading')
        t0 = time.time()
        ok, n, msg = load_one_state(state)
        elapsed = time.time() - t0
        if ok:
            log(f'  {state}: loaded {n} jurisdictions in {elapsed:.0f}s')
            try:
                record_status(PG, 'tiger_places', state, 'ok', n,
                              f'TIGER 2024 Places, {elapsed:.0f}s')
            except Exception as e:
                log(f'    (status write failed: {e})')
            loaded += 1
        else:
            log(f'  {state}: FAILED — {msg}')
            try:
                record_status(PG, 'tiger_places', state, 'failed', None, msg[:500])
            except Exception:
                pass
            failed += 1
        time.sleep(2)

    log(f'Done. loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
