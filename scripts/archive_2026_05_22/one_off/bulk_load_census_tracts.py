"""bulk_load_census_tracts.py — load Census Cartographic Boundary tract
polygons (national, single download) + ACS 2023 5-year population per
state into public.census_tracts.

Geometry: one-time national load from CB 500k shapefile (~150 MB,
simplified polygons; plenty of detail for radius queries).
  Source: https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_tract_500k.zip

Population: per-state ACS 5yr API call for B01003_001E (total
population), batched into update_census_tract_population.

Idempotent:
  - Geometry load skips if census_tracts already has >=80K rows
  - Per-state pop skips if external_source_status has
    census_tracts=ok for that state, OR the state already has
    population set on >=90%% of its tracts.

ACS API key (free, 1-min signup at https://api.census.gov/data/key_signup.html)
is recommended but not required for ~50 state-scoped queries. Set
CENSUS_API_KEY in scripts/pipeline/.env to use one.

Usage:
  python scripts/one_off/bulk_load_census_tracts.py
  python scripts/one_off/bulk_load_census_tracts.py --states CA,OR,WA
  python scripts/one_off/bulk_load_census_tracts.py --skip-existing
  python scripts/one_off/bulk_load_census_tracts.py --geom-only       (no ACS)
  python scripts/one_off/bulk_load_census_tracts.py --pop-only        (no shapefile)
"""
from __future__ import annotations
import argparse, json, os, subprocess, sys, time, urllib.parse, urllib.request, zipfile
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _external_source_status import record_status

TIGER_DIR = ROOT / 'supabase' / '.temp' / 'tiger'
# Cartographic Boundary 500k — single national tract file, simplified
# polygons. ~150 MB extracted. Plenty of detail for radius queries.
CB_URL = 'https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_tract_500k.zip'
CB_SHP_BASENAME = 'cb_2023_us_tract_500k'
ACS_VINTAGE = 2023
ACS_URL = ('https://api.census.gov/data/{year}/acs/acs5'
           '?get=B01003_001E&for=tract:*&in=state:{fp}')
CENSUS_API_KEY = os.environ.get('CENSUS_API_KEY', '').strip()

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
                            where source='census_tracts' and state=%s and status='ok'""",
                        (state,))
            return cur.fetchone() is not None


def state_load_status(state: str) -> tuple[int, int]:
    """Return (total_tracts, with_population) for a state."""
    fp = STATE_FIPS.get(state, '')
    if not fp:
        return 0, 0
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""select count(*), count(*) filter (where population is not null)
                             from public.census_tracts where state_fp=%s""", (fp,))
            r = cur.fetchone()
            return int(r[0]), int(r[1])


def download_and_extract_national() -> Path | None:
    """One-shot download of the national CB tract shapefile."""
    shp = TIGER_DIR / f'{CB_SHP_BASENAME}.shp'
    if shp.exists():
        return shp.with_suffix('')
    zip_path = TIGER_DIR / f'{CB_SHP_BASENAME}.zip'
    log(f'    downloading {CB_URL} (~150 MB)')
    rc = subprocess.run(
        ['curl', '--ssl-no-revoke', '-sSL', '-o', str(zip_path), CB_URL],
        capture_output=True
    )
    if rc.returncode != 0 or not zip_path.exists() or zip_path.stat().st_size < 10_000_000:
        log(f'    download failed (rc={rc.returncode})')
        return None
    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(TIGER_DIR)
    except zipfile.BadZipFile:
        log('    bad zipfile')
        return None
    if not shp.exists():
        log('    shapefile missing after extract')
        return None
    return shp.with_suffix('')


def total_tracts_loaded() -> int:
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("select count(*) from public.census_tracts")
            return int(cur.fetchone()[0])


def load_geometry_national(force: bool = False) -> tuple[bool, str]:
    """Load the national CB tract shapefile in one shot. Skips if
    census_tracts already has >=80K rows (full national load)."""
    n = total_tracts_loaded()
    if n >= 80_000 and not force:
        log(f'    census_tracts already has {n} rows; skip geometry load')
        return True, 'skipped (already loaded)'
    shp_base = download_and_extract_national()
    if shp_base is None:
        return False, 'download/extract failed'
    log(f'    applying load_census_tracts_shapefile.py to national CB')
    rc = subprocess.run(
        [sys.executable, str(ROOT / 'scripts' / 'load_census_tracts_shapefile.py'),
         str(shp_base)],
        cwd=str(ROOT)
    )
    if rc.returncode != 0:
        return False, f'shapefile loader rc={rc.returncode}'
    return True, 'ok'


def fetch_acs_population(fp: str) -> list[dict] | None:
    """Hit ACS API for total population per tract in this state.
    Returns list of {geoid, population} or None on failure.
    Response shape: [["B01003_001E","state","county","tract"],
                     ["12345","06","001","400100"], ...]
    """
    url = ACS_URL.format(year=ACS_VINTAGE, fp=fp)
    if CENSUS_API_KEY:
        url += f'&key={CENSUS_API_KEY}'
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            data = json.loads(r.read())
    except Exception as e:
        log(f'    ACS API failed: {e}')
        return None
    if not data or len(data) < 2:
        return []
    header = data[0]
    rows = data[1:]
    try:
        i_pop    = header.index('B01003_001E')
        i_state  = header.index('state')
        i_county = header.index('county')
        i_tract  = header.index('tract')
    except ValueError:
        log(f'    ACS API: unexpected header {header}')
        return None
    out = []
    for row in rows:
        geoid = f'{row[i_state]}{row[i_county]}{row[i_tract]}'
        pop = row[i_pop]
        out.append({'geoid': geoid, 'population': pop if pop and pop != '-' else None})
    return out


def update_population(state: str, fp: str) -> tuple[bool, int, str]:
    rows = fetch_acs_population(fp)
    if rows is None:
        return False, 0, 'ACS API fetch failed'
    if not rows:
        return True, 0, 'no ACS rows returned (state has no tracts?)'

    log(f'    pushing {len(rows)} population rows to DB...')
    n_updated = 0
    BATCH = 500
    with psycopg2.connect(**PG) as c:
        c.autocommit = True
        with c.cursor() as cur:
            for i in range(0, len(rows), BATCH):
                batch = rows[i:i + BATCH]
                cur.execute(
                    "select public.update_census_tract_population(%s::jsonb, %s)",
                    (json.dumps(batch), ACS_VINTAGE)
                )
                n_updated += int(cur.fetchone()[0])
    return True, n_updated, 'ok'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--states', help='Comma-sep state-code list (default: all)')
    ap.add_argument('--skip-existing', action='store_true',
                    help='Skip pop pull if external_source_status has ok for state')
    ap.add_argument('--geom-only', action='store_true',
                    help='Load national CB tract shapefile only; skip ACS')
    ap.add_argument('--pop-only', action='store_true',
                    help='Skip national CB shapefile load; only refresh population')
    ap.add_argument('--force-geom', action='store_true',
                    help='Re-load national CB shapefile even if already present')
    args = ap.parse_args()

    if args.geom_only and args.pop_only:
        print('--geom-only and --pop-only are mutually exclusive', file=sys.stderr)
        sys.exit(2)

    if args.states:
        states = [s.strip().upper() for s in args.states.split(',') if s.strip()]
    else:
        states = sorted(STATE_FIPS.keys())

    log(f'Plan: census tracts (CB national geom + per-state ACS pop)')
    log(f'States (for pop): {", ".join(states)}')
    log(f'Mode: {"geom-only" if args.geom_only else "pop-only" if args.pop_only else "geom+pop"}')
    log(f'ACS API key: {"present" if CENSUS_API_KEY else "missing (using anonymous calls)"}')

    TIGER_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1 — national geometry (one-time)
    if not args.pop_only:
        log('Step 1: national CB tract geometry')
        t0 = time.time()
        ok, msg = load_geometry_national(force=args.force_geom)
        if not ok:
            log(f'  geometry FAILED — {msg}')
            sys.exit(1)
        log(f'  geometry {msg} ({time.time()-t0:.0f}s)')

    if args.geom_only:
        log('Done (geom-only).')
        return

    # Step 2 — per-state ACS population
    log('Step 2: per-state ACS population')
    loaded = skipped = failed = 0
    for state in states:
        if args.skip_existing and already_loaded(state):
            log(f'{state}: SKIP (status=ok)')
            skipped += 1
            continue
        total, with_pop = state_load_status(state)
        if total > 0 and with_pop / total >= 0.90:
            log(f'{state}: SKIP ({with_pop}/{total} tracts already have pop)')
            try:
                record_status(PG, 'census_tracts', state, 'ok', total,
                              f'pre-existing: {with_pop}/{total} with population')
            except Exception:
                pass
            skipped += 1
            continue

        fp = STATE_FIPS.get(state)
        if not fp:
            log(f'{state}: unknown state code; skip')
            failed += 1
            continue

        log(f'{state}: ACS pop for FP={fp}')
        t0 = time.time()
        ok, n_pop, msg = update_population(state, fp)
        elapsed = time.time() - t0
        if ok:
            log(f'  {state}: ok ({n_pop} tracts in {elapsed:.0f}s)')
            try:
                record_status(PG, 'census_tracts', state, 'ok', n_pop,
                              f'CB 2023 + ACS {ACS_VINTAGE}, {elapsed:.0f}s')
            except Exception as e:
                log(f'    (status write failed: {e})')
            loaded += 1
        else:
            log(f'  {state}: FAILED — {msg}')
            try:
                record_status(PG, 'census_tracts', state, 'failed', None, msg[:500])
            except Exception:
                pass
            failed += 1
        time.sleep(1)

    log(f'Done. pop-loaded={loaded} skipped={skipped} failed={failed}')


if __name__ == '__main__':
    main()
