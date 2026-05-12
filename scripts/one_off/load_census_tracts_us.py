"""Download all US census tracts from TIGER 2025 and load into public.census_tracts.

Triggered by: Franz, 2026-05-11 (overnight autonomous run).

Per-state TIGER TRACT shapefiles ship as EPSG:4269 (NAD83). We transform
to 4326 for consistency with the rest of the geospatial layer. Inserts
are batched 500 rows at a time per state, idempotent via ON CONFLICT
on geoid. Re-running picks up where it left off.

Scope: 50 states + DC + 5 territories (PR, VI, GU, AS, MP) = 56 FIPS.
"""
import io, os, sys, time, urllib.parse, urllib.request, zipfile
from pathlib import Path
import psycopg2
import shapefile  # pyshp
from shapely.geometry import shape
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')
POOLER = (ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ['SUPABASE_DB_PASSWORD'],
          dbname=(_p.path or '/postgres').lstrip('/'), sslmode='require')

YEAR = 2025
BASE = f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/TRACT"
UA   = "DogBeachScout/1.0 (franz@franzfunk.com)"
BATCH = 500

# FIPS codes for the 50 states + DC + 5 territories (56 total)
FIPS = [
  "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18",
  "19","20","21","22","23","24","25","26","27","28","29","30","31","32","33",
  "34","35","36","37","38","39","40","41","42","44","45","46","47","48","49",
  "50","51","53","54","55","56",          # 50 states + DC (11)
  "60","66","69","72","78",                # AS, GU, MP, PR, VI
]


def download(url: str, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except Exception as e:
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"download {url} failed after {retries} retries: {last_err}")


def read_state_tracts(zip_bytes: bytes, fips: str):
    """Read .shp + .dbf from the zipped TIGER bundle, yield record dicts
    with attributes + shapely geometry."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    shp_name = f"tl_{YEAR}_{fips}_tract.shp"
    dbf_name = f"tl_{YEAR}_{fips}_tract.dbf"
    shx_name = f"tl_{YEAR}_{fips}_tract.shx"
    # pyshp wants file-like; load into memory
    shp_buf = io.BytesIO(zf.read(shp_name))
    dbf_buf = io.BytesIO(zf.read(dbf_name))
    shx_buf = io.BytesIO(zf.read(shx_name))
    sf = shapefile.Reader(shp=shp_buf, dbf=dbf_buf, shx=shx_buf)
    fields = [f[0] for f in sf.fields[1:]]  # skip DeletionFlag

    for sr in sf.iterShapeRecords():
        attrs = dict(zip(fields, sr.record))
        try:
            geom = shape(sr.shape.__geo_interface__)
        except Exception:
            geom = None
        yield attrs, geom


def upsert_state(fips: str, cur) -> tuple[int, int]:
    """Returns (rows_inserted, rows_skipped)."""
    url = f"{BASE}/tl_{YEAR}_{fips}_tract.zip"
    t0 = time.time()
    print(f"  [{fips}] downloading…", flush=True)
    zb = download(url)
    print(f"  [{fips}] downloaded {len(zb)/1024/1024:.1f} MB in {time.time()-t0:.1f}s", flush=True)

    inserted = skipped = 0
    batch: list[tuple] = []

    def flush():
        nonlocal inserted, skipped
        if not batch:
            return
        # Single executemany INSERT with ST_Transform to 4326
        cur.executemany("""
          insert into public.census_tracts
            (geoid, state_fp, county_fp, tract_ce, name, namelsad,
             funcstat, aland, awater, intpt_lat, intpt_lon, geom, loaded_at)
          values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  st_multi(st_transform(st_setsrid(st_geomfromtext(%s), 4269), 4326)),
                  now())
          on conflict (geoid) do update set
            state_fp   = excluded.state_fp,
            county_fp  = excluded.county_fp,
            tract_ce   = excluded.tract_ce,
            name       = excluded.name,
            namelsad   = excluded.namelsad,
            funcstat   = excluded.funcstat,
            aland      = excluded.aland,
            awater     = excluded.awater,
            intpt_lat  = excluded.intpt_lat,
            intpt_lon  = excluded.intpt_lon,
            geom       = excluded.geom,
            loaded_at  = now()
        """, batch)
        inserted += len(batch)
        batch.clear()

    for attrs, geom in read_state_tracts(zb, fips):
        if geom is None:
            skipped += 1
            continue
        try:
            wkt = geom.wkt
        except Exception:
            skipped += 1
            continue
        # TIGER attribute names are uppercase
        def s(k):
            v = attrs.get(k)
            return None if v in ("", None) else v
        def i(k):
            v = s(k)
            try: return int(v) if v is not None else None
            except (ValueError, TypeError): return None
        def f(k):
            v = s(k)
            try: return float(v) if v is not None else None
            except (ValueError, TypeError): return None

        batch.append((
            s("GEOID"), s("STATEFP"), s("COUNTYFP"), s("TRACTCE"),
            s("NAME"),  s("NAMELSAD"), s("FUNCSTAT"),
            i("ALAND"), i("AWATER"),
            f("INTPTLAT"), f("INTPTLON"),
            wkt,
        ))
        if len(batch) >= BATCH:
            flush()
    flush()
    return inserted, skipped


def main():
    print(f"=== US census-tracts loader (TIGER {YEAR}) — {len(FIPS)} states/territories ===", flush=True)
    t_overall = time.time()
    total_ins = total_skip = total_states = 0
    failed: list[str] = []

    for i, fips in enumerate(FIPS, 1):
        ts = time.time()
        try:
            # New connection per state — keeps autocommit and avoids
            # holding one giant transaction open across 85k inserts.
            with psycopg2.connect(**PG) as c:
                c.autocommit = False
                with c.cursor() as cur:
                    cur.execute("set statement_timeout = '600s'")
                    ins, skp = upsert_state(fips, cur)
                c.commit()
            total_ins += ins
            total_skip += skp
            total_states += 1
            print(f"  [{i:2d}/{len(FIPS)} fips={fips}] inserted={ins:5d}  skipped={skp:2d}  "
                  f"time={time.time()-ts:5.1f}s  running_total={total_ins:6d}", flush=True)
        except Exception as e:
            failed.append(fips)
            print(f"  [{i:2d}/{len(FIPS)} fips={fips}] FAILED after {time.time()-ts:.1f}s: {e}",
                  flush=True)

    print(f"\n=== DONE === states_processed={total_states}/{len(FIPS)}  "
          f"rows={total_ins}  skipped={total_skip}  "
          f"wall={time.time()-t_overall:.1f}s", flush=True)
    if failed:
        print(f"FAILED FIPS (re-run script to retry — idempotent): {','.join(failed)}", flush=True)

    # Final DB tally
    with psycopg2.connect(**PG) as c, c.cursor() as cur:
        cur.execute("""select count(*) total,
                              count(distinct state_fp) states,
                              count(*) filter (where geom is null) null_geom
                         from public.census_tracts""")
        r = cur.fetchone()
        print(f"DB state: total={r[0]}  distinct states={r[1]}  null_geom={r[2]}", flush=True)


if __name__ == "__main__":
    main()
