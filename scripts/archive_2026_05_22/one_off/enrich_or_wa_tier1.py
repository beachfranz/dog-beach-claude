"""enrich_or_wa_tier1.py — run structural enrichment passes on OR/WA Location
Tiers 1 + 2 (1_off-leash + 2_on-leash).

Per fid, runs:
  1. refire_bep_cascade(fid)  — wired populators + resolvers, including
     park_name (via populate_from_pad_us_gold) and c1_jurisdiction_id
     (via _resolve_polygon_containment, now that places are loaded for
     OR + WA).
  2. _emit_evidence_from_osm_amenities(fid)  — NOT in refire_bep_cascade
     (wiring gap noted, will fix in pipeline spec). Emits OSM amenity
     evidence to BEP.
  3. _resolve_practical_gold(fid) + promote_canonical_to_consumer_tables(fid)
     — roll new amenity evidence into beach_amenities.

Reports before/after coverage for the 5 columns we care about.
"""
from __future__ import annotations
import os, sys, time, urllib.parse
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

TIER_FILTER = """
public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash, bdp.dogs_prohibited_start::text)
"""


_conn = None


def _get_conn():
    """Persistent connection with reconnect-on-drop. Connection-per-call
    in a tight loop overwhelmed the pooler — keep one open and retry
    transient drops."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(**PG)
        _conn.autocommit = True
    return _conn


def q(sql, args=None, fetch=True):
    global _conn
    for attempt in range(3):
        try:
            c = _get_conn()
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, args)
                if fetch and cur.description:
                    return cur.fetchall()
                return None
        except psycopg2.OperationalError:
            try:
                if _conn is not None:
                    _conn.close()
            except Exception:
                pass
            _conn = None
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def coverage(state: str) -> dict:
    rs = q(f"""
    select
      count(*) total,
      count(*) filter (where g.address_city is not null)        addr,
      count(*) filter (where g.county_fips is not null)         cfips,
      count(*) filter (where g.park_name is not null)           park,
      count(*) filter (where g.c1_jurisdiction_id is not null)  c1,
      count(*) filter (where g.name_source is not null)         nsrc,
      count(*) filter (where ba.arena_group_id is not null)     amen
    from public.beaches_gold g
    left join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
    left join public.beach_amenities ba on ba.arena_group_id=g.fid
    where g.is_active and g.state=%s
      and ({TIER_FILTER}) in ('1_off-leash','2_on-leash')
    """, (state,))
    return dict(rs[0])


def get_tier1_fids(state: str) -> list[int]:
    rs = q(f"""
    select g.fid
    from public.beaches_gold g
    join public.beach_dog_policy bdp on bdp.arena_group_id=g.fid
    where g.is_active and g.state=%s
      and ({TIER_FILTER}) in ('1_off-leash','2_on-leash')
    order by g.fid
    """, (state,))
    return [r['fid'] for r in rs]


def log(m: str):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    states = sys.argv[1:] or ['OR', 'WA']

    # Baseline coverage
    log("Baseline coverage (1+1b+1c only):")
    base = {}
    for st in states:
        c = coverage(st)
        base[st] = c
        log(f"  {st}: tot={c['total']} addr={c['addr']} cfips={c['cfips']} park={c['park']} c1={c['c1']} nsrc={c['nsrc']} amen={c['amen']}")

    # Run refire_bep_cascade in chunks of 25 for each state
    for st in states:
        fids = get_tier1_fids(st)
        log(f"{st}: {len(fids)} tier-1/1b/1c fids to enrich")
        for i in range(0, len(fids), 25):
            chunk = fids[i:i+25]
            t0 = time.time()
            r = q("select * from public.refire_bep_cascade(%s)", (chunk,))
            elapsed = time.time() - t0
            log(f"  refire_bep_cascade chunk {i//25+1}: {len(chunk)} fids in {elapsed:.0f}s — {r[0]}")

        # OSM amenities emitter — not in refire, must fire per fid
        log(f"{st}: firing _emit_evidence_from_osm_amenities for {len(fids)} fids")
        t0 = time.time()
        emitted = 0
        for fid in fids:
            r = q("select * from public._emit_evidence_from_osm_amenities(%s)", (fid,))
            if r and (r[0]['rows_inserted'] or r[0]['rows_updated']):
                emitted += 1
        log(f"  emitted OSM amenities for {emitted}/{len(fids)} fids in {time.time()-t0:.0f}s")

        # Re-resolve practical + roll forward to beach_amenities
        log(f"{st}: re-resolving practical + promoting to beach_amenities")
        t0 = time.time()
        for fid in fids:
            q("select public._resolve_practical_gold(%s)", (fid,), fetch=False)
            q("select public.promote_canonical_to_consumer_tables(%s)", (fid,), fetch=False)
        log(f"  done in {time.time()-t0:.0f}s")

    log("After enrichment coverage:")
    for st in states:
        c = coverage(st)
        delta = lambda k: c[k] - base[st][k]
        log(f"  {st}: tot={c['total']} addr={c['addr']} (+{delta('addr')}) cfips={c['cfips']} (+{delta('cfips')}) park={c['park']} (+{delta('park')}) c1={c['c1']} (+{delta('c1')}) nsrc={c['nsrc']} (+{delta('nsrc')}) amen={c['amen']} (+{delta('amen')})")


if __name__ == '__main__':
    main()
