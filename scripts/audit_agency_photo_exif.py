"""audit_agency_photo_exif.py — sample existing agency-gallery photos and
report EXIF GPS hit rate.

Tells us empirically what fraction of CDPR/WSPRC/NPS/CCC photos carry
true per-photo coordinates. This determines whether building OPRD (and
backfilling the others) with EXIF parsing would meaningfully reduce
the multi-beach-fanout problem, or whether we need vision-driven
re-attribution instead.

NO DB WRITES. Read-only sample + EXIF extract + report.

Usage:
  python scripts/audit_agency_photo_exif.py                    # all 4 sources, 100/source
  python scripts/audit_agency_photo_exif.py --per-source 50    # smaller sample
  python scripts/audit_agency_photo_exif.py --sources wsprc    # one source
"""
from __future__ import annotations
import sys, os, argparse, urllib.parse, time, io
sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
import truststore; truststore.inject_into_ssl()
from pathlib import Path
from collections import defaultdict
import httpx
import psycopg2
from dotenv import load_dotenv
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / 'scripts' / 'pipeline' / '.env')

UA = 'dog-beach-EXIF-audit/1.0 (franz@franzfunk.com)'


def _connect():
    pp = urllib.parse.urlparse((ROOT / 'supabase' / '.temp' / 'pooler-url').read_text().strip())
    return psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ['SUPABASE_DB_PASSWORD'],
        dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')


def pick_sample(cur, source: str, n: int) -> list[tuple[int, str, str]]:
    """Random sample of (beach_fid, image_url, external_id) for the source."""
    cur.execute("""
      SELECT arena_group_id, image_url, external_id
        FROM beach_photos
       WHERE source = %s AND image_url IS NOT NULL
       ORDER BY random()
       LIMIT %s
    """, (source, n))
    return cur.fetchall()


def _gps_to_decimal(coord, ref: str) -> float | None:
    """Convert ((d, m, s)) + ref ('N'/'S'/'E'/'W') to decimal degrees."""
    try:
        d, m, s = coord
        dec = float(d) + float(m) / 60 + float(s) / 3600
        if ref in ('S', 'W'):
            dec = -dec
        return dec
    except Exception:
        return None


def extract_gps(image_bytes: bytes) -> tuple[float | None, float | None]:
    """Read EXIF GPS from image bytes. Returns (lat, lon) or (None, None)."""
    try:
        img = Image.open(io.BytesIO(image_bytes))
        exif = img._getexif()
        if not exif:
            return None, None
        gps_info = {}
        for tag_id, value in exif.items():
            tag = TAGS.get(tag_id, tag_id)
            if tag == 'GPSInfo':
                for gps_id, gps_value in value.items():
                    gps_tag = GPSTAGS.get(gps_id, gps_id)
                    gps_info[gps_tag] = gps_value
                break
        lat_raw = gps_info.get('GPSLatitude')
        lat_ref = gps_info.get('GPSLatitudeRef')
        lon_raw = gps_info.get('GPSLongitude')
        lon_ref = gps_info.get('GPSLongitudeRef')
        if not (lat_raw and lat_ref and lon_raw and lon_ref):
            return None, None
        return _gps_to_decimal(lat_raw, lat_ref), _gps_to_decimal(lon_raw, lon_ref)
    except Exception:
        return None, None


def fetch_image(client: httpx.Client, url: str) -> bytes | None:
    """GET image bytes; return None on failure."""
    try:
        r = client.get(url, timeout=20.0, follow_redirects=True)
        if r.status_code != 200:
            return None
        return r.content
    except Exception:
        return None


def beach_coords(cur, fid: int) -> tuple[float | None, float | None]:
    """Get beach lat/lon for distance reference."""
    cur.execute("SELECT ST_Y(geom::geometry), ST_X(geom::geometry) "
                " FROM beaches_gold WHERE fid = %s", (fid,))
    r = cur.fetchone()
    return (r[0], r[1]) if r else (None, None)


def haversine_m(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    R = 6371000.0
    dlat = radians(lat2 - lat1); dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--per-source', type=int, default=100)
    ap.add_argument('--sources', default='wsprc,cdpr,nps,ccc',
                     help='comma-separated subset')
    ap.add_argument('--pace', type=float, default=0.25,
                     help='seconds between fetches (politeness)')
    args = ap.parse_args()

    sources = [s.strip() for s in args.sources.split(',') if s.strip()]
    conn = _connect()
    cur = conn.cursor()

    headers = {'User-Agent': UA}
    client = httpx.Client(headers=headers)

    results: dict[str, dict] = {}
    for src in sources:
        print(f'\n=== {src.upper()} ===', flush=True)
        sample = pick_sample(cur, src, args.per_source)
        if not sample:
            print(f'  no photos in beach_photos for source={src}')
            continue
        print(f'  sample size: {len(sample)}', flush=True)

        n_with_gps = 0
        n_fetch_fail = 0
        distances_m = []
        for i, (fid, url, ext_id) in enumerate(sample, 1):
            data = fetch_image(client, url)
            if data is None:
                n_fetch_fail += 1
                continue
            lat, lon = extract_gps(data)
            if lat is not None and lon is not None:
                n_with_gps += 1
                # measure distance to beach centroid for sanity
                b_lat, b_lon = beach_coords(cur, fid)
                if b_lat is not None:
                    distances_m.append(haversine_m(lat, lon, b_lat, b_lon))
            if i % 10 == 0:
                print(f'    {i}/{len(sample)}  gps_hits={n_with_gps}  '
                      f'fetch_fail={n_fetch_fail}', flush=True)
            time.sleep(args.pace)

        gps_rate = n_with_gps / max(1, len(sample) - n_fetch_fail)
        results[src] = {
            'sampled': len(sample),
            'fetch_fail': n_fetch_fail,
            'with_gps': n_with_gps,
            'gps_rate': gps_rate,
            'distances_m': distances_m,
        }

    print('\n\n========== SUMMARY ==========')
    print(f'{"source":<8} {"sampled":<8} {"fetched":<8} {"gps_hit":<8} '
          f'{"gps_rate":<10} {"med_dist_m":<12} {"max_dist_m":<10}')
    for src, r in results.items():
        fetched = r['sampled'] - r['fetch_fail']
        ds = sorted(r['distances_m'])
        med = ds[len(ds)//2] if ds else None
        mx = ds[-1] if ds else None
        med_s = f'{med:.0f}' if med is not None else '-'
        mx_s = f'{mx:.0f}' if mx is not None else '-'
        print(f'{src:<8} {r["sampled"]:<8} {fetched:<8} {r["with_gps"]:<8} '
              f'{r["gps_rate"]:<10.0%} {med_s:<12} {mx_s:<10}')

    print('\n# Interpretation:')
    print('#  gps_rate >= 50%   → EXIF-based attribution is worth building')
    print('#  med_dist_m < 500  → EXIF coords are accurate (photo IS at beach)')
    print('#  med_dist_m > 5000 → EXIF coords are noise (camera location, not subject)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
