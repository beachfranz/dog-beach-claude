"""insert_auto_extracted_polygon.py — re-run sand extraction for one beach,
georeference the contour, and INSERT into public.beach_polygons_auto_extracted
as a pending review row.

Idempotent on (fid, algorithm_version): if a pending row exists for the same
(fid, algorithm_version) it is replaced (delete + insert) so re-running with
the same algorithm doesn't accumulate duplicates. Approved rows are NEVER
overwritten.

Usage:
  python scripts/insert_auto_extracted_polygon.py seal     # Seal Beach OC
  python scripts/insert_auto_extracted_polygon.py <fid>    # any fid (looks up centroid)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import dotenv
import numpy as np
from PIL import Image
from skimage import measure, morphology

import extract_hybrid as eh

ALGO_VERSION_TOPO  = 'sand_v2_z16_anchor_grow'   # topo basemap v2
ALGO_VERSION_SAT   = 'sat_v1_z17_anchor'         # satellite v1
DEFAULT_ZOOM_TOPO  = 16
DEFAULT_ZOOM_SAT   = 17
DEFAULT_SIZE_TOPO  = 1280
DEFAULT_SIZE_SAT   = 2048
EDGE_BUFFER_PX = 16
MAX_GROW_PASSES = 2

# Default unless --source is overridden
ALGO_VERSION = ALGO_VERSION_TOPO
DEFAULT_ZOOM = DEFAULT_ZOOM_TOPO
DEFAULT_SIZE = DEFAULT_SIZE_TOPO


def supabase_query(sql: str) -> dict:
    """Run an SQL query via the local supabase CLI; return the {rows, ...} dict.

    Uses -f tmpfile rather than passing SQL on the command line to dodge
    the Windows CreateProcess 32K argv limit (high-vertex WKT polygons +
    OSM MultiPolygon GeoJSON exceed it)."""
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False,
                                     encoding='utf-8') as f:
        f.write(sql)
        tmp = f.name
    try:
        out = subprocess.check_output(
            ['supabase', 'db', 'query', '--linked', '-f', tmp],
            text=True,
        )
    finally:
        os.unlink(tmp)
    idx = out.find('{')
    end = out.rfind('}')
    return json.loads(out[idx:end+1])


def fetch_beach(fid: int) -> dict:
    rows = supabase_query(f"""
        SELECT bg.fid, bg.name, bg.lat, bg.lon, bg.county_name
          FROM public.beaches_gold bg
         WHERE bg.fid = {fid} AND bg.is_active
    """)['rows']
    if not rows:
        raise RuntimeError(f'fid {fid} not found in beaches_gold')
    return rows[0]


def fetch_osm_polygon(fid: int) -> dict | None:
    """Return GeoJSON of the OSM beach polygon intersecting this fid, or None."""
    rows = supabase_query(f"""
        SELECT ST_AsGeoJSON(op.geom) AS g
          FROM public.osm_beach_polys op
          JOIN public.beaches_gold bg ON ST_Intersects(op.geom, bg.geom)
         WHERE bg.fid = {fid} AND bg.is_active
         LIMIT 1
    """)['rows']
    if not rows:
        return None
    return json.loads(rows[0]['g'])


def compute_iou_pg(wkt_extracted: str, osm_geojson: dict | None) -> float | None:
    """Compute intersection-over-union with the OSM polygon via PostGIS."""
    if osm_geojson is None:
        return None
    geom_a = f"ST_GeomFromText('{wkt_extracted}', 4326)"
    geom_b = f"ST_GeomFromGeoJSON('{json.dumps(osm_geojson)}')"
    rows = supabase_query(f"""
        SELECT ST_Area(ST_Intersection({geom_a}, {geom_b})::geography)
             / NULLIF(ST_Area(ST_Union({geom_a}, {geom_b})::geography), 0) AS iou
    """)['rows']
    return float(rows[0]['iou']) if rows[0]['iou'] is not None else None


def _touches_edge(mask: np.ndarray, buf: int) -> bool:
    """True if any True pixel in `mask` lies within `buf` of any edge."""
    h, w = mask.shape
    return (mask[:buf, :].any() or mask[-buf:, :].any()
            or mask[:, :buf].any() or mask[:, -buf:].any())


def contour_to_wkt(contour: np.ndarray, T: dict) -> str:
    """Convert a pixel-space contour (Nx2 row,col array) to WKT POLYGON in lat/lon."""
    coords = []
    for r, c in contour:
        lon, lat = eh.pixel_to_lonlat(float(c), float(r), T)
        coords.append((lon, lat))
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    coord_str = ', '.join(f'{lon:.7f} {lat:.7f}' for lon, lat in coords)
    return f'POLYGON(({coord_str}))'


def main(target: str, source: str = 'topo'):
    """source: 'topo' (Esri World_Topo z=16) or 'satellite' (Esri World_Imagery z=17)."""
    # Resolve target → fid
    if target.lower() == 'seal':
        fid = 8270
    elif target.isdigit():
        fid = int(target)
    else:
        raise SystemExit(f'unknown target: {target}')

    # Pick stitcher + sand detector + algorithm version per source
    if source == 'satellite':
        import satellite_ab as sat
        stitch_fn   = sat.stitch_imagery
        sand_fn     = sat.detect_sand_imagery
        clean_fn    = sat.clean
        algo_ver    = ALGO_VERSION_SAT
        zoom        = DEFAULT_ZOOM_SAT
        size        = DEFAULT_SIZE_SAT
        cache       = Path('tmp/esri_imagery_cache')
        basemap_src = 'esri_world_imagery'
        max_passes  = 0   # 2048px @ z=17 ≈ 2km — plenty; growing tends to
                          # connect neighbor beaches into one giant blob
    else:
        stitch_fn   = eh.stitch
        sand_fn     = eh.detect_sand
        clean_fn    = lambda m: eh.clean_mask(m, min_size=800)
        algo_ver    = ALGO_VERSION_TOPO
        zoom        = DEFAULT_ZOOM_TOPO
        size        = DEFAULT_SIZE_TOPO
        cache       = Path('tmp/esri_tile_cache')
        basemap_src = 'esri_world_topo'
        max_passes  = MAX_GROW_PASSES

    beach = fetch_beach(fid)
    print(f'Beach: fid={beach["fid"]}  {beach["name"]} ({beach["county_name"]})  '
          f'lat={beach["lat"]} lon={beach["lon"]}')

    # ---- Pass 1+: stitch, detect, anchor on catalog pt; if blob hits edge, grow ----
    target_mask = None
    T = None
    rgb = None
    for grow_pass in range(max_passes + 1):
        rgb, T = stitch_fn(beach['lon'], beach['lat'], zoom, size, size, cache)
        sand_raw = sand_fn(rgb)
        sand = clean_fn(sand_raw)
        print(f'  pass {grow_pass}  size={size}px  sand={sand.sum():,} px (raw {sand_raw.sum():,})')

        if not sand.any():
            if grow_pass < MAX_GROW_PASSES:
                size *= 2
                continue
            raise SystemExit('no sand detected at any canvas size')

        labeled = measure.label(sand)

        # Anchor on catalog point: prefer the blob containing the lat/lon,
        # else the nearest blob centroid. (Fixes "polygon does not contain
        # catalog point" — the 7-of-13 rejection theme from first batch.)
        px, py = eh.lonlat_to_pixel(beach['lon'], beach['lat'], T)
        pyi, pxi = int(round(py)), int(round(px))
        in_frame = 0 <= pyi < labeled.shape[0] and 0 <= pxi < labeled.shape[1]
        label_at_pt = labeled[pyi, pxi] if in_frame else 0

        if label_at_pt > 0:
            target_mask = labeled == label_at_pt
            picked = 'contained-by'
        else:
            regions = measure.regionprops(labeled)
            best = min(regions,
                       key=lambda r: (r.centroid[0] - pyi)**2
                                   + (r.centroid[1] - pxi)**2)
            target_mask = labeled == best.label
            picked = 'nearest-to'

        # Fallback: catalog point on noise (pier base, vegetation) → tiny
        # neighbor blob picked. Take the largest blob within 600 px instead.
        if target_mask.sum() < 5000:
            regions = measure.regionprops(labeled)
            within = [r for r in regions
                      if (r.centroid[0]-pyi)**2 + (r.centroid[1]-pxi)**2 < 600**2]
            if within:
                biggest = max(within, key=lambda r: r.area)
                target_mask = labeled == biggest.label
                picked = 'largest-within-600px (catalog pt on noise)'
        print(f'    blob {picked} catalog point: {target_mask.sum():,} px')

        # If the chosen blob hits the canvas edge, grow and retry.
        if grow_pass < max_passes and _touches_edge(target_mask, EDGE_BUFFER_PX):
            print(f'    blob touches canvas edge — growing to {size*2}px')
            size *= 2
            continue
        break

    contour = eh.trace_largest_contour(target_mask, tol_px=1.0)  # tightened from 2px
    if contour is None:
        raise SystemExit('contour trace failed')
    print(f'Contour: {len(contour)} vertices (Douglas-Peucker @ 1px)')

    # 3) Project to lat/lon WKT
    wkt = contour_to_wkt(contour, T)

    # 4) Clean self-intersections via ST_MakeValid + take largest polygon part.
    #    Sand-mask hairpins create twisted contours that fail ST_IsValid;
    #    MakeValid + CollectionExtract(3=POLYGON) produces a clean output.
    val = supabase_query(f"""
        WITH raw AS (SELECT ST_GeomFromText('{wkt}', 4326) AS g),
             cleaned AS (
               SELECT ST_CollectionExtract(ST_MakeValid(g), 3) AS g FROM raw
             ),
             biggest AS (
               -- ST_CollectionExtract may return MultiPolygon if MakeValid
               -- split the geometry; pick the largest polygon component.
               SELECT (ST_Dump(g)).geom AS g FROM cleaned
             )
        SELECT ST_AsText(g) AS clean_wkt,
               ST_IsValid(g) AS valid,
               ST_Area(g::geography) AS area_m2
          FROM biggest
         ORDER BY ST_Area(g::geography) DESC
         LIMIT 1
    """)['rows'][0]
    if not val['valid']:
        raise SystemExit('cleaned polygon still invalid; aborting')
    print(f'Polygon area: {val["area_m2"]:,.0f} m²  (valid={val["valid"]})')
    # Use the cleaned WKT for the insert
    wkt = val['clean_wkt']

    # 5) Compute IoU with OSM if present
    osm_gj = fetch_osm_polygon(fid)
    iou = compute_iou_pg(wkt, osm_gj)
    if osm_gj:
        print(f'IoU with OSM beach polygon: {iou:.3f}')
    else:
        print('No OSM beach polygon for this fid')

    # 6) Replace any prior PENDING row with the same algorithm; insert new
    sql = f"""
        DELETE FROM public.beach_polygons_auto_extracted
         WHERE fid = {fid}
           AND algorithm_version = '{algo_ver}'
           AND review_status = 'pending';
        INSERT INTO public.beach_polygons_auto_extracted
            (fid, geom, algorithm_version, tile_zoom, basemap_source,
             source_image_path, vertex_count, pixel_count, osm_iou)
        VALUES
            ({fid},
             ST_GeomFromText('{wkt}', 4326),
             '{algo_ver}',
             {zoom},
             '{basemap_src}',
             'share/{target}_{source}.png',
             {len(contour)},
             {int(target_mask.sum())},
             {iou if iou is not None else 'NULL'});
        SELECT id FROM public.beach_polygons_auto_extracted
         WHERE fid = {fid} AND algorithm_version = '{algo_ver}'
         ORDER BY extracted_at DESC LIMIT 1;
    """
    res = supabase_query(sql)
    print(f'Inserted row id: {res["rows"][0]["id"]}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    # Optional --source flag: --source satellite (default: topo)
    args = list(sys.argv[1:])
    source = 'topo'
    if '--source' in args:
        i = args.index('--source')
        source = args[i+1]
        args = args[:i] + args[i+2:]
    main(args[0], source=source)
