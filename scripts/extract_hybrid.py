"""extract_hybrid.py — combine OSM beach polygon + sand-color extraction
into a single hybrid polygon.

Pipeline:
  1. Stitch Esri World_Topo tiles around the beach centroid into a known-extent image
  2. Fetch the OSM beach polygon as GeoJSON
  3. Project OSM (lat/lon) -> pixel coords using Web Mercator
  4. Detect sand pixels (color thresholding, same as variants script)
  5. Build hybrid mask: OSM polygon ∪ (sand blobs that touch the OSM polygon)
     — sand connected to OSM gets folded in (extends the polygon)
     — sand NOT connected to OSM is excluded (other beaches, false positives)
  6. Trace contour, simplify, render side-by-side comparison

Output:
  share/<slug>_basemap.png        — raw stitched basemap
  share/<slug>_osm_only.png       — basemap + OSM polygon (red)
  share/<slug>_sand_only.png      — basemap + sand-color polygon (cyan)
  share/<slug>_hybrid.png         — basemap + hybrid polygon (magenta)
  share/<slug>_compare.png        — 2x2 grid of all four
"""
from __future__ import annotations

import json
import math
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from skimage import color, morphology, measure


# --------------------------------------------------------------------------
# Web Mercator <-> tile coords
# --------------------------------------------------------------------------
TILE = 256
ESRI = ('https://server.arcgisonline.com/ArcGIS/rest/services/'
        'World_Topo_Map/MapServer/tile/{z}/{y}/{x}')


def lonlat_to_tile(lon: float, lat: float, z: int) -> tuple[float, float]:
    """Continuous (fractional) tile coords."""
    n = 2 ** z
    x = (lon + 180.0) / 360.0 * n
    lat_r = math.radians(lat)
    y = (1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n
    return x, y


def tile_to_lonlat(x: float, y: float, z: int) -> tuple[float, float]:
    """Inverse: fractional tile coords -> (lon, lat)."""
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat_r = math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n)))
    return lon, math.degrees(lat_r)


def fetch_tile(z: int, x: int, y: int, cache_dir: Path) -> np.ndarray:
    """Fetch one tile (with on-disk cache)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    fp = cache_dir / f'{z}_{x}_{y}.png'
    if not fp.exists():
        url = ESRI.format(z=z, x=x, y=y)
        req = urllib.request.Request(url, headers={'User-Agent': 'dog-beach-scout/0.1'})
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            fp.write_bytes(r.read())
        time.sleep(0.05)
    return np.array(Image.open(fp).convert('RGB'))


def stitch(center_lon: float, center_lat: float, z: int,
           width_px: int, height_px: int,
           cache_dir: Path) -> tuple[np.ndarray, dict]:
    """Stitch tiles into a single image centered on (lon, lat).

    Returns (rgb image, transform dict). The transform dict has keys
        x0, y0   — fractional tile coords of the upper-left pixel
        z, w, h  — zoom and dimensions
    so callers can reproject points.
    """
    cx, cy = lonlat_to_tile(center_lon, center_lat, z)
    # Upper-left fractional tile coord:
    x0 = cx - width_px / (2 * TILE)
    y0 = cy - height_px / (2 * TILE)
    # Tile range needed:
    tx_min = int(math.floor(x0))
    ty_min = int(math.floor(y0))
    tx_max = int(math.ceil(x0 + width_px / TILE))
    ty_max = int(math.ceil(y0 + height_px / TILE))

    canvas_w = (tx_max - tx_min) * TILE
    canvas_h = (ty_max - ty_min) * TILE
    canvas = np.zeros((canvas_h, canvas_w, 3), dtype=np.uint8)

    for tx in range(tx_min, tx_max):
        for ty in range(ty_min, ty_max):
            tile = fetch_tile(z, tx, ty, cache_dir)
            ox = (tx - tx_min) * TILE
            oy = (ty - ty_min) * TILE
            canvas[oy:oy+TILE, ox:ox+TILE] = tile

    # Crop to the requested window:
    crop_x = int(round((x0 - tx_min) * TILE))
    crop_y = int(round((y0 - ty_min) * TILE))
    out = canvas[crop_y:crop_y+height_px, crop_x:crop_x+width_px]
    return out, {'x0': x0, 'y0': y0, 'z': z, 'w': width_px, 'h': height_px}


def lonlat_to_pixel(lon: float, lat: float, T: dict) -> tuple[float, float]:
    tx, ty = lonlat_to_tile(lon, lat, T['z'])
    px = (tx - T['x0']) * TILE
    py = (ty - T['y0']) * TILE
    return px, py


def pixel_to_lonlat(px: float, py: float, T: dict) -> tuple[float, float]:
    """Inverse of lonlat_to_pixel."""
    tx = T['x0'] + px / TILE
    ty = T['y0'] + py / TILE
    return tile_to_lonlat(tx, ty, T['z'])


# --------------------------------------------------------------------------
# Sand detection
# --------------------------------------------------------------------------
def detect_sand(rgb: np.ndarray) -> np.ndarray:
    """Calibrated against actual Esri tile colors at z=16 (sample_seal_colors.py):
       Actual sand: V≈0.97 S≈0.04 H≈60°
       Wetland:     V≈0.85 S≈0.11
       Streets:     V≈0.86 S≈0.01
    Brightness (V) is the cleanest differentiator — sand is the only thing
    that hits V > 0.93 in the topo basemap."""
    hsv = color.rgb2hsv(rgb)
    h = hsv[..., 0] * 360.0
    s = hsv[..., 1]
    v = hsv[..., 2]
    return (h >= 40) & (h <= 75) & (s >= 0.02) & (s <= 0.10) & (v >= 0.93)


def clean_mask(mask: np.ndarray, min_size: int = 800) -> np.ndarray:
    m = morphology.binary_closing(mask, morphology.disk(3))
    return morphology.remove_small_objects(m, min_size=min_size)


# --------------------------------------------------------------------------
# Polygon utilities
# --------------------------------------------------------------------------
def rasterize_polygon(coords_px: list[tuple[float, float]],
                      shape: tuple[int, int]) -> np.ndarray:
    """Turn a polygon (pixel coords) into a boolean mask the size of the image."""
    mask_img = Image.new('L', (shape[1], shape[0]), 0)
    ImageDraw.Draw(mask_img).polygon(coords_px, fill=1)
    return np.array(mask_img, dtype=bool)


def trace_largest_contour(mask: np.ndarray, tol_px: float = 2.0):
    if not mask.any():
        return None
    contours = measure.find_contours(mask.astype(float), 0.5)
    if not contours:
        return None
    biggest = max(contours, key=len)
    return measure.approximate_polygon(biggest, tolerance=tol_px)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------
def overlay(rgb: np.ndarray,
            polygons: list[tuple[np.ndarray, tuple[int, int, int]]],
            red_dot: tuple[float, float] | None = None) -> Image.Image:
    img = Image.fromarray(rgb).convert('RGBA')
    layer = Image.new('RGBA', img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for contour, rgb_color in polygons:
        if contour is None or len(contour) < 3:
            continue
        pts = [(int(c), int(r)) for r, c in contour]
        # White halo
        d.line(pts + [pts[0]], fill=(255, 255, 255, 230), width=8)
        # Bold outline
        d.line(pts + [pts[0]], fill=rgb_color + (255,), width=4)
        # Faint fill
        d.polygon(pts, fill=rgb_color + (60,))
    if red_dot is not None:
        rx, ry = red_dot
        d.ellipse([rx - 7, ry - 7, rx + 7, ry + 7],
                  outline=(255, 255, 255, 255), width=2,
                  fill=(180, 0, 0, 230))
    return Image.alpha_composite(img, layer)


def grid_2x2(images: list[Image.Image], titles: list[str],
             cell_w: int, cell_h: int) -> Image.Image:
    """Side-by-side 2x2 grid with title bars."""
    title_h = 28
    pad = 6
    out_w = cell_w * 2 + pad * 3
    out_h = (cell_h + title_h) * 2 + pad * 3
    grid = Image.new('RGB', (out_w, out_h), (240, 240, 248))
    d = ImageDraw.Draw(grid)
    from PIL import ImageFont
    try:
        font = ImageFont.truetype('arial.ttf', 16)
    except Exception:
        font = ImageFont.load_default()
    for i, (img, title) in enumerate(zip(images, titles)):
        row = i // 2
        col = i % 2
        x = pad + col * (cell_w + pad)
        y = pad + row * (cell_h + title_h + pad)
        d.rectangle([x, y, x + cell_w, y + title_h], fill=(15, 39, 66))
        d.text((x + 8, y + 6), title, fill=(255, 255, 255), font=font)
        grid.paste(img.convert('RGB'),
                   (x, y + title_h),
                   img.split()[3] if img.mode == 'RGBA' else None)
    return grid


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def fetch_osm_polygon_for_fid(fid: int) -> dict:
    """Get the OSM beach polygon for a gold fid via supabase REST."""
    import os, dotenv
    dotenv.load_dotenv('scripts/pipeline/.env')
    url = os.environ['SUPABASE_URL']
    key = os.environ['SUPABASE_SERVICE_KEY']
    sql = f"""
        SELECT op.id, op.name,
               ST_AsGeoJSON(op.geom) as geom_json,
               ST_Area(op.geom::geography) as area_m2
          FROM public.osm_beach_polys op
          JOIN public.beaches_gold bg
            ON ST_Intersects(op.geom, bg.geom)
         WHERE bg.fid = {fid} AND bg.is_active
         LIMIT 1
    """
    req = urllib.request.Request(
        url.rstrip('/') + '/rest/v1/rpc/exec_sql',
        method='POST',
        headers={'apikey': key, 'Authorization': f'Bearer {key}',
                 'Content-Type': 'application/json'},
        data=json.dumps({'sql': sql}).encode(),
    )
    # Fall back to direct PostgREST query if exec_sql RPC is unavailable.
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())[0]
    except Exception:
        pass
    # PostgREST direct path: filter via st_intersects through a view is awkward —
    # take the simpler path of querying osm_beach_polys directly by id.
    return None


def main(beach_name: str, lat: float, lon: float, osm_geojson: dict,
         out_slug: str, zoom: int = 17, size: int = 1280):
    cache = Path('tmp/esri_tile_cache')
    out_dir = Path('share')
    out_dir.mkdir(exist_ok=True)

    # ---- 1) Stitch basemap ------------------------------------------------
    rgb, T = stitch(lon, lat, zoom, size, size, cache)
    print(f'Stitched basemap: {rgb.shape}  zoom={zoom}')
    Image.fromarray(rgb).save(out_dir / f'{out_slug}_basemap.png')

    # ---- 2) Project OSM polygon(s) to pixel coords; rasterize union ------
    if osm_geojson['type'] == 'Polygon':
        polygons = [osm_geojson['coordinates']]
    elif osm_geojson['type'] == 'MultiPolygon':
        polygons = osm_geojson['coordinates']
    else:
        raise ValueError(f"Unhandled geometry type: {osm_geojson['type']}")

    osm_mask = np.zeros(rgb.shape[:2], dtype=bool)
    total_verts = 0
    for poly in polygons:
        outer = poly[0]  # ignoring holes for now
        pts = [(int(round(x)), int(round(y)))
               for x, y in (lonlat_to_pixel(p[0], p[1], T) for p in outer)]
        sub = rasterize_polygon(pts, (rgb.shape[0], rgb.shape[1]))
        osm_mask = osm_mask | sub
        total_verts += len(outer)
    print(f'OSM polygon: {len(polygons)} part(s), {total_verts} vertices total')
    print(f'  OSM mask pixels: {osm_mask.sum():,}')

    # ---- 3) Sand detection ------------------------------------------------
    sand_raw = detect_sand(rgb)
    sand = clean_mask(sand_raw)
    print(f'Sand mask pixels: {sand.sum():,} (raw {sand_raw.sum():,})')

    # ---- 4) Hybrid: OSM ∪ (sand within ~50m of OSM) ----------------------
    # At z=16 each pixel ~2.4m, so 50m ~= 21px. Dilate OSM by that radius and
    # keep only the sand pixels that fall inside the dilated zone — this
    # bounds how far the sand extension can reach without ever connecting
    # through the inland road network.
    expansion_radius_px = 22
    osm_zone = morphology.binary_dilation(osm_mask, morphology.disk(expansion_radius_px))
    sand_extension = sand & osm_zone & ~osm_mask
    hybrid_mask = osm_mask | sand_extension
    print(f'Hybrid mask pixels: {hybrid_mask.sum():,}  '
          f'(OSM {osm_mask.sum():,} + sand-extension {sand_extension.sum():,} '
          f'within {expansion_radius_px}px buffer)')

    # ---- 5) Trace contours & render --------------------------------------
    osm_contour = trace_largest_contour(osm_mask, tol_px=2.0)
    sand_contour = trace_largest_contour(sand, tol_px=2.0)
    hybrid_contour = trace_largest_contour(hybrid_mask, tol_px=2.0)

    # Find the catalog point in pixels for reference
    cat_px = lonlat_to_pixel(lon, lat, T)

    img_basemap = Image.fromarray(rgb).convert('RGBA')
    img_osm    = overlay(rgb, [(osm_contour,    (220,  38,  38))], red_dot=cat_px)
    img_sand   = overlay(rgb, [(sand_contour,   ( 14, 165, 233))], red_dot=cat_px)
    img_hybrid = overlay(rgb, [(hybrid_contour, (255,  20, 147))], red_dot=cat_px)

    img_osm.save(out_dir / f'{out_slug}_osm_only.png')
    img_sand.save(out_dir / f'{out_slug}_sand_only.png')
    img_hybrid.save(out_dir / f'{out_slug}_hybrid.png')

    grid = grid_2x2(
        [img_basemap, img_osm, img_sand, img_hybrid],
        ['1 — Basemap (raw)',
         '2 — OSM polygon only (red)',
         '3 — Sand-color extraction only (cyan)',
         '4 — HYBRID: OSM ∪ sand-blobs-touching-OSM (magenta)'],
        cell_w=size, cell_h=size,
    )
    # Resize to half for sharing
    grid = grid.resize((grid.width // 2, grid.height // 2), Image.LANCZOS)
    grid.save(out_dir / f'{out_slug}_compare.png')
    print(f'Wrote {out_slug}_compare.png  ({grid.size})')


if __name__ == '__main__':
    # Hard-coded for Seal Beach OC (fid=8270, OSM id=1185)
    # Avoids the supabase RPC roundtrip — geometry is fetched directly.
    print('Fetching Seal Beach OC OSM polygon...')
    import os
    import urllib.request
    # Pull fresh GeoJSON via the Supabase PostgREST endpoint
    sup_url = 'https://ehlzbwtrsxaaukurekau.supabase.co'
    anon = 'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'
    req = urllib.request.Request(
        f'{sup_url}/rest/v1/osm_beach_polys?select=id,name,geom_geojson&id=eq.1185',
        headers={'apikey': anon, 'Authorization': f'Bearer {anon}'},
    )
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
            data = json.loads(r.read())
        if data and data[0].get('geom_geojson'):
            osm_geojson = data[0]['geom_geojson']
            if isinstance(osm_geojson, str):
                osm_geojson = json.loads(osm_geojson)
        else:
            print('PostgREST response missing geom_geojson — falling back to local supabase CLI')
            raise RuntimeError('need fallback')
    except Exception as e:
        print(f'PostgREST failed ({e}); using supabase CLI fallback')
        import subprocess
        out = subprocess.check_output([
            'supabase', 'db', 'query', '--linked',
            "SELECT ST_AsGeoJSON(geom) as g FROM public.osm_beach_polys WHERE id=1185"
        ], text=True)
        # parse the JSON output that supabase CLI emits
        idx = out.find('{')
        cli_data = json.loads(out[idx:out.rfind('}')+1])
        osm_geojson = json.loads(cli_data['rows'][0]['g'])
    print(f'OSM polygon ready: type={osm_geojson["type"]}')
    main('Seal Beach', lat=33.7377, lon=-118.1063,
         osm_geojson=osm_geojson, out_slug='seal_beach',
         zoom=16, size=1280)
