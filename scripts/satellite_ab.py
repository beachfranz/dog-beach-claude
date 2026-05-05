"""satellite_ab.py — A/B sand-color extraction on Esri World_Imagery vs the
v2 topo polygon for one beach.

Pipeline:
  1. Stitch World_Imagery tiles at z=18 (≈0.6 m/px, 4× topo resolution)
  2. Sample known points (catalog centroid) to calibrate sand thresholds
  3. Run anchor-on-point + auto-grow extraction (same as v2)
  4. Pull the existing v2 topo polygon from beach_polygons_auto_extracted
  5. Render both polygons on the imagery basemap for comparison

Output:
  share/<slug>_imagery.png        — basemap only (raw)
  share/<slug>_imagery_v2.png     — topo v2 polygon overlay (red dashed)
  share/<slug>_imagery_sat.png    — satellite-derived polygon (cyan)
  share/<slug>_imagery_compare.png — both on the same image

Usage:
  python scripts/satellite_ab.py 8270    # Seal Beach
  python scripts/satellite_ab.py 8270 --zoom 17 --size 1024
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
import urllib.request
import ssl
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from skimage import color, measure, morphology

import extract_hybrid as eh

IMAGERY_URL = ('https://server.arcgisonline.com/ArcGIS/rest/services/'
               'World_Imagery/MapServer/tile/{z}/{y}/{x}')


def fetch_imagery_tile(z: int, x: int, y: int, cache_dir: Path) -> np.ndarray:
    cache_dir.mkdir(parents=True, exist_ok=True)
    fp = cache_dir / f'{z}_{x}_{y}.jpg'
    if not fp.exists():
        url = IMAGERY_URL.format(z=z, x=x, y=y)
        req = urllib.request.Request(url, headers={'User-Agent': 'dog-beach-scout/0.1'})
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
            fp.write_bytes(r.read())
        time.sleep(0.04)
    return np.array(Image.open(fp).convert('RGB'))


def stitch_imagery(center_lon, center_lat, z, w, h, cache_dir):
    """Same as eh.stitch but uses World_Imagery and JPEG tiles."""
    cx, cy = eh.lonlat_to_tile(center_lon, center_lat, z)
    x0 = cx - w / (2 * eh.TILE)
    y0 = cy - h / (2 * eh.TILE)
    tx_min = int(math.floor(x0))
    ty_min = int(math.floor(y0))
    tx_max = int(math.ceil(x0 + w / eh.TILE))
    ty_max = int(math.ceil(y0 + h / eh.TILE))
    canvas_w = (tx_max - tx_min) * eh.TILE
    canvas_h = (ty_max - ty_min) * eh.TILE
    canvas = np.zeros((canvas_h, canvas_w, 3), dtype=np.uint8)
    for tx in range(tx_min, tx_max):
        for ty in range(ty_min, ty_max):
            canvas[(ty-ty_min)*eh.TILE:(ty-ty_min+1)*eh.TILE,
                   (tx-tx_min)*eh.TILE:(tx-tx_min+1)*eh.TILE] = \
                fetch_imagery_tile(z, tx, ty, cache_dir)
    crop_x = int(round((x0 - tx_min) * eh.TILE))
    crop_y = int(round((y0 - ty_min) * eh.TILE))
    return canvas[crop_y:crop_y+h, crop_x:crop_x+w], \
           {'x0': x0, 'y0': y0, 'z': z, 'w': w, 'h': h}


def detect_sand_imagery(rgb: np.ndarray) -> np.ndarray:
    """Calibrated against actual Esri World_Imagery pixels at z=18 for Seal
    Beach. Real sand clusters at H≈46°, S≈0.16, V≈0.83. Tight thresholds
    around that cluster — sand looks remarkably consistent at typical
    daytime captures. Wet sand near waterline is captured by a second
    rule (slightly darker, slightly more saturated)."""
    hsv = color.rgb2hsv(rgb)
    h = hsv[..., 0] * 360.0
    s = hsv[..., 1]
    v = hsv[..., 2]
    dry = (h >= 38) & (h <= 60) & (s >= 0.08) & (s <= 0.28) & (v >= 0.70)
    wet = (h >= 30) & (h <= 55) & (s >= 0.15) & (s <= 0.40) & (v >= 0.45) & (v <= 0.72)
    return dry | wet


def clean(mask: np.ndarray, min_size: int = 1500) -> np.ndarray:
    m = morphology.binary_closing(mask, morphology.disk(3))
    m = morphology.binary_opening(m, morphology.disk(2))
    return morphology.remove_small_objects(m, min_size=min_size)


def query(sql: str) -> list[dict]:
    out = subprocess.check_output(
        ['supabase', 'db', 'query', '--linked', sql], text=True)
    idx = out.find('{')
    end = out.rfind('}')
    return json.loads(out[idx:end+1])['rows']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fid', type=int)
    ap.add_argument('--zoom', type=int, default=18)
    ap.add_argument('--size', type=int, default=2048)
    args = ap.parse_args()

    rows = query(f"""
      SELECT bg.fid, bg.name, bg.lat, bg.lon, bg.county_name
        FROM public.beaches_gold bg WHERE bg.fid = {args.fid}
    """)
    if not rows:
        raise SystemExit(f'fid {args.fid} not found')
    beach = rows[0]
    slug = ''.join(c.lower() if c.isalnum() else '_' for c in beach['name'])
    print(f'Beach: fid={beach["fid"]} {beach["name"]} '
          f'lat={beach["lat"]} lon={beach["lon"]}')

    # ---- Stitch imagery ----
    cache = Path('tmp/esri_imagery_cache')
    rgb, T = stitch_imagery(beach['lon'], beach['lat'],
                            args.zoom, args.size, args.size, cache)
    out_dir = Path('share')
    Image.fromarray(rgb).save(out_dir / f'{slug}_imagery.png')
    print(f'Imagery: {rgb.shape}  z={args.zoom}  ~{156543.03/2**args.zoom*math.cos(math.radians(beach["lat"])):.2f} m/px')

    # ---- Sample sand color near catalog point ----
    px, py = eh.lonlat_to_pixel(beach['lon'], beach['lat'], T)
    pyi, pxi = int(round(py)), int(round(px))
    box = rgb[max(0, pyi-30):pyi+30, max(0, pxi-30):pxi+30]
    if box.size:
        hsv = color.rgb2hsv(box)
        print(f'Sample around red dot:  RGB mean={tuple(int(c) for c in box.reshape(-1,3).mean(0))}  '
              f'H={hsv[...,0].mean()*360:.0f} S={hsv[...,1].mean():.2f} V={hsv[...,2].mean():.2f}')

    # ---- Detect + clean ----
    sand_raw = detect_sand_imagery(rgb)
    sand = clean(sand_raw)
    print(f'Sand mask: {sand.sum():,} px (raw {sand_raw.sum():,})  '
          f'= {sand.sum() * (156543.03/2**args.zoom*math.cos(math.radians(beach["lat"])))**2:,.0f} m²')

    if not sand.any():
        raise SystemExit('no sand detected')

    # ---- Anchor on catalog point, with a size sanity check ----
    # If catalog point sits on non-sand (pier, vegetation, etc.) the literal
    # nearest blob can be tiny noise. Prefer largest-area blob within a 400px
    # radius of the catalog point if the contained/nearest pick is < 5K px.
    labeled = measure.label(sand)
    label_at_pt = labeled[pyi, pxi] if (0 <= pyi < labeled.shape[0]
                                        and 0 <= pxi < labeled.shape[1]) else 0
    pick_reason = ''
    if label_at_pt > 0:
        target = labeled == label_at_pt
        pick_reason = 'contained-by'
    else:
        regions = measure.regionprops(labeled)
        # Score: closer is better, but bigger blobs beat tiny ones tied for distance.
        def score(r):
            cy, cx = r.centroid
            dist = ((cy-pyi)**2 + (cx-pxi)**2) ** 0.5
            return dist - 0.05 * np.log(r.area)  # area gets log-weighted bonus
        best = min(regions, key=score)
        target = labeled == best.label
        pick_reason = 'nearest+area-weighted'

    # Fallback: if pick is tiny (<5K px), the catalog point is in noise —
    # take the largest blob within 600 px radius instead.
    if target.sum() < 5000:
        regions = measure.regionprops(labeled)
        within_radius = [r for r in regions
                         if ((r.centroid[0]-pyi)**2 + (r.centroid[1]-pxi)**2) < 600**2]
        if within_radius:
            biggest = max(within_radius, key=lambda r: r.area)
            target = labeled == biggest.label
            pick_reason = 'largest-within-600px (catalog point on noise)'

    print(f'Anchor: {pick_reason}  -> {target.sum():,} px')

    contour = eh.trace_largest_contour(target, tol_px=1.5)
    print(f'Contour: {len(contour)} vertices')

    # ---- Pull the v2 topo polygon for comparison ----
    v2_rows = query(f"""
        SELECT ST_AsGeoJSON(geom) as g, area_m2, vertex_count
          FROM public.beach_polygons_auto_extracted
         WHERE fid = {args.fid}
           AND algorithm_version LIKE 'sand_v%'
         ORDER BY extracted_at DESC LIMIT 1
    """)
    v2_geom = json.loads(v2_rows[0]['g']) if v2_rows else None
    if v2_geom:
        print(f'v2 topo polygon: {v2_rows[0]["area_m2"]:,.0f} m², {v2_rows[0]["vertex_count"]} verts')

    # ---- Render comparison ----
    img_base = Image.fromarray(rgb).convert('RGBA')
    layer = Image.new('RGBA', img_base.size, (0,0,0,0))
    d = ImageDraw.Draw(layer)
    # v2 topo polygon (red dashed-feel via doubled line)
    if v2_geom:
        rings = (v2_geom['coordinates'] if v2_geom['type'] == 'Polygon'
                 else max(v2_geom['coordinates'], key=lambda p: len(p[0])))
        ring0 = rings[0] if v2_geom['type'] == 'Polygon' else rings[0]
        v2_pts = [eh.lonlat_to_pixel(p[0], p[1], T) for p in ring0]
        v2_pts_int = [(int(x), int(y)) for x, y in v2_pts]
        d.line(v2_pts_int + [v2_pts_int[0]], fill=(255,255,255,200), width=8)
        d.line(v2_pts_int + [v2_pts_int[0]], fill=(220, 38, 38, 255), width=4)
    # imagery polygon (cyan)
    sat_pts = [(int(c), int(r)) for r, c in contour]
    d.line(sat_pts + [sat_pts[0]], fill=(255,255,255,200), width=8)
    d.line(sat_pts + [sat_pts[0]], fill=(14, 165, 233, 255), width=4)
    d.polygon(sat_pts, fill=(14, 165, 233, 50))
    # red dot
    rx, ry = eh.lonlat_to_pixel(beach['lon'], beach['lat'], T)
    d.ellipse([rx-7, ry-7, rx+7, ry+7], outline=(255,255,255,255), width=2,
              fill=(255, 0, 0, 230))

    out = Image.alpha_composite(img_base, layer)
    out.save(out_dir / f'{slug}_imagery_compare.png')
    # Resize for sharing
    h_small = 800
    ratio = h_small / out.height
    small = out.resize((int(out.width * ratio), h_small), Image.LANCZOS)
    small.save(out_dir / f'{slug}_imagery_compare_small.png')
    print(f'Wrote {slug}_imagery_compare.png  ({out.size[0]}x{out.size[1]})')


if __name__ == '__main__':
    main()
