"""detect_walkway.py — for a beach with an existing sand polygon, look for
a thin concrete walkway running parallel to the inland edge of the sand.

Pipeline:
  1. Re-extract sand polygon
  2. Build a 30-px inland buffer band around the sand polygon's inland edge
  3. Within the band, detect concrete-like pixels (low saturation, mid V)
  4. Skeletonize the concrete mask
  5. Filter skeleton components by:
     a. orientation — keep only chains roughly parallel to the sand boundary
     b. length      — drop chains shorter than MIN_CHAIN_PX (drops noise)
  6. Compute walkway length and gate on coverage:
     if walkway_length < WALKWAY_GATE_FRAC * sand_inland_edge_length,
     report walkway_present=false (this beach probably doesn't have a strand)

Usage:
  python scripts/detect_walkway.py 8270    # Seal Beach
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from skimage import color, measure, morphology

import extract_hybrid as eh
import satellite_ab as sat


# ---- Tuning constants ----
MIN_CHAIN_PX           = 80     # skeleton chains shorter than this are noise
ORIENTATION_TOLERANCE  = 25.0   # degrees off sand boundary direction to accept
WALKWAY_GATE_FRAC      = 0.30   # walkway must cover this fraction of inland edge
                                # to be considered "present" on this beach


def boundary_orientation_deg(sand_blob: np.ndarray) -> float:
    """Compute the dominant orientation (in degrees, 0-180) of the sand
    polygon by PCA on its boundary pixels. Sand strips are typically long
    and thin so the major axis aligns with the beach direction."""
    coords = np.argwhere(sand_blob)  # (N, 2): rows, cols
    if len(coords) < 50:
        return 0.0
    coords = coords - coords.mean(axis=0)
    cov = np.cov(coords.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    # major axis = eigenvector with largest eigenvalue
    major = eigvecs[:, -1]  # (drow, dcol)
    angle = math.degrees(math.atan2(major[1], major[0]))  # 0 = horizontal
    # Normalize to [0, 180)
    while angle < 0:    angle += 180
    while angle >= 180: angle -= 180
    return angle


def angle_diff_deg(a: float, b: float) -> float:
    """Smallest angular difference, accounting for line-direction symmetry
    (a line at 10° == a line at 190°)."""
    d = abs(a - b) % 180
    return min(d, 180 - d)


def filter_skeleton_by_orientation(skel: np.ndarray, target_angle_deg: float
                                   ) -> tuple[np.ndarray, float]:
    """Keep only skeleton chains whose principal-axis orientation falls within
    ORIENTATION_TOLERANCE of target_angle_deg AND whose length >= MIN_CHAIN_PX.
    Returns (filtered skeleton, total length in px)."""
    labeled = measure.label(skel, connectivity=2)
    keep = np.zeros_like(skel)
    total = 0
    for region in measure.regionprops(labeled):
        chain = labeled == region.label
        chain_len = chain.sum()
        if chain_len < MIN_CHAIN_PX:
            continue
        coords = np.argwhere(chain) - region.centroid
        if len(coords) < 5:
            continue
        cov = np.cov(coords.T)
        eigvals, eigvecs = np.linalg.eigh(cov)
        major = eigvecs[:, -1]
        ang = math.degrees(math.atan2(major[1], major[0]))
        while ang < 0:    ang += 180
        while ang >= 180: ang -= 180
        if angle_diff_deg(ang, target_angle_deg) <= ORIENTATION_TOLERANCE:
            keep |= chain
            total += chain_len
    return keep, total


def detect_concrete(rgb: np.ndarray) -> np.ndarray:
    """Concrete walkway pixels: low saturation, mid brightness, near-neutral hue.

    A concrete promenade in Esri World_Imagery typically reads as
    RGB ~(140-180, 140-180, 140-180) — gray, low color, mid brightness.
    Slightly bluish-gray shadows are also possible. Excludes:
      - asphalt streets (darker, V<0.45)
      - sand (V too high, S nonzero, H warm)
      - water (high S in blue range)
      - vegetation (green hue)
      - rooftops with color tint (red roofs, terracotta, etc.)
    """
    hsv = color.rgb2hsv(rgb)
    h = hsv[..., 0] * 360.0
    s = hsv[..., 1]
    v = hsv[..., 2]
    return (s <= 0.12) & (v >= 0.45) & (v <= 0.78)


def query(sql: str) -> list[dict]:
    out = subprocess.check_output(
        ['supabase', 'db', 'query', '--linked', sql], text=True)
    return json.loads(out[out.find('{'):out.rfind('}')+1])['rows']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fid', type=int)
    ap.add_argument('--zoom', type=int, default=17)
    ap.add_argument('--size', type=int, default=2048)
    ap.add_argument('--buffer-px', type=int, default=30,
                    help='How far inland from sand polygon to look for walkway')
    args = ap.parse_args()

    # Look up beach
    rows = query(f"""
        SELECT bg.fid, bg.name, bg.lat, bg.lon FROM public.beaches_gold bg
         WHERE bg.fid = {args.fid}
    """)
    beach = rows[0]
    slug = ''.join(c.lower() if c.isalnum() else '_' for c in beach['name'])
    print(f'Beach: fid={beach["fid"]} {beach["name"]}')

    # Stitch imagery (cached after satellite_ab.py run)
    cache = Path('tmp/esri_imagery_cache')
    rgb, T = sat.stitch_imagery(beach['lon'], beach['lat'],
                                args.zoom, args.size, args.size, cache)

    # Re-extract sand polygon
    sand_raw = sat.detect_sand_imagery(rgb)
    sand = sat.clean(sand_raw)
    labeled = measure.label(sand)
    px, py = eh.lonlat_to_pixel(beach['lon'], beach['lat'], T)
    pyi, pxi = int(round(py)), int(round(px))
    label_at_pt = labeled[pyi, pxi] if (0 <= pyi < labeled.shape[0]
                                        and 0 <= pxi < labeled.shape[1]) else 0
    if label_at_pt > 0:
        sand_blob = labeled == label_at_pt
    else:
        regions = measure.regionprops(labeled)
        # Largest blob within 600px (same heuristic as satellite_ab)
        within = [r for r in regions if (r.centroid[0]-pyi)**2 + (r.centroid[1]-pxi)**2 < 600**2]
        biggest = max(within or regions, key=lambda r: r.area)
        sand_blob = labeled == biggest.label
    print(f'Sand blob: {sand_blob.sum():,} px')

    # ---- Build inland buffer band ----
    # Inland from sand = pixels NORTH of the sand polygon (in image coords,
    # smaller y). For Seal Beach the inland direction is "up" (smaller y).
    # General approach: dilate sand polygon outward, then subtract sand to
    # get the buffer ring around it. Then restrict to "north of the sand
    # boundary" by checking which side of the centroid each pixel is on.
    dilated = morphology.binary_dilation(sand_blob, morphology.disk(args.buffer_px))
    band_full = dilated & ~sand_blob

    # Restrict to inland side: keep only pixels that are NORTH of the
    # sand polygon's row centroid (smaller y means more inland in
    # most CA beach orientations)
    sand_centroid_y = measure.regionprops(sand_blob.astype(np.uint8))[0].centroid[0]
    yy, _xx = np.ogrid[:rgb.shape[0], :rgb.shape[1]]
    inland_band = band_full & (yy < sand_centroid_y)
    print(f'Inland buffer band: {inland_band.sum():,} px')

    # ---- Detect concrete pixels in the band ----
    concrete = detect_concrete(rgb) & inland_band
    print(f'Concrete in band: {concrete.sum():,} px')

    # Clean small noise + close gaps along the linear walkway
    concrete = morphology.binary_closing(concrete, morphology.disk(2))
    concrete = morphology.remove_small_objects(concrete, min_size=200)
    print(f'Concrete after cleanup: {concrete.sum():,} px')

    # ---- Skeletonize to get a 1-px centerline ----
    skel_raw = morphology.skeletonize(concrete)
    print(f'Raw skeleton: {skel_raw.sum():,} px')

    # ---- Filter by orientation + length ----
    sand_angle = boundary_orientation_deg(sand_blob)
    print(f'Sand polygon principal axis: {sand_angle:.1f}°')

    skel, walkway_length_px = filter_skeleton_by_orientation(skel_raw, sand_angle)
    print(f'Skeleton after orientation+length filter: {walkway_length_px:,} px')

    # ---- Presence gate ----
    # Compute the sand polygon's inland-edge length to scale against.
    sand_contour = eh.trace_largest_contour(sand_blob, tol_px=2.0)
    if sand_contour is not None:
        # Inland-edge length: sum lengths of contour edges where both endpoints
        # are above the sand centroid (= inland-facing).
        cy = measure.regionprops(sand_blob.astype(np.uint8))[0].centroid[0]
        inland_edge_px = 0.0
        for i in range(len(sand_contour)):
            r1, c1 = sand_contour[i]
            r2, c2 = sand_contour[(i+1) % len(sand_contour)]
            if r1 < cy and r2 < cy:
                inland_edge_px += math.hypot(r2-r1, c2-c1)
    else:
        inland_edge_px = 0

    coverage = walkway_length_px / inland_edge_px if inland_edge_px > 0 else 0
    walkway_present = coverage >= WALKWAY_GATE_FRAC

    # Convert to meters using the per-pixel scale at this lat/zoom
    m_per_px = 156543.03 / 2**args.zoom * math.cos(math.radians(beach['lat']))
    walkway_length_m = walkway_length_px * m_per_px
    inland_edge_m    = inland_edge_px    * m_per_px

    print(f'Inland edge length:  {inland_edge_px:.0f} px = {inland_edge_m:.0f} m')
    print(f'Walkway length:      {walkway_length_px} px = {walkway_length_m:.0f} m')
    print(f'Coverage:            {coverage:.0%}  '
          f'(gate {WALKWAY_GATE_FRAC:.0%})  '
          f'-> walkway_present={walkway_present}')

    # ---- Render the overlay ----
    img = Image.fromarray(rgb).convert('RGBA')
    layer = Image.new('RGBA', img.size, (0,0,0,0))
    d = ImageDraw.Draw(layer)

    # Sand polygon outline (cyan) for reference (already computed above)
    if sand_contour is not None:
        sand_pts = [(int(c), int(r)) for r, c in sand_contour]
        d.line(sand_pts + [sand_pts[0]], fill=(255,255,255,200), width=4)
        d.line(sand_pts + [sand_pts[0]], fill=(14, 165, 233, 255), width=2)

    # Inland buffer band — translucent yellow tint to show search zone
    band_layer = np.zeros((*rgb.shape[:2], 4), dtype=np.uint8)
    band_layer[inland_band] = (255, 235, 59, 50)
    layer = Image.alpha_composite(layer, Image.fromarray(band_layer))

    # Concrete pixels — magenta
    concrete_layer = np.zeros((*rgb.shape[:2], 4), dtype=np.uint8)
    concrete_layer[concrete] = (236, 72, 153, 180)
    layer = Image.alpha_composite(layer, Image.fromarray(concrete_layer))

    # Skeleton / centerline — bright lime (drawn as thick line on top)
    skel_layer = np.zeros((*rgb.shape[:2], 4), dtype=np.uint8)
    # Dilate skeleton by 2px so it's visible
    skel_thick = morphology.binary_dilation(skel, morphology.disk(2))
    skel_layer[skel_thick] = (132, 204, 22, 255)
    layer = Image.alpha_composite(layer, Image.fromarray(skel_layer))

    out = Image.alpha_composite(img, layer)
    out_path = Path('share') / f'{slug}_walkway.png'
    out.save(out_path)
    print(f'Wrote {out_path}  {out.size}')

    # Also write a half-size version for sharing
    half = out.resize((out.width // 2, out.height // 2), Image.LANCZOS)
    half.save(Path('share') / f'{slug}_walkway_small.png')


if __name__ == '__main__':
    main()
