"""extract_beach_polygon_variants.py — produce multiple polygon-extraction
variants from a single beach screenshot for visual comparison.

Each variant writes <input>_v<N>.png with the detected polygon outlined in
cyan over the original image. Variants differ in how the sand mask is
filtered + clipped:

  V1  largest-blob, raw            — single dominant sand region, no smoothing
  V2  largest-blob, simplified     — same blob, Douglas-Peucker @ 3px
  V3  multi-blob                   — every sand blob ≥ 2000px (overlapping
                                     islands etc preserved)
  V4  red-dot anchor               — the sand blob that contains/abuts the
                                     red catalog marker (excludes neighboring
                                     beaches if their sand is not contiguous
                                     with the marker)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from skimage import color, morphology, measure


# ---------------------------------------------------------------------------
# Sand & red-dot detection
# ---------------------------------------------------------------------------
def detect_sand(rgb: np.ndarray, mode: str = "default") -> np.ndarray:
    """Sand pixel mask. Modes:
       loose   — wider hue, lower saturation floor (catches wet/edge sand)
       default — balanced
       strict  — only the dryest brightest sand
    """
    hsv = color.rgb2hsv(rgb)
    h = hsv[..., 0] * 360.0
    s = hsv[..., 1]
    v = hsv[..., 2]
    if mode == "loose":
        return (h >= 20) & (h <= 80) & (s >= 0.03) & (s <= 0.35) & (v >= 0.72)
    if mode == "strict":
        return (h >= 30) & (h <= 60) & (s >= 0.06) & (s <= 0.25) & (v >= 0.85)
    return     (h >= 25) & (h <= 70) & (s >= 0.04) & (s <= 0.30) & (v >= 0.78)


def detect_red_dot(rgb: np.ndarray) -> tuple[int, int] | None:
    """Find the red catalog marker (#ef4444 = ~239/68/68). Strict thresholds
    so basemap artifacts (red-ish roofs, route shields) don't get picked up.
    Marker is roughly a 10px-radius filled circle."""
    r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
    red = (r > 220) & (g < 95) & (b < 95) & ((r.astype(int) - g.astype(int)) > 130)
    if not red.any():
        return None
    cleaned = morphology.binary_closing(red, morphology.disk(2))
    cleaned = morphology.remove_small_objects(cleaned, min_size=40)
    if not cleaned.any():
        return None
    labeled = measure.label(cleaned)
    # Marker is a roughly-circular blob. Pick the most-circular region.
    regions = measure.regionprops(labeled)
    def circularity(rg):
        if rg.perimeter == 0:
            return 0
        return 4 * np.pi * rg.area / (rg.perimeter ** 2)
    best = max(regions, key=lambda rg: circularity(rg) * rg.area)
    cy, cx = best.centroid
    return int(round(cy)), int(round(cx))


def clean_mask(mask: np.ndarray, min_size: int = 2000) -> np.ndarray:
    """Close gaps + drop specks."""
    m = morphology.binary_closing(mask, morphology.disk(5))
    return morphology.remove_small_objects(m, min_size=min_size)


def trace_contour(mask: np.ndarray):
    contours = measure.find_contours(mask.astype(float), 0.5)
    return max(contours, key=len) if contours else None


def simplify(contour: np.ndarray, tol_px: float) -> np.ndarray:
    return measure.approximate_polygon(contour, tolerance=tol_px)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def overlay(rgb: np.ndarray, contours: list[np.ndarray], label: str,
            color_rgba=(255, 20, 147, 255),  # hot magenta — max contrast against cream/blue/gray basemap
            red_dot: tuple[int, int] | None = None
            ) -> Image.Image:
    img = Image.fromarray(rgb).convert("RGBA")
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    for c in contours:
        if len(c) < 3:
            continue
        pts = [(int(col), int(row)) for row, col in c]
        # 1) Thick white halo so the colored line pops against any basemap color.
        draw.line(pts + [pts[0]], fill=(255, 255, 255, 240), width=10)
        # 2) Bright hot-magenta outline.
        draw.line(pts + [pts[0]], fill=color_rgba, width=6)
        # 3) More-visible magenta fill so the interior is unambiguous.
        draw.polygon(pts, fill=(255, 20, 147, 70))
        # 4) Vertex dots so each corner of the polygon is identifiable —
        #    helps spot exactly where the boundary turns.
        for x, y in pts:
            draw.ellipse([x - 4, y - 4, x + 4, y + 4],
                         fill=(255, 255, 0, 255),  # bright yellow
                         outline=(0, 0, 0, 255), width=1)
    if red_dot is not None:
        ry, rx = red_dot
        # White-ringed dark-red dot, distinct from polygon color
        draw.ellipse([rx - 7, ry - 7, rx + 7, ry + 7],
                     outline=(255, 255, 255, 255), width=2,
                     fill=(180, 0, 0, 220))

    out = Image.alpha_composite(img, layer)
    # No burn-in label — the HTML page labels each variant in its card header,
    # and a burned label was hiding the polygon's NW corner.
    return out


# ---------------------------------------------------------------------------
# Variant generators
# ---------------------------------------------------------------------------
def variant_largest(mask: np.ndarray, tol_px: float):
    if not mask.any():
        return []
    labeled = measure.label(mask)
    largest = max(measure.regionprops(labeled), key=lambda r: r.area)
    region = labeled == largest.label
    contour = trace_contour(region)
    if contour is None:
        return []
    return [simplify(contour, tol_px)] if tol_px > 0 else [contour]


def variant_multi(mask: np.ndarray, tol_px: float = 3.0):
    out = []
    labeled = measure.label(mask)
    for region in measure.regionprops(labeled):
        if region.area < 2000:
            continue
        sub = labeled == region.label
        contour = trace_contour(sub)
        if contour is not None and len(contour) >= 4:
            out.append(simplify(contour, tol_px))
    return out


def variant_red_dot_anchor(mask: np.ndarray, red: tuple[int, int],
                           tol_px: float = 3.0):
    """Keep only the connected component that contains, or is closest to, the
    red marker."""
    if not mask.any():
        return []
    ry, rx = red
    labeled = measure.label(mask)
    # Did the marker land inside a sand blob?
    label_at_dot = labeled[ry, rx] if 0 <= ry < labeled.shape[0] and 0 <= rx < labeled.shape[1] else 0
    if label_at_dot == 0:
        # Pick the closest blob (centroid distance)
        regions = measure.regionprops(labeled)
        if not regions:
            return []
        best = min(regions, key=lambda r: (r.centroid[0] - ry) ** 2
                                          + (r.centroid[1] - rx) ** 2)
        label_at_dot = best.label
    region = labeled == label_at_dot
    contour = trace_contour(region)
    return [simplify(contour, tol_px)] if contour is not None else []


# ---------------------------------------------------------------------------
def main(image_path: str):
    p = Path(image_path)
    if not p.exists():
        print(f"ERROR: {p} does not exist")
        return 2
    rgb = np.array(Image.open(p).convert("RGB"))
    print(f"Loaded {p.name}  shape={rgb.shape}")

    red = detect_red_dot(rgb)
    print(f"  red marker:       {red}")

    mask_default = clean_mask(detect_sand(rgb, "default"))
    mask_loose   = clean_mask(detect_sand(rgb, "loose"))
    mask_strict  = clean_mask(detect_sand(rgb, "strict"))
    print(f"  sand mask px (loose/default/strict): "
          f"{mask_loose.sum():,} / {mask_default.sum():,} / {mask_strict.sum():,}")

    # Radius-limit mask: keep only pixels within R px of the red dot.
    def disk_mask(shape, center, radius):
        rr, cc = np.ogrid[:shape[0], :shape[1]]
        return (rr - center[0]) ** 2 + (cc - center[1]) ** 2 <= radius ** 2

    near_300 = disk_mask(rgb.shape[:2], red, 300) if red else None  # ~half the image diagonal
    near_180 = disk_mask(rgb.shape[:2], red, 180) if red else None  # tighter

    variants = [
        ("V1 — DEFAULT threshold, full strip",
         variant_largest(mask_default, tol_px=2.0)),

        ("V2 — LOOSE threshold (catches wet sand), full strip",
         variant_largest(mask_loose, tol_px=2.0)),

        ("V3 — STRICT threshold (dry sand only), full strip",
         variant_largest(mask_strict, tol_px=2.0)),

        ("V4 — DEFAULT + 300px radius from red dot",
         variant_largest(mask_default & near_300, tol_px=2.0) if red is not None else []),

        ("V5 — DEFAULT + 180px radius (tight around marker)",
         variant_largest(mask_default & near_180, tol_px=2.0) if red is not None else []),
    ]

    out_dir = p.parent
    for i, (label, contours) in enumerate(variants, start=1):
        if not contours:
            print(f"  V{i}: no polygon")
            continue
        img = overlay(rgb, contours, label, red_dot=red)
        out_path = out_dir / f"{p.stem}_v{i}.png"
        img.save(out_path)
        print(f"  V{i}: {len(contours)} contour(s), wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1
                  else "share/sunset clean.png"))
