"""extract_beach_polygon.py — find sand-colored regions in a raster image,
extract the largest connected region as a polygon.

Tonight's experiment: take a screenshot of the gold-dogs-map (Sunset Beach
area), detect the sand region by color, output a pixel-coordinate polygon.
If the input has a known geographic bbox, can be georeferenced after.

Output:
  - <input>_overlay.png : visualization (original + detected polygon outline)
  - <input>_polygon.geojson : pixel-coordinate polygon as GeoJSON
  - stdout : summary stats (largest-region area in pixels, vertex count)
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from skimage import color, morphology, measure
from skimage.draw import polygon_perimeter


def detect_sand(rgb: np.ndarray) -> np.ndarray:
    """Returns a boolean mask where True == sand-like pixel.

    Strategy: HSV thresholding for warm-light-low-saturation pixels.
    Sand colors typically:
      - Hue: 30-65 degrees (yellow-tan-cream range)
      - Saturation: low to moderate (not vibrant)
      - Value: moderate to high (bright)
    """
    hsv = color.rgb2hsv(rgb)
    h = hsv[..., 0] * 360.0  # 0-360
    s = hsv[..., 1]          # 0-1
    v = hsv[..., 2]          # 0-1

    # Warm hue: 25-70 degrees (light yellow / tan / cream / soft orange)
    # Plus near-white pixels (low saturation, very bright)
    warm = (h >= 25) & (h <= 70) & (s >= 0.10) & (v >= 0.55)
    return warm


def mask_out_snipping_dialog(rgb: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Black rectangular overlay (Snipping Tool dialog) — exclude from
    processing so it doesn't create a black hole in the contour.

    Detect pixels that are very dark (the dialog body is near-black) and
    dilate the mask slightly to cover the dialog edge."""
    very_dark = (rgb.max(axis=-1) < 60)  # near-black pixels
    very_dark = morphology.binary_dilation(very_dark, morphology.disk(8))
    # Set those pixels to "False" in the sand mask (won't count as sand)
    return mask & ~very_dark


def find_largest_region(mask: np.ndarray) -> np.ndarray:
    """Take the largest connected component of the boolean mask."""
    # Clean: close small gaps, remove specks
    cleaned = morphology.binary_closing(mask, morphology.disk(5))
    cleaned = morphology.remove_small_objects(cleaned, min_size=2000)

    labeled = measure.label(cleaned)
    if labeled.max() == 0:
        return np.zeros_like(mask)
    regions = measure.regionprops(labeled)
    largest = max(regions, key=lambda r: r.area)
    return labeled == largest.label


def trace_contour(mask: np.ndarray):
    """Find the outer contour of the boolean mask. Returns (N, 2) array
    of (row, col) pixel coordinates."""
    contours = measure.find_contours(mask.astype(float), 0.5)
    if not contours:
        return None
    # Largest contour by length
    return max(contours, key=len)


def simplify_contour(contour: np.ndarray, tolerance_px: float = 3.0) -> np.ndarray:
    """Douglas-Peucker simplification to reduce vertex count."""
    return measure.approximate_polygon(contour, tolerance=tolerance_px)


def render_overlay(rgb: np.ndarray, contour: np.ndarray) -> Image.Image:
    """Draw the contour as a bright cyan line over the original image."""
    img = Image.fromarray(rgb).convert("RGBA")
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    pts = [(c, r) for r, c in contour]  # PIL is (x, y) = (col, row)
    if len(pts) >= 3:
        # Bright cyan outline + magenta fill
        draw.polygon(pts, outline=(0, 255, 255, 255), fill=(255, 0, 255, 70))
        # Thick outline for visibility
        for d in range(1, 4):
            draw.line(pts + [pts[0]], fill=(0, 255, 255, 255), width=3)
    return Image.alpha_composite(img, overlay)


def contour_to_geojson(contour: np.ndarray) -> dict:
    """Pixel-space GeoJSON Polygon (col, row) ordered as (x, y)."""
    coords = [[float(c), float(r)] for r, c in contour]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return {"type": "Polygon", "coordinates": [coords]}


def main(image_path: str):
    p = Path(image_path)
    if not p.exists():
        print(f"ERROR: {p} does not exist")
        return 2

    img = Image.open(p).convert("RGB")
    rgb = np.array(img)
    print(f"Loaded {p.name}  shape={rgb.shape}")

    sand_mask = detect_sand(rgb)
    print(f"  initial sand-colored pixels: {sand_mask.sum():,} ({sand_mask.mean()*100:.1f}%)")

    sand_mask = mask_out_snipping_dialog(rgb, sand_mask)
    print(f"  after masking dark overlays: {sand_mask.sum():,}")

    largest = find_largest_region(sand_mask)
    print(f"  largest connected region:    {largest.sum():,} pixels")

    contour = trace_contour(largest)
    if contour is None or len(contour) < 4:
        print("ERROR: no contour found")
        return 3
    print(f"  raw contour vertices: {len(contour)}")

    simplified = simplify_contour(contour, tolerance_px=3.0)
    print(f"  simplified vertices:  {len(simplified)}")

    overlay = render_overlay(rgb, simplified)
    out_overlay = p.with_name(p.stem + "_overlay.png")
    overlay.save(out_overlay)
    print(f"  wrote {out_overlay}")

    geojson = contour_to_geojson(simplified)
    out_geojson = p.with_name(p.stem + "_polygon.geojson")
    out_geojson.write_text(json.dumps(geojson, indent=2))
    print(f"  wrote {out_geojson}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1
                  else "share/sunset.png"))
