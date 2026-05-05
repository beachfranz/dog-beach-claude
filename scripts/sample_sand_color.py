"""Quick sampler — print HSV stats around the red catalog dot to figure out
what sand actually looks like in the Esri World_Topo basemap."""
import sys
from pathlib import Path
import numpy as np
from PIL import Image
from skimage import color, morphology, measure

p = Path(sys.argv[1] if len(sys.argv) > 1 else "share/sunset clean.png")
rgb = np.array(Image.open(p).convert("RGB"))
print(f"shape: {rgb.shape}")

# Find red dot
r, g, b = rgb[..., 0], rgb[..., 1], rgb[..., 2]
red = (r > 180) & (g < 90) & (b < 90)
labeled = measure.label(red)
if labeled.max() == 0:
    print("no red dot")
    sys.exit(1)
biggest = max(measure.regionprops(labeled), key=lambda x: x.area)
ry, rx = map(int, biggest.centroid)
print(f"red dot at  (row={ry}, col={rx})")

# Sample 30x30 box around the dot — this should be water + sand mostly
box = rgb[max(0, ry-50):ry+50, max(0, rx-30):rx+60]
hsv = color.rgb2hsv(box)
h = hsv[..., 0] * 360
s = hsv[..., 1]
v = hsv[..., 2]

print("\nBox stats around red dot:")
print(f"  R: min={box[...,0].min()} max={box[...,0].max()} mean={box[...,0].mean():.0f}")
print(f"  G: min={box[...,1].min()} max={box[...,1].max()} mean={box[...,1].mean():.0f}")
print(f"  B: min={box[...,2].min()} max={box[...,2].max()} mean={box[...,2].mean():.0f}")
print(f"  H: min={h.min():.0f} max={h.max():.0f} mean={h.mean():.0f}")
print(f"  S: min={s.min():.2f} max={s.max():.2f} mean={s.mean():.2f}")
print(f"  V: min={v.min():.2f} max={v.max():.2f} mean={v.mean():.2f}")

# Now sample two specific pixels: just east and just west of the red dot
# (east should be sand, west should be water)
print(f"\nPixel just EAST of red dot (sand?):  RGB={tuple(rgb[ry, rx+15])}  H={h.flat[h.size//2]:.0f}")

# Histogram of hues + saturations + values across whole image
hsv_all = color.rgb2hsv(rgb)
h_all = hsv_all[..., 0] * 360
s_all = hsv_all[..., 1]
v_all = hsv_all[..., 2]
print("\nWhole-image hue histogram (10° bins):")
hist, _ = np.histogram(h_all, bins=36, range=(0, 360))
for i, c in enumerate(hist):
    if c > 1000:
        print(f"  {i*10:>3}-{(i+1)*10:>3}°: {c:,}")

# Try a few thresholds for sand and see how many pixels survive
print("\nThreshold experiments:")
for h_lo, h_hi, s_min, v_min in [
    (25, 70, 0.10, 0.55),  # original
    (25, 70, 0.05, 0.55),  # looser saturation
    (30, 60, 0.05, 0.50),  # tighter hue, looser everything else
    (20, 80, 0.05, 0.50),  # all warm + bright
    (30, 60, 0.10, 0.70),  # bright tan only
]:
    m = (h_all >= h_lo) & (h_all <= h_hi) & (s_all >= s_min) & (v_all >= v_min)
    print(f"  H[{h_lo}-{h_hi}] S>={s_min} V>={v_min}: {m.sum():,} pixels ({m.mean()*100:.1f}%)")
