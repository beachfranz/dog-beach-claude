"""Sample wet sand pixels in the East Beach satellite image to check if
the current thresholds are catching them."""
from pathlib import Path
import numpy as np
from PIL import Image
from skimage import color

# Re-stitch (cached) so we have the same canvas as the extraction run
import math, subprocess, json
import extract_hybrid as eh
import satellite_ab as sat

beach = {'lat': 33.7391601, 'lon': -118.3752663}
rgb, T = sat.stitch_imagery(beach['lon'], beach['lat'], 17, 2048, 2048,
                            Path('tmp/esri_imagery_cache'))
hsv = color.rgb2hsv(rgb)
h, s, v = hsv[..., 0]*360, hsv[..., 1], hsv[..., 2]

# East Beach cove is in lower-half of the image. Let me sample at known
# beach pixel locations. Catalog point is ~center; sand cove is south.
# Try multiple points along what visually looks like the wet/dry boundary.
samples = [
    ('A. dry sand mid-cove',     1010, 1300),
    ('B. dry sand near catalog', 1024, 1024),  # the catalog point itself
    ('C. wet sand band',         1010, 1380),
    ('D. wet sand deeper',       1010, 1410),
    ('E. waterline',             1010, 1440),
    ('F. shallow water',         1000, 1480),
    ('G. cliff/grass north',     1010,  900),
    ('H. residential roof',      1100,  600),
    ('I. dry sand SE',           1180, 1340),
    ('J. wet sand SE',           1200, 1395),
]
print(f'{"label":35} {"px":12} {"RGB":18} {"H":>4} {"S":>5} {"V":>5}  matches')
print('-' * 100)

# Current thresholds from satellite_ab.detect_sand_imagery
def matches(h_v, s_v, v_v):
    dry = (38 <= h_v <= 60) and (0.08 <= s_v <= 0.28) and (v_v >= 0.70)
    wet = (30 <= h_v <= 55) and (0.15 <= s_v <= 0.40) and (0.45 <= v_v <= 0.72)
    if dry: return 'DRY'
    if wet: return 'WET'
    return '-'

for label, x, y in samples:
    if not (0 <= x < rgb.shape[1] and 0 <= y < rgb.shape[0]):
        print(f'{label:35} OUT OF FRAME')
        continue
    box = rgb[max(0,y-3):y+4, max(0,x-3):x+4]
    h_box = h[max(0,y-3):y+4, max(0,x-3):x+4]
    s_box = s[max(0,y-3):y+4, max(0,x-3):x+4]
    v_box = v[max(0,y-3):y+4, max(0,x-3):x+4]
    rgb_mean = tuple(int(c) for c in box.reshape(-1,3).mean(0))
    print(f'{label:35} ({x:>4},{y:>4})  {str(rgb_mean):18} '
          f'{h_box.mean():>4.0f} {s_box.mean():>5.2f} {v_box.mean():>5.2f}  '
          f'{matches(h_box.mean(), s_box.mean(), v_box.mean())}')
