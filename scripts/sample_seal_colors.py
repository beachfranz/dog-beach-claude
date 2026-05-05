"""Sample colors in the stitched Seal Beach basemap at three known regions:
  A. Inside the OSM beach polygon (should be the canonical sand color)
  B. Near the red dot (catalog centroid — also sand)
  C. The eastern wetland (where the wrong sand polygon landed)
Compare HSV ranges so we can pick a threshold that includes A but not C.
"""
from pathlib import Path
import numpy as np
from PIL import Image
from skimage import color
import json, math, subprocess

# --- Re-derive the same transform the hybrid script used ---
TILE = 256
def lonlat_to_tile(lon, lat, z):
    n = 2**z
    lat_r = math.radians(lat)
    return ((lon + 180) / 360 * n,
            (1 - math.asinh(math.tan(lat_r)) / math.pi) / 2 * n)

zoom = 16
center_lat, center_lon = 33.7377, -118.1063
size = 1280
cx, cy = lonlat_to_tile(center_lon, center_lat, zoom)
x0 = cx - size / (2 * TILE)
y0 = cy - size / (2 * TILE)
T = {'x0': x0, 'y0': y0, 'z': zoom, 'w': size, 'h': size}

def lonlat_to_px(lon, lat):
    tx, ty = lonlat_to_tile(lon, lat, T['z'])
    return (tx - T['x0']) * TILE, (ty - T['y0']) * TILE

# Load basemap
rgb = np.array(Image.open('share/seal_beach_basemap.png').convert('RGB'))
hsv = color.rgb2hsv(rgb)
h, s, v = hsv[..., 0] * 360, hsv[..., 1], hsv[..., 2]

# Three sample points
samples = [
    ('A. OSM beach centroid',          -118.1063, 33.7377),  # red dot location
    ('B. east end of beach (sand)',    -118.0985, 33.7350),  # known sand
    ('C. wetland east of beach',       -118.0950, 33.7378),  # the wetland
    ('D. near jetty/sand spit east',   -118.0930, 33.7395),
    ('E. west end of beach (sand)',    -118.1130, 33.7395),  # west sand
    ('F. somewhere north (street)',    -118.1063, 33.7440),  # street
]

print(f'{"label":40} {"px(x,y)":15} {"RGB":15} {"H":6} {"S":6} {"V":6}')
print('-' * 100)
for label, lon, lat in samples:
    px, py = lonlat_to_px(lon, lat)
    pxi, pyi = int(px), int(py)
    if pxi < 0 or pxi >= rgb.shape[1] or pyi < 0 or pyi >= rgb.shape[0]:
        print(f'{label:40} OUT OF FRAME')
        continue
    # Average over a 5x5 box for stability
    rgb_box = rgb[max(0,pyi-2):pyi+3, max(0,pxi-2):pxi+3]
    h_box = h[max(0,pyi-2):pyi+3, max(0,pxi-2):pxi+3]
    s_box = s[max(0,pyi-2):pyi+3, max(0,pxi-2):pxi+3]
    v_box = v[max(0,pyi-2):pyi+3, max(0,pxi-2):pxi+3]
    rgb_mean = tuple(int(c) for c in rgb_box.reshape(-1, 3).mean(axis=0))
    print(f'{label:40} ({pxi:>4},{pyi:>4})    {str(rgb_mean):15} '
          f'H={h_box.mean():>5.0f} S={s_box.mean():>4.2f} V={v_box.mean():>4.2f}')
