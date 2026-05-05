"""Sample known sand patches in the Seal Beach satellite image to figure
out what threshold works. Pixel coords by visual inspection of the rendered
basemap (image is 2048×2048)."""
import numpy as np
from PIL import Image
from skimage import color

rgb = np.array(Image.open('share/seal_beach_imagery.png').convert('RGB'))
hsv = color.rgb2hsv(rgb)
h = hsv[..., 0] * 360
s = hsv[..., 1]
v = hsv[..., 2]

# Visually sampled points (col, row) from the imagery rendering above
samples = [
    ('A. dry sand mid-beach east (bright)', 1300,  500),
    ('B. dry sand mid-beach east (mid)',    1300,  700),
    ('C. dry sand near waterline (wet?)',   1100, 1000),
    ('D. dry sand far east',                1700,  500),
    ('E. dry sand west of pier',             550,  600),
    ('F. very-bright sand strip',           1200,  300),
    ('G. water (control)',                  1000, 1500),
    ('H. street/concrete (control)',        1700,  100),
    ('I. roof (control, red)',              1900,  300),
    ('J. dark wet sand at edge',             450, 1200),
]

print(f'{"label":42} {"px":12} {"RGB":18} {"H":>4} {"S":>5} {"V":>5}')
print('-' * 110)
for label, x, y in samples:
    if not (0 <= x < rgb.shape[1] and 0 <= y < rgb.shape[0]):
        continue
    box = rgb[max(0,y-3):y+4, max(0,x-3):x+4]
    h_box = h[max(0,y-3):y+4, max(0,x-3):x+4]
    s_box = s[max(0,y-3):y+4, max(0,x-3):x+4]
    v_box = v[max(0,y-3):y+4, max(0,x-3):x+4]
    rgb_mean = tuple(int(c) for c in box.reshape(-1,3).mean(0))
    print(f'{label:42} ({x:>4},{y:>4})  {str(rgb_mean):18} '
          f'{h_box.mean():>4.0f} {s_box.mean():>5.2f} {v_box.mean():>5.2f}')

# Threshold experiments
print('\nThreshold experiments (% of image):')
configs = [
    ('current dry+foam+wet',
     ((h>=30)&(h<=70)&(s>=0.05)&(s<=0.40)&(v>=0.60)) |
     ((s<=0.10)&(v>=0.85)) |
     ((h>=25)&(h<=60)&(s>=0.15)&(s<=0.45)&(v>=0.30)&(v<=0.65))),
    ('looser warm bright',
     (h>=25)&(h<=80)&(s>=0.04)&(s<=0.45)&(v>=0.55)),
    ('only V>=0.55 + warm',
     (h>=20)&(h<=80)&(v>=0.55)&(s<=0.45)),
    ('green hue tolerant',
     (h>=15)&(h<=90)&(s>=0.05)&(s<=0.45)&(v>=0.55)),
]
for name, mask in configs:
    print(f'  {name:35}: {mask.sum():>9,} px ({mask.mean()*100:.1f}%)')
