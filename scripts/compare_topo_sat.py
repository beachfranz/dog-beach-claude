"""compare_topo_sat.py — render the topo and satellite polygons for one
beach over the satellite imagery (as ground truth). Shows whether topo
is capturing more real sand or over-extending into non-sand."""
import argparse, json, math, subprocess
from pathlib import Path
import numpy as np
from PIL import Image, ImageDraw

import extract_hybrid as eh
import satellite_ab as sat


def query(sql):
    out = subprocess.check_output(['supabase', 'db', 'query', '--linked', sql], text=True)
    return json.loads(out[out.find('{'):out.rfind('}')+1])['rows']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fid', type=int)
    args = ap.parse_args()

    # Beach centroid
    rows = query(f"SELECT name, lat, lon FROM public.beaches_gold WHERE fid = {args.fid}")
    beach = rows[0]
    slug = ''.join(c.lower() if c.isalnum() else '_' for c in beach['name'])

    # Render satellite imagery as ground truth (z=17, 2048)
    rgb, T = sat.stitch_imagery(beach['lon'], beach['lat'], 17, 2048, 2048,
                                Path('tmp/esri_imagery_cache'))

    # Pull both polygons
    polys = query(f"""
      SELECT algorithm_version, ST_AsGeoJSON(geom)::text AS gj
        FROM public.beach_polygons_auto_extracted
       WHERE fid = {args.fid}
         AND algorithm_version IN ('sat_v1_z17_anchor', 'sand_v2_z16_anchor_grow')
       ORDER BY id DESC
    """)

    # Take latest of each version
    by_ver = {}
    for r in polys:
        if r['algorithm_version'] not in by_ver:
            by_ver[r['algorithm_version']] = json.loads(r['gj'])

    img = Image.fromarray(rgb).convert('RGBA')
    layer = Image.new('RGBA', img.size, (0,0,0,0))
    d = ImageDraw.Draw(layer)

    def render_poly(geom, rgb_color, label_pos):
        if geom['type'] == 'Polygon':
            polys = [geom['coordinates']]
        else:
            polys = geom['coordinates']
        for poly in polys:
            outer = poly[0]
            pts = [(int(round(x)), int(round(y)))
                   for x, y in (eh.lonlat_to_pixel(p[0], p[1], T) for p in outer)]
            # White halo + colored line
            d.line(pts + [pts[0]], fill=(255, 255, 255, 220), width=8)
            d.line(pts + [pts[0]], fill=rgb_color + (255,), width=4)

    if 'sand_v2_z16_anchor_grow' in by_ver:
        render_poly(by_ver['sand_v2_z16_anchor_grow'], (255, 165, 0), 'topo')
    if 'sat_v1_z17_anchor' in by_ver:
        render_poly(by_ver['sat_v1_z17_anchor'], (14, 165, 233), 'satellite')

    # Catalog point
    px, py = eh.lonlat_to_pixel(beach['lon'], beach['lat'], T)
    d.ellipse([px-9, py-9, px+9, py+9], outline=(255,255,255,255), width=3,
              fill=(255, 0, 0, 230))

    # Legend
    d.rectangle([8, 8, 280, 70], fill=(0, 0, 0, 200))
    d.line([(20, 26), (60, 26)], fill=(255, 165, 0, 255), width=4)
    d.text((70, 18), f"topo (z=16): {beach['name']}", fill=(255,255,255,255))
    d.line([(20, 50), (60, 50)], fill=(14, 165, 233, 255), width=4)
    d.text((70, 42), f"satellite (z=17)", fill=(255,255,255,255))

    out = Image.alpha_composite(img, layer)
    out_path = Path('share') / f'{slug}_topo_vs_sat.png'
    out.save(out_path)
    half = out.resize((out.width // 2, out.height // 2), Image.LANCZOS)
    half.save(Path('share') / f'{slug}_topo_vs_sat_small.png')
    print(f'Wrote {out_path}')


if __name__ == '__main__':
    main()
