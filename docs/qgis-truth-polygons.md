# Drawing ground-truth beach polygons in QGIS

Goal: hand-draw canonical sand-water polygons for ~50-100 representative
California beaches over Esri World Imagery, save them to `public.beach_polygons_truth`.
These serve two purposes:

1. **Canonical override** — wherever a truth polygon exists, it wins over
   auto-extracted polygons.
2. **Test set** — IoU against truth becomes the metric for any future
   threshold or algorithm change.

---

## One-time setup

### 1. Install QGIS

Free, open-source. https://qgis.org/en/site/forusers/download.html — grab
the long-term release ("LTR") for stability.

### 2. Connect to Supabase Postgres

QGIS reads/writes PostGIS directly. **Browser pane → PostgreSQL → New Connection.**

| Field | Value |
|---|---|
| Name | `dog-beach-AI` |
| **Service** | **leave BLANK** ← critical, see note below |
| Host | `aws-1-us-east-1.pooler.supabase.com` |
| Port | `5432` |
| Database | `postgres` |
| SSL mode | `require` |

In the **Authentication** tab → **Basic** sub-tab:

| Field | Value |
|---|---|
| User | `postgres.ehlzbwtrsxaaukurekau` |
| Password | (from `scripts/pipeline/.env` `SUPABASE_DB_PASSWORD`) |

Check **"Save Username"** and **"Save Password"** so QGIS doesn't prompt
every restart. Test connection → OK. The full connection string for reference
is in `supabase/.temp/pooler-url`.

> **Gotcha — "definition of service not found"**: if you typed anything in
> the **Service** field, QGIS uses libpq's service-name lookup (a file at
> `pg_service.conf` that doesn't exist by default) and ignores Host/Port.
> Leave Service blank.

### 3. Add Esri World Imagery basemap

**Browser pane → XYZ Tiles → New Connection.**

| Field | Value |
|---|---|
| Name | `Esri World Imagery` |
| URL | `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}` |
| Min/Max zoom | 0 / 19 |

### 4. Add Esri reference labels overlay

Same dialog, second connection.

| Field | Value |
|---|---|
| Name | `Esri Place Labels` |
| URL | `https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}` |
| Min/Max zoom | 0 / 19 |

This overlay is mostly transparent — only place names + roads draw. It sits
on top of the imagery so labels are readable.

---

## Project setup (do once, save as `.qgz` to reuse)

### Layer order (top to bottom in the Layers panel)

1. `Esri Place Labels` (XYZ Tiles)
2. `beach_polygons_truth` (your hand-drawn polygons — **edit this**)
3. `beaches_gold` (catalog points — for finding beaches)
4. `beach_polygons_auto_extracted_geojson` (existing extractions — reference only)
5. `osm_beach_polys` (OSM beach polygons — reference only)
6. `Esri World Imagery` (XYZ Tiles, basemap)

To add the Postgres layers:
- Browser → PostgreSQL → `dog-beach-AI` → public →
  - drag `beaches_gold` onto the canvas (or right-click → Add Layer)
  - drag `beach_polygons_auto_extracted` (or its `_geojson` view)
  - drag `osm_beach_polys`
  - drag `beach_polygons_truth` ← this is the one you'll edit

### Style suggestions

- **`beach_polygons_truth`**: bright green outline, 30% green fill — your work
- **`beach_polygons_auto_extracted`**: magenta outline, no fill — algorithm output
- **`osm_beach_polys`**: orange dashed, no fill — reference
- **`beaches_gold`**: red dot, 4px — catalog points

(Layer Styling panel → Symbology, Single Symbol)

---

## Drawing workflow

### Find a beach

Layers panel → `beaches_gold` → right-click → Open Attribute Table.
Sort by `name` or filter by `county_name = 'Orange'`.
Right-click a row → **Zoom to feature**. Map snaps to that beach.

### Draw the polygon

1. Click `beach_polygons_truth` layer to make it active.
2. **Toggle Editing** (pencil icon, or Ctrl+E).
3. **Add Polygon Feature** (key `.` or the polygon-with-yellow-star icon).
4. Click around the actual sand boundary. Right-click to finish.
5. QGIS asks for attributes:
   - `fid`: the gold fid you're drawing for (from beaches_gold attribute row)
   - `drawn_by`: your name / `franz`
   - `notes`: optional ("rocky cove with small sand strip")
   - `source`: leave default (`qgis_hand_drawn`)
   - leave `geom`, `area_m2`, `id`, `drawn_at` alone — auto-filled
6. **Save Layer Edits** (floppy-disk icon).
7. **Toggle Editing off** when done with that beach.

### Tips

- **Snap to vertices** (Project → Snapping Options → enable, tolerance 10px)
  helps when correcting a polygon you've already drawn.
- **20-50 vertices** is plenty for most beaches. Don't trace every wave.
- **Multi-part polygon**: for beaches with disjoint sand sections (e.g.,
  Seal Beach with the pier in the middle), draw multiple polygons and
  merge them: select features with rectangle, **Edit → Merge Selected
  Features**.
- **Re-draw**: if you mess up, delete the row from the attribute table
  and start fresh. The unique constraint on `fid` will reject duplicate
  rows for the same beach.

### Saving back

Edits hit Postgres immediately when you click Save Layer Edits — no upload
step. Verify with:

```sql
select count(*) from public.beach_polygons_truth;
select fid, area_m2, drawn_at from public.beach_polygons_truth order by drawn_at desc limit 5;
```

---

## Suggested coverage strategy

**Round 1: stubborn beaches we've been iterating on (~30 fids)**

Pull the list of currently-rejected pendings:

```sql
select ae.fid, bg.name
  from public.beach_polygons_auto_extracted ae
  join public.beaches_gold bg on bg.fid = ae.fid
 where ae.review_status = 'rejected' and bg.is_active and bg.is_scoreable
 order by bg.county_name, bg.name;
```

These are the highest-leverage targets — drawing once eliminates them
from the iteration loop.

**Round 2: Laguna pocket coves (~25 — single dense session)**

Laguna Beach has more discrete pocket-cove beaches per mile of coastline
than anywhere else in CA, and they cluster tightly enough that one map
view + one pan gets you through 3-4 beaches at a time. High value because:
- Each cove is small + visually distinct → easy to draw fast (2-3 min each)
- Topology is uniform (cliff-bound pocket sand) → algorithm baseline
- Many already in our rejected-pending queue

```sql
-- Pull Laguna candidates worth drawing in one session
select fid, name, lat, lon
  from public.beaches_gold
 where county_name = 'Orange'
   and is_active and is_scoreable
   and (lat between 33.50 and 33.57)
   and (lon between -117.81 and -117.74)  -- Laguna proper
 order by lat desc;
```

Targets (north → south):
- **Heisler Park area**: Picnic Beach, Diver's Cove, Fisherman's Cove,
  Rockpile, Crescent Bay
- **Downtown Laguna**: Main Beach, St. Ann's, Thalia Street, Oak Street,
  Anita Street, Pearl Street
- **Mid-Laguna**: Goff Island, Treasure Island, Wood's Cove, Pearl Street,
  Victoria Beach (the tower beach), Moss Point, Arch Beach
- **South Laguna**: 1000 Steps, Aliso, West Street, Table Rock

Set the QGIS map extent to roughly the Laguna coast bbox, lock the zoom
at z=18 for detail, then methodically work north-to-south. ~75 minutes
of focused work for the whole stretch.

**Round 3: topology spread (~20 more)**

Pick representatives across the rest of the coast:
- continuous strip endpoint (Sunset/Surfside boundary, Venice/Santa Monica)
- urban strand-style (Hermosa, Manhattan, Mission)
- river mouth / sandbar (Doheny, Bolsa Chica)
- cliff-base cove outside Laguna (Black's, El Matador)
- North Coast pocket (Stinson, Bolinas, Carmel)

This gives a balanced ~75-row test set spanning the failure modes.

---

## Using truth polygons later

Once we have ~30+ truth polygons:

- **IoU eval script** (todo): compute mean IoU between auto-extracted and truth
  for every algorithm change. Replace eyeballing with measurement.
- **Resolver gate** (todo): in `refresh_beach_polygons()`, prefer truth over
  auto-extracted. Truth polygons become the canonical layer.
- **Training data** (future): if we move to a CNN-based approach, these
  serve as the labeled set for fine-tuning.
