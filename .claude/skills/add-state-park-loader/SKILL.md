---
name: add-state-park-loader
description: Use this skill when adding a new state-park photo loader to bring a US state's official park gallery into beach_photos. Triggers include "write a state-park loader for <STATE>", "add NH state parks photos", "build a state parks loader", or any work involving scripts/loaders/<state>.py + scripts/state_park_urls.json. DO NOT use for non-state-park photo sources (Flickr / Wikimedia / Tavily — those are state-agnostic loaders that already exist), for dog-park photos (dp_photos_* ops in dog-park-state-launch), or for non-photo state-park enrichment (codify-state covers state-agency dog policy).
---

# add-state-park-loader — three pieces, ship all three

The trap: shipping the loader script + state_park_urls.json entry without the photo_source_type row. Loader runs, finds 240 photos, INSERTs zero (silent FK violation), subprocess exits 0, the next-state-launch detector catches it but a less-careful caller silently loses the data.

NH 2026-05-23 shipped 2/3 and bit the next caller. This skill exists to make sure the third piece always lands.

## The three pieces (ALL must ship in the same session)

### 1. `scripts/loaders/<state_code_lower>.py` — the loader class

Subclass `scripts.loaders._base.StateParksLoader`. Required methods: `iter_parks()` → yields `ParkInfo`, `iter_photos(park)` → yields `Photo`. Pattern selection driven by the agency's CMS:

| Pattern | Used by | Notes |
|---|---|---|
| `sharepoint_static` | MD DNR | Static SharePoint pages |
| `wordpress_slug` | DE DNREC | WordPress with park-name slugs |
| `kentico_cms` | NH State Parks | Kentico GUID-keyed `/getmedia/` + sized `?width=N&height=M` |
| `drupal_slug` | AL (deferred) | Drupal `/parks/<slug>/` |
| `aspx_classic` | CDPR (CA) | Legacy ASP.NET `?page_id=N` |

Inserts rows into `public.beach_photos` with `source='<src>'`. The `<src>` must match piece 3's `id`.

**sys.path bootstrap is mandatory** per [[sys-path-bootstrap-for-common-imports]]:
```python
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.loaders._base import StateParksLoader, ParkInfo, Photo
```

Without this, invoking `python scripts/loaders/nhsp.py` (the pipeline-runner pattern) breaks with ModuleNotFoundError.

### 2. `scripts/state_park_urls.json` registry entry

Schema (see existing entries for examples):
```json
"<STATE>": {
  "agency": "Full agency name",
  "url": "https://authoritative-listing-url.gov/",
  "loader": "scripts/loaders/<state_code_lower>.py",
  "loader_class": "<StateCode>Loader",
  "pattern": "<pattern_name_from_table_above>",
  "added": "YYYY-MM-DD"
}
```

If the state DOESN'T have a state-park-specific loader (Wikimedia + websearch are good enough), use `"loader_deferred": true` with a `notes` field explaining why. Example: AL deferred because Drupal slug pattern + state has no codified leash law.

### 3. `supabase/migrations/<YYYYMMDD>_photo_source_type_<src>.sql` — FK target row

**This is the piece that bit us.** Without it, every `INSERT INTO beach_photos (source, …)` raises a FK violation.

```sql
-- supabase/migrations/20260604_photo_source_type_nhsp.sql
INSERT INTO public.photo_source_type (
  id, label, source_kind, default_license,
  attribution_template, loader_script, notes
) VALUES (
  'nhsp',
  'NH State Parks (NH Division of Parks and Recreation)',
  'agency',
  'gov_public_domain',
  'Photo: NH State Parks',
  'scripts/loaders/nhsp.py',
  'Kentico CMS; sitemap-driven discovery; two URL conventions on same domain'
)
ON CONFLICT (id) DO UPDATE SET
  label = EXCLUDED.label,
  loader_script = EXCLUDED.loader_script,
  notes = EXCLUDED.notes;
```

**Apply it in the same session** — don't leave it for "next deploy":
```bash
supabase db query --linked -f supabase/migrations/<YYYYMMDD>_photo_source_type_<src>.sql
```

Verify the row landed:
```sql
SELECT id, label, loader_script FROM public.photo_source_type WHERE id='<src>';
```

## Step-by-step

1. **Investigate the agency's site** — what CMS? Is there a sitemap? Do park URLs use slugs or numeric IDs? What's the image URL pattern? Are there activity-icon noise images mixed with hero photos? Use Playwright dev tools.

2. **Pick a pattern** from the table above (or invent one if novel — document in the loader's docstring).

3. **Subclass `StateParksLoader`** in `scripts/loaders/<state_code_lower>.py`:
   - `iter_parks()` — sitemap parse OR enumeration
   - `iter_photos(park)` — fetch park page, parse img tags, filter by relevance heuristic
   - Filter strategy: drop chrome subpaths, drop icon-size images (<400 on either axis), keep hero banners unconditionally, keep getmedia images with ≥4-char park-name token in filename

4. **Add the `state_park_urls.json` entry** with `loader` + `loader_class` + `pattern` + `added` date.

5. **Write + apply the `photo_source_type` migration** in the same session. Verify the row.

6. **Smoke-test the loader on one park** before bulk-running:
```bash
.venv-pipeline/Scripts/python.exe scripts/loaders/<state_code_lower>.py --park <one-park-slug> --dry-run
```
Expected: prints N photos found, 0 INSERTed (dry run). If you see "FK violation," piece 3 didn't land.

7. **Bulk-run for the state**:
```bash
.venv-pipeline/Scripts/python.exe scripts/loaders/<state_code_lower>.py --state <STATE> --apply > tmp/<state>_state_park_photos.log 2>&1 &
```

8. **Verify rows landed** (the silent-failure check):
```sql
SELECT count(*) FROM public.beach_photos
WHERE source='<src>' AND created_at > now() - interval '2 hours';
```
If this returns 0 and the loader log says "found 240 photos," piece 3 is missing.

9. **Trigger curation if appropriate** — `get_beach_photos_diverse` requires `lat IS NOT NULL` per [[dp-curate-vision-only-beach-curate-lat-gated]]. If your loader writes geo-anchored photos (sitemap → park → beach centroid), curation will pick them up automatically. Otherwise run `backfill_nogeo_photo_centroid.py` to unlock them.

## After-launch verification (the `verify-sweep` step)

Pick 5 beaches in the new state that should now have state-park photos. Open `beach.html?fid=<N>` and check:
- Photo block renders state-park photos (not just Flickr/Wikimedia)
- Attribution shows the agency name
- Quality is reasonable (no activity icons, no chrome banners)
- `has_dog=true` rate is healthy per [[apply-loader-bias-to-beach-photos]]

If quality is poor, iterate on the filter heuristic in piece 1 — don't skip the filter.

## Common gotchas

1. **Forgetting piece 3** — the canonical failure mode. NH 2026-05-23 + several others before that. CHECK the table-row exists before celebrating.
2. **AVG MITM** per [[avg-antivirus-https-mitm]] — Franz's machine intercepts HTTPS. `pip install pip-system-certs` in venv if `requests` fails but `curl` works.
3. **403 from agency site** per [[403-means-playwright-skip-ua-tricks]] — jump to Playwright; don't iterate on UA.
4. **Activity-icon noise** — Kentico's `/getmedia/<guid>/<filename>.aspx?width=46&height=46` is the tell. Filter by dimension < 400 on either axis OR by single-word activity-token filenames.
5. **Park-name match too loose** — "Beach" in filename matches every park. Require ≥4-char token from `ParkInfo.name`.
6. **Loader pattern named wrong** — keep `pattern` lowercase + snake_case for cross-loader grep ("sharepoint_static", not "SharepointStatic").

## Templates to clone

| Pattern | File | Lift |
|---|---|---|
| sharepoint_static | `scripts/loaders/md_dnr.py` | Highest |
| wordpress_slug | `scripts/loaders/dnrec_de.py` | Medium |
| kentico_cms | `scripts/loaders/nhsp.py` | Medium |
| ASPX classic | `scripts/loaders/cdpr.py` | Low (CDPR is gnarly) |
| OPRD/WSPRC pattern | `scripts/loaders/oprd.py`, `wsprc.py` | Medium |

## Per Franz preferences

- [[state-park-loader-three-pieces]] — the parent rule. Pieces 1+2+3 ship together.
- [[promote-ad-hoc-tools-to-process]] — if you discover a new CMS pattern, add it to the table here AND a memory pin.
- [[this-is-a-dog-app]] — bias the loader to dog-relevant photos when possible (filename tokens like "dog", "beach", "shoreline"); skip "memorial", "interior", "icon" matches.
- [[claim-tested-without-end-state-verification]] — SQL `count(*) > 0` is not enough; click `beach.html` for 3-5 fids in the new state to see the photo block.
