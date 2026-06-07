---
name: wikimedia
description: Use this skill when running, debugging, or extending the Wikimedia Commons photo loader. Triggers include "rerun wikimedia for <state>", "why does <beach> have no wikimedia photos", "add a category candidate to the wikimedia loader", "wikimedia geosearch dearth", "extract wikimedia photos for <fid>", "what category does Hanauma Bay match on Commons". Covers the geosearch-vs-category-lookup architecture and the dog-filter carve-out for category members. DO NOT use for Tavily websearch (see `websearch` skill), Flickr, NPS / CDPR / OPRD / WSPRC galleries, or for cross-state photo-coverage strategy (those have different loaders and different shape).
---

# wikimedia — Commons photo loader (geosearch + Category-based)

The Wikimedia loader has two complementary input paths:

1. **Geosearch** — finds Commons files with geo-coordinates within `--radius` meters (default 500m) of the beach centroid. Returns whatever's geo-tagged in radius: boats, tents, panoramas, occasional dogs. Yield is low and content-blind. Has_dog rate on raw geosearch hits: ~1.7% catalog-wide.

2. **Category-based lookup** (2026-06-07) — for each beach, tries 3–5 candidate `Category:<Name>` page titles, fetches their File: members. Category membership is a stronger relevance signal than geo-distance because a Commons curator vetted "this photo is OF this beach." Unlocks famous-beach Categories like `Category:Popham_Beach_State_Park` (32 members) and `Category:Lanikai_Beach` (79 members) whose members typically aren't geo-tagged within 500m.

The two paths are merged + deduped by `pageid`. The dog-word filter applies only to the geosearch path (Category members bypass it — see "Dog-filter carve-out" below).

## Run shapes

```bash
# All tier-1+2 beaches in a state (orchestrator photo phase fires this)
.venv-pipeline/Scripts/python.exe scripts/load_wikimedia_commons_photos.py --states ME

# Specific fids
.venv-pipeline/Scripts/python.exe scripts/load_wikimedia_commons_photos.py --fids 14351,14975,14323

# Pilot 20 to sanity-check a state's coverage
.venv-pipeline/Scripts/python.exe scripts/load_wikimedia_commons_photos.py --pilot 20

# Disable category lookup (geosearch-only — back-compat / cost mode)
... --no-category-lookup

# Disable dog filter (scenic-placeholder backfills)
... --no-dog-filter

# Wider geosearch radius
... --radius 1500
```

## Architecture quick map

| Function | What it does |
|---|---|
| `commons_geosearch(lat, lng, radius_m)` | Hits Commons `list=geosearch`. Returns dicts with `pageid, title, lat, lon, dist`. |
| `derive_candidate_categories(name, state)` | Generates 3–5 `Category:<Name>` candidates from beach display_name + state. Skips generic single-word names (Sand Beach, Long Beach). |
| `commons_category_exists(title)` | Cheap one-call probe via `action=query&titles=<Category>`. |
| `commons_category_members(title, limit=80)` | Fetches up to 80 File: namespace members via `generator=categorymembers`. Returns geosearch-shaped dicts with `lat=None, lon=None` (the None is what distinguishes them downstream). |
| `commons_imageinfo(pageids)` | Batched metadata fetch. **Chunks at 50 pageids/call** — Commons rejects unauthenticated requests over 50 with an empty pages dict (no error). |
| `rank_and_pick(...)` | Applies dog filter, photographer blocklist, name-match scoring, distance scoring, tier cap. |

## The dog-filter carve-out

`require_dog=True` (default) makes `rank_and_pick` reject hits whose title or description lacks a dog token (dog/puppy/retriever/labrador/...). This is right for **geosearch hits** — Commons photos near beach centroids are dominated by scenic content the curate gate would have to drop visually anyway.

But for **Category members** the filter would reject nearly everything (Popham Beach SP has 32 members, 0 mention dogs in title/description). The carve-out distinguishes them by `g.get("lat") is None` — Category members have no geo coords, geosearch hits do. Category members skip the dog filter because their relevance is established by Category membership.

If `--no-dog-filter` is passed both paths skip the filter (back-compat for scenic-placeholder backfills).

## Candidate Category derivation

`derive_candidate_categories("Old Orchard Beach", "ME")` returns:

```python
['Category:Old_Orchard_Beach',
 'Category:Old_Orchard_Beach_(Maine)',
 'Category:Old_Orchard_Beach,_Maine',
 'Category:Old_Orchard_Beach_State_Park']
```

Priority order. Caller tries each via `commons_category_exists()` and uses the first that exists. For Old Orchard, the third (`,_Maine`) is the canonical Commons title.

**Adding a new variation when famous beaches miss:** the 2026-06-07 smoke test showed Hanauma + Hapuna missed because their Commons Categories are `Category:Hanauma_Bay` and `Category:Hapuna_Beach_State_Recreation_Area` — names that don't match what `derive_candidate_categories` generates from `display_name`. To add a new variation:

1. Find the canonical Category page on Commons (search `https://commons.wikimedia.org/wiki/Special:Search?search=Category%3A<beach name>`)
2. Edit `derive_candidate_categories` to append the new pattern. Examples:
   - Strip "Beach" suffix and try `Category:<Name>_Bay` (Hanauma Bay)
   - Try `Category:<Name>_State_Recreation_Area` (Hapuna)
   - Try `Category:<Name>` without state suffix (when famous enough to be unambiguous)
3. Re-run for the canary fid; verify the new candidate is picked and members fetch.

## When wikimedia is the wrong lever

The Wikimedia + dog filter combo will always be low-yield on coastal beaches because Commons content is curated by tourists and editors, who post scenic photos. **The dog filter eliminates ~98% of pre-filter content**, and even with Category lookup the surviving fraction is small relative to Tavily websearch (32.6% dog-content yield catalog-wide).

Reach for Tavily when you need volume; reach for Wikimedia when you need attribution-clean content for famous beaches that already have Tavily coverage.

## Common gotchas

1. **Commons API 50-pageid cap on imageinfo.** Hitting it silently returns empty pages. `commons_imageinfo` chunks transparently — don't bypass.
2. **`derive_candidate_categories` skips generic names** ("Sand Beach", "Long Beach", "Main Beach"). They'd collide with non-beach categories. If a beach really IS named `Sand Beach` and it's famous, use `display_name_override` on `beaches_gold` to give it a disambiguating name.
3. **AVG Antivirus + AWS pooler intercept** — pip-system-certs is installed in `.venv-pipeline`; don't worry about cert errors. If `urlopen` to Commons starts failing, see `[[avg-antivirus-https-mitm]]`.
4. **Don't run with `--full --no-dog-filter --no-category-lookup` against the whole catalog** — that's the old geosearch-only behavior and will load thousands of landscape photos no one will curate. The new Category path is the leverage; keep it ON.

## Per Franz preferences

- [[this-is-a-dog-app]] — every loader bias defaults to dog-centric. The dog filter on the geosearch path embodies this. The Category carve-out is the explicit, scoped exception (curator-vetted relevance > word-match heuristic).
- [[no-unilateral-architectural-decisions]] — don't drop the dog filter unilaterally, don't widen the geosearch radius without Franz's nod (existing 500m is the canonical setting).
- [[apply-loader-bias-to-beach-photos]] — same family as the Tavily "dogs playing" cue. Wikimedia's bias mechanism is the dog-word post-filter; Tavily's is the query rewrite.
- [[paired-functions-port-fixes-both-sides]] — Wikimedia loader changes apply equally to `--entity beach` and `--entity dog_park`. Both share the same code path; no port required.

## Anchors

- `scripts/load_wikimedia_commons_photos.py` — the loader.
- `scripts/_photo_filters.py:ENTITIES` — entity-table + FK + select-field registry shared with the websearch + Flickr loaders.
- `scripts/run_state_pipeline.py:action_photos_wikimedia` — orchestrator action that fires the loader per state.
- Commons API base: `https://commons.wikimedia.org/w/api.php`. Category search test:  
  `https://commons.wikimedia.org/wiki/Special:Search?search=Category%3A<beach name>`
