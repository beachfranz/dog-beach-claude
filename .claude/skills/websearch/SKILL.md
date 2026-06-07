---
name: websearch
description: Use this skill when running, debugging, or iterating on the Tavily websearch photo loader. Triggers include "rerun websearch for <state>", "preview the websearch photos for <state>", "block <host> in websearch", "why does <state> have no websearch photos", "show me 50 stratified dog parks", "websearch dearth", "websearch host pollution". Covers the diagnosis → sweep → preview → blocklist iteration loop for both beaches and dog parks. DO NOT use for Wikimedia, Flickr, NPS, CDPR, OPRD, WSPRC photo loaders — those have different shapes (geosearch vs keyword search vs gallery scrape).
---

# websearch — Tavily image search loader + visual QA loop

The Tavily websearch loader is the highest-dog-rate photo source we have (32.6% catalog-wide vs 1.7% on Wikimedia). The bias comes from a single "dogs playing" query token appended at load time per `[[apply-loader-bias-to-beach-photos]]`. When it works, it produces dog-relevant photos with breed-specific descriptions ("Cavalier King Charles Spaniel runs along the shore"). When it fails it fails silently — silent entity-refactor bugs and silent host-pollution-renders-broken bugs both look identical at the SQL boundary.

The workflow here is opinionated: **descriptions over vision tags, sequential over parallel, preview over screenshots**.

## Step 1 — Diagnosis (only if there's a coverage gap)

If a state has 0 or surprisingly few websearch photos, check this class of bug first BEFORE running the loader again:

```sql
-- Per-state websearch coverage. Anomaly: a state with daily/hourly beaches
-- having ~0 websearch photos when the catalog shows >100/day load rate.
SELECT b.state,
       COUNT(*) AS scoreable,
       COUNT(DISTINCT bp.arena_group_id) FILTER (WHERE bp.source='websearch') AS w_websearch
FROM public.beaches_gold b
LEFT JOIN public.beach_photos bp ON bp.arena_group_id = b.fid
WHERE b.is_active AND b.scoring_tier IN ('daily','hourly')
GROUP BY 1 ORDER BY 2 DESC;

-- When did websearch photos last land per state?
SELECT b.state, MAX(bp.loaded_at) AS last_loaded
FROM public.beach_photos bp
JOIN public.beaches_gold b ON b.fid = bp.arena_group_id
WHERE bp.source = 'websearch'
GROUP BY 1 ORDER BY 2 DESC NULLS LAST;
```

**Silent failure modes that match this signature** (each was a real bug 2026-06-06):

| Symptom | Root cause | Fix |
|---|---|---|
| Action returns `rows_affected=0` for every state launched after 2026-05-26 | `action_photos_websearch` in `run_state_pipeline.py` missing `--entity beach` (loader defaults to `dog_park`) | `extra_args=['--entity', 'beach', '--per-entity', '10']` |
| Wikimedia loader returns 0 targets for `--fids` of valid beaches | `_shape()` reads `r.get("lat")` but `select_fields` returns `nav_lat` | Read `nav_lat`/`nav_lon` with fallback to lat/lon |
| Wikimedia `--states`/`--pilot`/`--full` errors PostgREST 42703 | `is_scoreable` filter hardcoded but column lives on `dog_parks_gold`, not `beaches_gold` | Branch the scoreable filter by `args.entity` |

These three are the entity-refactor bug family. After ANY photo-loader refactor, audit per-source `loaded_at` distribution by `DATE_TRUNC('day')` for a drop-cliff — if loads drop from N/day to ~0 around a known refactor date, this class is the suspect.

## Step 2 — Run the sweep

The sweep is a per-state invocation of `run_state_pipeline.py` scoped to just the `photos_websearch` phase. Use `--phase-from photos_websearch --until-phase photos_websearch` so neither the upstream phases nor the downstream `photos_tag` / `photos_curate` run.

```bash
for st in MA WA MI OH NH RI AL UT DE; do
  echo "=== START $st $(date +%H:%M:%S) ==="
  .venv-pipeline/Scripts/python.exe scripts/run_state_pipeline.py \
    --state $st --phase-from photos_websearch --until-phase photos_websearch --force \
    2>&1 | tee tmp/websearch_${st}.log >/dev/null
  echo "=== END $st $(date +%H:%M:%S) ==="
  sleep 15
done
```

**Sequential only.** Tavily rate-limits silently — when two workers hit it at the same time, calls start returning empty `images` arrays with no 429 / no exception / no error trace. Catalog-wide audit 2026-06-06: parallel workers produced 0 photos for MI/NH/RI/AL/DE (583 beach calls, all empty); single-worker resweep right after produced 1,508 photos clean. The `errors=0` line in the script output is misleading; it counts client-side exceptions, not API quota exhaustion.

**Cost rule of thumb**: ~$0.001 per Tavily image_search call. 50-state run ≈ $5-15 depending on per-state beach count.

## Step 3 — Build the preview

The preview generator handles both entities + both stratification modes:

```bash
# Beach: 5 per state across the 9 affected states (recently-loaded photos only)
python scripts/build_websearch_preview.py \
  --entity beach --strata-mode state --strata 5 \
  --load-after '2026-06-06 18:00' \
  --out admin/preview_websearch_$(date +%Y%m%d).html \
  --notes "round N — what got purged this round"

# Dog parks: 50 stratified globally across the whole catalog
python scripts/build_websearch_preview.py \
  --entity dog_park --strata-mode global --strata 50 \
  --load-after "" \
  --out admin/preview_dog_parks_$(date +%Y%m%d).html
```

`--load-after ""` (empty string) disables the load-after filter — sample from the entire catalog instead of a recent sweep. Use this when QAing historical data, not a fresh sweep.

The preview lazy-loads thumbnails so a 200-photo page renders fast. Click any thumbnail to open the `page_url` (Tavily's link to the source page) — useful for verifying attribution.

## Step 4 — Spot host pollution

While scrolling the preview, broken-image placeholders or visibly-bad thumbnails point at hosts to block. The pattern: a host is blocking hot-linked rendering (CDN that requires session cookies, anti-scraping headers), or every image from that host has watermarks / AI-gen artifacts / generic non-dog content.

**Hosts blocked as of 2026-06-07** (in `scripts/load_websearch_photos.HOST_BLOCKLIST`):

| Host | Reason | First flagged |
|---|---|---|
| barkparkfinder.com | AVG flagged the domain | 2026-05-27 |
| lookaside.fbsbx.com | Facebook session-bound CDN, no hot-link render | 2026-05-27 |
| lookaside.instagram.com | Instagram session-bound CDN | 2026-05-27 |
| tiktok.com | Blocks hot-linked image rendering | 2026-06-07 |
| alamy.com | Stock agency; every image has a giant watermark | 2026-06-07 |
| vecteezy.com | Low-quality AI-generated stock | 2026-06-07 |
| metroparkstacoma.org | Hot-link rendering blocked | 2026-06-07 |
| parkstacoma.gov | Hot-link rendering blocked | 2026-06-07 |

The blocklist uses substring matching against the URL hostname, so `c8.alamy.com`, `www.alamy.com`, and any future CDN variant all match `alamy.com`.

## Step 5 — Block + purge + regen iteration loop

Three-step loop for each pollution host Franz flags:

```bash
# 1. Add to HOST_BLOCKLIST (one line in scripts/load_websearch_photos.py)
#    with a comment dating the addition and the reason.

# 2. Purge both photo tables ([[paired-functions-port-fixes-both-sides]] applies).
#    Use the regex-style filter to catch CDN variants.
supabase db query --linked "
WITH del_beach AS (
  DELETE FROM public.beach_photos
  WHERE source='websearch' AND source_meta->>'host' ~* '<HOST_PATTERN>'
  RETURNING 1
),
del_dp AS (
  DELETE FROM public.dog_park_photos
  WHERE source='websearch' AND source_meta->>'host' ~* '<HOST_PATTERN>'
  RETURNING 1
)
SELECT (SELECT COUNT(*) FROM del_beach) AS beach_deleted,
       (SELECT COUNT(*) FROM del_dp) AS dp_deleted;"

# 3. Regenerate the preview.
python scripts/build_websearch_preview.py \
  --entity dog_park --strata-mode global --strata 50 --load-after "" \
  --out admin/preview_dog_parks_$(date +%Y%m%d).html \
  --notes "round N — <host> purged"
```

**Curated rows are NOT preserved.** Under normal circumstances [[get_beach_photos_diverse temp-table lock leak]]-style "human always wins" applies, but blocking hosts is a different shape: if the host blocks rendering, the curator's pick is a broken image regardless of the curated_at flag. Delete unconditionally. Note this in the commit message.

## Step 6 — Bias quantification (optional, when deciding pipeline shape)

The pin `[[assess-dog-bias-quality-2026-06-07]]` documents the decision rules for whether `photos_tag` (vision tagger) is still needed downstream of websearch. The key SQL:

```sql
WITH ws AS (
  SELECT b.state, bp.source_meta->>'description' AS descr
  FROM public.beach_photos bp
  JOIN public.beaches_gold b ON b.fid = bp.arena_group_id
  WHERE bp.source = 'websearch'
    AND COALESCE(bp.source_meta->>'description','') <> ''   -- exclude no-desc rows
)
SELECT state,
       COUNT(*) AS with_desc,
       COUNT(*) FILTER (WHERE descr ~* '\m(dogs?|puppy|puppies|canines?|retriever|labrador|poodle|terrier|shepherd|dachshund|husky|corgi|beagle|chihuahua|bulldog|spaniel|dalmatian)\M') AS dog_mention,
       ROUND(100.0 * COUNT(*) FILTER (WHERE descr ~* '\m(dogs?|puppy|puppies|canines?|retriever|labrador|poodle|terrier|shepherd|dachshund|husky|corgi|beagle|chihuahua|bulldog|spaniel|dalmatian)\M') / NULLIF(COUNT(*),0), 1) AS pct_dog
FROM ws GROUP BY 1 ORDER BY pct_dog DESC;
```

Catalog-wide weighted average 2026-06-07 was 95%. Decision per the pin:
- **≥90% dog-relevant** → drop `photos_tag` from pipeline
- **70–90%** → keep but lighten (tag only no-description rows)
- **<70%** → keep current vision-tagger gate

## Common gotchas

1. **Don't run the sweep through the full state-launch orchestrator.** `--phase-from photos_websearch --until-phase photos_websearch` scopes correctly. Skipping `--until-phase` lets `photos_tag` run, which incurs Haiku spend Franz explicitly wants to defer right now.

2. **Don't parallelize Tavily.** Even two workers cause rate limits with no error signal. Sequential pace is ~1 sec/beach at the loader's 8-thread chunk-internal parallelism, which is plenty.

3. **Don't trust `errors=0` in the loader output.** It only counts client-side exceptions. Empty `images` arrays from Tavily look like success to the script. The real signal is the catalog-side `loaded_at` delta after the run.

4. **`--load-after ""` is intentional**, not a bug. When sampling the whole catalog (not just a recent sweep), the empty string disables the timestamp filter cleanly.

5. **CRLF warnings on commit are noise.** Windows line endings on the Python files. Ignore.

6. **photos_tag is gated by Franz directive.** As of 2026-06-07 the tagger is paused pending the bias QA per `[[assess-dog-bias-quality-2026-06-07]]`. Don't run it unprompted.

## Per Franz preferences

- [[this-is-a-dog-app]] — every photo source MUST be dog-biased at load time, not at curate time. The Tavily "dogs playing" query token is the bias mechanism.
- [[apply-loader-bias-to-beach-photos]] — codified the dog-text query bias after the DP loader proved it (33%→100% has_dog on Toad Hollow).
- [[paired-functions-port-fixes-both-sides]] — blocklist deletes always touch both `beach_photos` AND `dog_park_photos`. The two tables share the source identity.
- [[no-unilateral-architectural-decisions]] — don't drop the dog-word filter, don't expand the blocklist scope beyond what Franz asks, don't re-enable photos_tag without Franz's call.
- [[claim-tested-without-end-state-verification]] — "test" means open the preview HTML and scroll. A SQL row count is not a test.

## Anchors

- `scripts/load_websearch_photos.py` — the loader. `HOST_BLOCKLIST` constant near top.
- `scripts/build_websearch_preview.py` — preview generator. Both entities + both stratification modes.
- `scripts/run_state_pipeline.py:action_photos_websearch` — orchestrator action that fires the loader per state.
- `admin/preview_websearch_*.html` and `admin/preview_dog_parks_*.html` — generated preview pages.
- `scripts/_photo_filters.py:ENTITIES` — entity-table + FK + select-field registry shared with Wikimedia/Flickr loaders.
