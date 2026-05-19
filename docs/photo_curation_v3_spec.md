# Photo curation v3 spec

Per Franz 2026-05-19 design conversation. Consolidates six interlocking changes to the photo curation pipeline:

1. **Vision schema v3** — add `has_path` + `has_vehicle` tags
2. **Per-tier ingest caps** with rare-keyword override (loader-side; not a separate post-load filter)
3. **Unified scoring formula** — single `score_photo()` used by every loader at ingest time
4. **Diverse selector update** — new `path` bucket; explicit vehicle penalty
5. **Auto-curate mode** — picks N best+diverse photos per beach without a human, writes back durably
6. **Pre-flight: per-state photo source discovery** — identify state-aligned photo-rich sites and wire them in

## Architecture (collapsed per Franz 2026-05-19)

```
External source → loader fetches raw candidates →
  apply unified _photo_filters.pre_vision_rank() with beach_meta →
  insert survivors into beach_photos →
  vision-tag every inserted row (no second filter) →
  diverse selector + curator for final gallery
```

Earlier two-stage design (load-everything + post-load `v3_eligible` filter) was redundant. There's no good reason to ingest junk and then filter it out before vision tagging. The criteria we developed BECOME the load filter.

---

## 1. Vision schema v3

Bump `SCHEMA_VERSION = "v3"` in `scripts/load_photo_vision_tags.py`. Two new boolean tags added to the Haiku Pass 2 extraction prompt:

- **`has_path`** *(true|false)* — paths, trails, boardwalks, walkways, or stairs leading to or along the beach. Not roads or driveways. Franz 2026-05-19: *"include more pics of paths."*
- **`has_vehicle`** *(true|false)* — cars, trucks, RVs, parking lots in the frame. Not boats. Franz 2026-05-19: *"penalize pics with vehicles."*

Bumping the version flag auto-invalidates v2 rows for re-tagging on next loader run (existing idempotency mechanism).

---

## 2. Per-tier vision-tagging caps

Per Franz 2026-05-19: don't vision-tag everything; tag only the top-N per beach.

| Condition (priority order, first match wins) | Cap |
|---|---:|
| `beach_dog_policy.dogs_allowed = 'no'` | **0** |
| `beaches_gold.scoring_tier = 'hourly'` | **15** |
| `beaches_gold.scoring_tier = 'daily'` | **12** |
| Everything else (`'none'`, NULL) | **10** |

**Rare-keyword override:** photos whose `title` (and `attribution`) matches any keyword in the rare-keyword list below are **always tagged**, even if outside the per-beach cap. The override exists because a rare-bucket photo (the only `dog` shot at a 30-photo beach, ranking 18th by pre-vision score) shouldn't be lost.

### Rare-keyword list

Whole-word, case-insensitive match. Organized by bucket:

```python
RARE_KEYWORDS = {
    # Dogs — Phase 1 hard-keep in get_beach_photos_diverse
    "dog", "dogs", "puppy", "puppies", "pup", "pups", "doggo", "doggy",
    "doggie", "pooch", "hound", "canine",

    # Surf
    "surf", "surfer", "surfers", "surfing", "surfboard", "paddleboard",
    "kayak", "kayaking", "wave", "waves", "longboard", "kiteboard",
    "windsurf",

    # Atmosphere — moody / time-of-day
    "sunset", "sunrise", "golden hour", "twilight", "dusk", "dawn",
    "fog", "foggy", "mist", "misty", "storm", "stormy",

    # Landscape — rare iconic features
    "driftwood", "sea stack", "sea stacks", "tide pool", "tide pools",
    "tidepool", "cliff", "cliffs", "arch", "cave", "lagoon", "sandbar",
    "dune", "dunes", "bluff", "headland",

    # Path — NEW v3
    "path", "trail", "boardwalk", "walkway", "stairs",

    # Structure — iconic
    "pier", "lighthouse", "jetty",
}
```

### Projected impact (current corpus)

| Tier | Beaches | Photos available | Photos under cap | % kept |
|---|---:|---:|---:|---:|
| hourly | 433 | 2,635 | ~2,073 | 78% |
| daily | 1,135 | 7,531 | ~5,485 | 73% |
| none | 112 | 918 | ~205 | 22% |
| `dogs_allowed='no'` | (subset) | (subset) | 0 | 0% |

Total ~7,763 photos vs uncapped 11,084 = saves ~3,300 photos × $0.005 ≈ **$16.50** on the v3 re-tag.

---

## 3. Pre-vision ranking formula

For each beach, the top-N photos (per tier cap) are picked by this score. Higher = picked first. Rare-keyword hits are also force-included regardless of rank.

### Hard exclusions (never enter candidate pool)

- `hidden_at IS NOT NULL`
- Distance gate (geo-tagged photos only):
  - Default: `distance_m > 500` → excluded
  - **Rare-keyword titles** (dog/surf/sunset/path/etc. — see RARE_KEYWORDS): `distance_m > 2000` → excluded. This is the dog-loose-radius escape per [[loose-radius-dog-filter]] — validated at Del Mar Dog Beach 2026-05-12 where event/cluster photos at the parking lot or trail head (~700-1500m) carry real signal.
- Title matches `NEGATIVE_RE` — auto-compiled from `NEGATIVE_TERMS` in `_photo_filters.py` (127 entries: vehicles, wildlife-specimens, birds, Latin genus names, maps/diagrams, event content). **Marine mammals deliberately excluded from the list** (whale/dolphin/porpoise are beach spectacle, not specimen clutter — Franz 2026-05-19 "drop the mammals").

`POSITIVE_TERMS_DOG`, `POSITIVE_TERMS_GENERIC`, `POSITIVE_TERMS`, and `NEGATIVE_TERMS` all live in `scripts/_photo_filters.py` as the canonical single source. Loaders import from there (no duplication).

### Score formula

```
score = 0
+ (curator_touched ? +10 : 0)             # curator decisions always win
+ source_weight (Type B > Type A baseline):
    CCC       = +3        # CA-only; 81% curator-keep density
    CDPR      = +2        # Type B page-gallery
    NPS       = +2        # Type B page-gallery
    Wikimedia = +1        # Type A geo-tagged
    Flickr    = +1        # Type A geo-tagged
    Mapillary = 0         # untested at scale
    Unsplash  = 0         # untested at scale
+ distance_bonus (Type A only; Type B has NULL distance):
    ≤200m = +2
    ≤500m = +1
    NULL  = 0  (no penalty for Type B)
+ title rare-keyword hit: +5    # ALSO force-tags outside per-beach cap
+ generic positive title token ('beach'): +0.3
+ photographer_keep_rate (≥5 prior decisions):
    ≥80% kept = +1
    ≤20% kept = -1
    otherwise = 0
+ recency (captured_at age):
    ≤5yr  = +0.02
    ≤10yr = +0.01
    older = 0
```

### Resulting comparison

- Best CCC: ~+3
- Best CDPR / NPS: ~+2 (Type B beats Type A baseline ✓)
- Best Flickr/Wikimedia at <200m: +1+2 = +3 (ties CCC)
- Flickr/Wikimedia at ~500m: +1+1 = +2 (below Type B)
- Flickr/Wikimedia >500m: EXCLUDED

Sort by score DESC, take top-N per beach (tier cap), force-include any title-rare-keyword hits not in the top-N.

---

## 4. Diverse selector update

`get_beach_photos_diverse` (the consumer-facing 6-photo gallery selector) gets two updates:

### 4a. New `path` bucket

Insert between `landscape` and `people` in the priority order:

```
surf → landscape → PATH → people → structure → atmosphere → wide → water
```

A photo matches the `path` bucket if `source_meta.vision.has_path = true`.

### 4b. Vehicle penalty (belt + suspenders)

The model will naturally learn a negative weight on `has_vehicle` from curator history once v3 retags + retrains. For belt-and-suspenders during the v3 rollout, add an **explicit post-hoc penalty** in `apply_vision_rules()`:

```python
if vision.get('has_vehicle'):
    prob -= 0.15
```

(Mirrors the existing `+0.07` boost for `has_dog`.)

---

## 5. Auto-curate mode

Per Franz 2026-05-19. Bypasses human curation: picks the N best + most-diverse photos per beach via the existing `get_beach_photos_diverse` logic + writes the selection back durably.

**Key design (Franz 2026-05-19):** auto-curate writes to the SAME `curated_at` flag a human uses — but the existing `curated_by` text column records the **method**. One flag, one discriminator. No parallel column.

### Method discriminator on existing column

`beach_photos.curated_by` (existing text column) records *how* the curation happened:
- `curated_by = '<username>'` (e.g., `'franz'`) → human curator pick
- `curated_by = 'auto:n=<N>'` (e.g., `'auto:n=6'`) → auto-curate pick with the N value preserved

Both populate `curated_at` identically. The selector treats them the same downstream because both flow through `curated_at IS NOT NULL`. The method is recoverable from `curated_by` for audit / filtering.

### Conflict rule: human always wins

- **Before auto-curate runs on a beach:** check if ANY photo for that beach has `curated_at IS NOT NULL AND curated_by NOT LIKE 'auto:%'` (i.e., a human touched it). If yes, **skip** — the human owns this beach.
- **If a human curates after auto-curate ran:** human's actions overwrite auto-curate's selections on that beach (the human curator UI already records `curated_by=<username>`; auto-curate rows can either stay as-is or be cleared via a "human takes over" sweep).
- **Re-running auto-curate:** safe + idempotent. Clears prior `auto:*` rows for the beach first, then re-picks. Human picks are never touched.

### Selector update

No schema-side change needed in `get_beach_photos_diverse` — the existing `curated_at IS NOT NULL` predicate now covers both human + auto picks. The +0.07 curator boost in `eff_score` applies equally (auto picks are treated as first-class).

Optional refinement (defer unless useful): give human-curated a slightly higher boost than auto-curated by inspecting `curated_by NOT LIKE 'auto:%'` in the rank tie-breaker. For v1, equal treatment is correct.

### Script: `scripts/auto_curate.py`

```
python scripts/auto_curate.py                            # ALL eligible beaches, default N=6
python scripts/auto_curate.py --n 8                      # gallery of 8 per beach
python scripts/auto_curate.py --state WA                 # one state
python scripts/auto_curate.py --fid 9806                 # single beach
python scripts/auto_curate.py --dry-run                  # list intended picks, no writes
python scripts/auto_curate.py --reset                    # clear auto_curated_at for beaches re-run
```

For each beach in scope:
1. Skip if any photo for the beach has `curated_at IS NOT NULL AND curated_by NOT LIKE 'auto:%'` (a human has touched it)
2. Clear any prior `curated_by LIKE 'auto:%'` rows for the beach (reset auto picks; re-runs converge)
3. Call `get_beach_photos_diverse(fid, N)`
4. For each returned photo: set `curated_at = now()`, `curated_by = 'auto:n=<N>'`, `sort_order = position` (1=hero)

Chunked per [[chunked-subprocess]] — commit per 100 beaches.

### N defaults

Default `N=6` (mirrors current selector default). Per-tier defaults considered but deferred — caller passes N. Future: tie N to scoring_tier (hourly=8, daily=6, else=4) if useful.

### Use case for MVP+ launch

Auto-curate lets us ship the 1,962-beach photo catalog without waiting for a human pass. After v3 re-tag + model retrain, run `auto_curate.py --n 6` once → every beach gets a 6-photo gallery sourced from its highest-scoring + most-diverse content. Human curation continues in parallel and naturally takes precedence wherever Franz spends time.

---

## 6. Pre-flight: per-state photo source discovery

Per Franz 2026-05-19: for each MVP+ state, identify state-aligned photo-rich sites whose URLs are already in BEP (so we know they cover our beaches), then wire each as a loader.

### Site categories (in scope)

1. **State park systems** — OPRD (OR), WSPRC (WA), CDPR (CA — done)
2. **County park systems** — LA County Parks, King County, Tillamook County, etc.
3. **Regional conservancies / sanctuaries** — CCC analog per state (e.g., Puget Sound Partnership for WA, South Slough NERR for OR)

Not in scope (per Franz): state tourism boards.

### Alignment criterion

A candidate site is worth wiring **if its domain appears in `beach_enrichment_provenance.source_url` for our beaches**. The existing governance/codify work has already proven coverage; we leverage that. No spatial PIP or fuzzy matching needed in v1.

### Discovery procedure

For each MVP+ state:

```sql
-- For each beach in <state>, what URL domains are present in BEP?
WITH per_domain AS (
  SELECT regexp_replace(source_url, '^https?://(www\.)?([^/]+).*', '\2') AS domain,
         count(DISTINCT bep.gold_fid) AS beaches_covered
    FROM beach_enrichment_provenance bep
    JOIN beaches_gold b ON b.fid = bep.gold_fid
   WHERE b.state = <state>
     AND b.is_active
     AND bep.source_url IS NOT NULL
   GROUP BY 1
)
SELECT * FROM per_domain
 WHERE beaches_covered >= 5  -- worth a loader build only if it hits ≥5 beaches
 ORDER BY beaches_covered DESC;
```

Output is a ranked list of domains per state. Filter to ones matching site categories above; build loaders.

### Loader build pattern

Clone `scripts/load_cdpr_park_gallery.py`. Per site:
1. Confirm the per-page gallery URL pattern (each CMS differs)
2. Build the image-URL regex
3. Replace `source='cdpr'` with `source='<state>_parks'` / `source='<county>_parks'` / etc.
4. Per-state loader runs against beaches whose BEP carries a URL for that domain
5. `match_quality='park_gallery_shared'` (same fan-out semantics — one gallery per multiple sub-beaches)

### Per-state targets (initial pass)

| State | Site | Domain | URL coverage | Status |
|---|---|---|---:|---|
| CA | CDPR | parks.ca.gov | 556 / 557 | DONE — `load_cdpr_park_gallery.py` |
| OR | OPRD | stateparks.oregon.gov | 44 / 151 | TODO |
| WA | WSPRC | parks.wa.gov | 419 / 419 | TODO (biggest immediate lift) |
| CA + OR + WA | County parks | various .gov | TBD via discovery query | Run discovery, build per-county loaders |
| OR | South Slough NERR | various | TBD | Likely small but worth checking |
| WA | Puget Sound Partnership | various | TBD | TBD |

Discovery query above produces the actual target list per state. Build loaders for top hits.

---

## 7. Retroactive filter for pre-existing photos

The collapsed architecture (loaders apply the unified filter at ingest) is forward-looking — any new photo loaded via the refactored Flickr / Wikimedia loaders is filter-clean.

But there are **~13,700 photos already in `beach_photos` from prior loader runs** that pre-date the unified filter. They were ingested under the OLD per-source filters. Before paying Haiku $ to vision-tag them at v3, we want to skip the ones that wouldn't survive the new filter (over-tier-cap, negative-keyword title, >500m).

### Approach: `v3_skipped` sentinel

`scripts/filter_non_curated_for_retag.py`:
1. Pulls all non-curated MVP+ photos missing v3 tags
2. Groups by beach; fetches `scoring_tier` + `dogs_allowed`
3. Calls `_photo_filters.pre_vision_rank()` per beach
4. For **rejected** rows: sets `source_meta.v3_skipped = true` (+ `v3_skipped_at`, `v3_skip_reason`)
5. Vision tagger's WHERE clause excludes `source_meta->>'v3_skipped' = 'true'`

Curated photos are never marked (curator-touched is sacred). The sentinel is one-way — re-runs only evaluate not-yet-marked rows. Idempotent.

### Result (2026-05-19 dry-run on 7,106 non-curated MVP+ photos)

| | Count | % |
|---|---:|---:|
| Would re-tag (survives filter) | 5,021 | 71% |
| Would skip (over-cap / negative-kw / >500m) | 2,085 | 29% |

Cost saving: ~$6 (re-tag $15 vs $21 unfiltered) just for non-curated MVP+. Bigger savings on the full corpus.

### Recommended targeted re-tag scope

For MVP+ launch, re-tag two narrow populations rather than the full corpus:

| Population | Why | Photos | Cost |
|---|---|---:|---:|
| Curated set (curator-kept) | Trains the new `has_path`/`has_vehicle` model weights on positives | 1,091 (of 1,747; 656 already v3) | $3.30 |
| Non-curated MVP+ post-filter | Enables eligibility for auto-curate selection on MVP+ beaches | 5,021 (filtered from 7,106) | $15.06 |
| **Total** | | **6,112** | **~$18.40** |

vs ~$38 for full-corpus re-tag. Same model quality (positives drive learning); MVP+ launch quality identical.

---

## 8. Implementation sequence (collapsed-architecture revision)

1. **Vision schema bump** — edit `scripts/load_photo_vision_tags.py`: add `has_path` + `has_vehicle` to `_EXTRACT_PROMPT`, bump `SCHEMA_VERSION = "v3"`. Commit.
2. **Pre-vision ranking helper** — new module/function `scripts/_photo_filters.py::pre_vision_rank(photos, beach_meta) -> ranked_list_capped` implementing the score formula + per-tier caps + rare-keyword override + 500m exclusion. Unit-testable.
3. **Wire the cap into the vision tagger** — modify the candidate query in `load_photo_vision_tags.py` to consume the ranked-capped list instead of "all photos missing v3 tag."
4. **Run re-tag** in chunks (script already supports `--chunk-size`). Budget ~$50-65; bump `DEFAULT_BUDGET_USD` if needed.
5. **Retrain model** — `python scripts/train_photo_model.py`. New features (`has_path`, `has_vehicle`) enter the feature set automatically.
6. **Add path bucket + vehicle penalty** — SQL migration updating `get_beach_photos_diverse` + Python tweak to `apply_vision_rules()`.
7. **Verify** — spot-check 10 beaches: do path photos surface? Do vehicle photos drop in rank? Spot-check the cap holds (no over-tagging on long-tail beaches).

---

## Related

- [[photo-curation-ml-tier2-spec]] — original Tier 2 model spec
- [[photo-distance-450m]] — existing 450m hard cutoff at load (upstream of this spec's 500m rank cut)
- [[pexels-pixabay-rejected]] — sources NOT to add
- [[state-parks-dept-baseline-photo-source]] — companion strategy for fresh ingest
- `scripts/load_photo_vision_tags.py` — vision tagger
- `scripts/train_photo_model.py` — Tier 2 trainer
- `scripts/_photo_filters.py` — pre-vision rank helper (to build)
- DB function `get_beach_photos_diverse` — selector
