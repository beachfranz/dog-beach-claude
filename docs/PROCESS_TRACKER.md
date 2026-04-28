# Data Process Tracker

**Purpose.** Defensible audit trail of every load-bearing decision we've made
about beach + dog-policy data quality. Each entry records the rule, the
rationale, alternatives considered, the measurable impact, and how to reverse
or re-run if needed.

**Audience.** Future maintainers (including future-Franz, future-Claude,
future-someone-else), data reviewers, and anyone asking "why does this beach
have this name?" / "why isn't dogs allowed surfaced as 'yes' here?".

**Conventions.**

- Newest entries on top.
- Dates absolute (YYYY-MM-DD), not relative.
- **Decision** = what we did.
- **Why** = the rationale, including the failure mode the decision protects against.
- **Alternatives considered** = options rejected and why.
- **Impact** = quantified change (rows touched, recovery rates, drops).
- **Reversibility** = how to undo and what data is recoverable.

---

## 2026-04-27 — County nudge for offshore-centered OSM features

**Decision.** For osm_features rows still null on `county_name_tiger`
after the strict PIP pass, borrow the county from the nearest OSM
feature that DOES have one, capped at 5 km radius. Falls back to
`null` outside the cap (we accept the loss rather than reach further).

**Why.** Three OSM features fell outside CA county polygons under
strict containment:

- `Border Field State Park Beach` (way 757463741) — SD/MX border,
  geometry just south of the county line in Tijuana waters.
- An unnamed park polygon (way 757463707) at the same border.
- An unnamed beach (way 759717128) at the OR/CA border, similarly
  fractionally outside Del Norte's polygon.

These aren't offshore-centroid bugs — they're border-edge artifacts.
The named one ("Border Field State Park Beach") didn't have a similar-
named neighbor to snap to, so the originally-planned name-similarity
nudge doesn't generalize. Nearest-neighbor county borrow does.

**Alternatives considered.**

- *Snap-to-similar-name nudge.* Original plan from the previous TIGER
  PIP entry. Rejected when only 1 of 3 unassigned features had a name
  at all, and that one had no similar-named beach within reach.
- *Manual assignment for the 3.* Workable for 3 records but doesn't
  scale to other states; programmatic rule is reusable.
- *Wider radius (e.g. 25 km).* Risk of borrowing the wrong county for
  a point that's genuinely outside all CA counties (e.g., an OR beach
  that slipped into the dataset). 5 km is tight enough that it can
  only succeed if a CA-bound neighbor is right next door.

**Impact.**

| osm_id | name | county_name_tiger | county_fips_tiger |
|---|---|---|---|
| 757463741 | Border Field State Park Beach | San Diego | 06073 |
| 757463707 | _(unnamed park)_ | San Diego | 06073 |
| 759717128 | _(unnamed beach)_ | Del Norte | 06015 |

`osm_features` now at 100% county coverage (19,916 / 19,916).

**Reversibility.** Reset the three rows' `county_name_tiger` /
`county_fips_tiger` to null. Originating geometry is untouched.

---

## 2026-04-27 — County assignment: TIGER county polygons, point-in-polygon

**Decision.** Replace the prior nearest-neighbor county borrow with
authoritative point-in-polygon assignment from `public.counties` (TIGER
county boundaries). Add new columns on all three sources, leaving the
prior `county` / `canonical_county` text columns untouched as historical
reference:

- `ccc_access_points.county_name_tiger`, `county_fips_tiger`
- `us_beach_points.county_name_tiger`, `county_fips_tiger` (CA only)
- `osm_features.county_name_tiger`, `county_fips_tiger`

`county_fips_tiger` is the 5-digit TIGER GEOID (state + county FIPS, e.g.
`06037` for Los Angeles). `_tiger` suffix marks these as the canonical
gold-standard fields.

Strict containment via `ST_Contains(counties.geom, t.geom)`, scoped to
`state_fp='06'`. Points outside any CA county polygon are left null —
no fallback, no buffer.

Migration: `supabase/migrations/20260427_county_pip_tiger.sql`.

**Why.** The earlier borrow (county inherited from nearest CCC point)
was a workaround because we hadn't found the county polygon table.
Polygon-based PIP is the authoritative source — no propagation of
neighbor errors, no special-casing for inland vs coastal, no question
of "whose nearest." This matches the architectural direction in
`project_ccc_as_overlay_not_spine.md`: county is a fundamental attribute
that comes from a single authoritative source, never inherited from one
specific dataset.

**Alternatives considered.**

- *Coastal buffer (e.g. 100m).* Rejected per Franz's guidance — keep
  the unassigned set visible so we can see exactly which beach polygons
  have offshore-center geometry, then handle them in a later step
  ("move them toward the closest beach polygon with a similar name").
- *Overwrite existing `county` / `canonical_county` columns.* Rejected
  — preserve the originals as historical reference. The two fields can
  be diff'd later to find drift between data sources.

**Impact.**

| table | total | tiger_assigned | unassigned |
|---|---:|---:|---:|
| `ccc_access_points` | 1,632 | 1,632 | 0 |
| `us_beach_points` (CA) | 952 | 952 | 0 |
| `osm_features` (all) | 19,916 | 19,913 | 3 |
| `osm_features` (beaches only) | 1,528 | 1,526 | 2 |

CCC and UBP at 100%. The 3 OSM holdouts (2 of which are beaches) are
features whose geometric center sits just offshore — known limitation
of using `out center` instead of `out geom` from Overpass. Slated for
a follow-up "snap to nearest like-named beach polygon" step.

**Reversibility.** New columns are additive — drop them to revert. The
prior nearest-neighbor `county` / `canonical_county` values remain
untouched.

---

## 2026-04-27 — OSM beach names: re-borrowed with UBP priority + CCC beach-word filter

**Decision.** Reset all spatially-borrowed OSM beach names. New rules:

1. **Pass 1 (UBP first):** for each OSM beach with no name, find nearest
   `us_beach_points` row within 200m. Use that name. Source flag:
   `name_source = 'us_beach_points'`.
2. **Pass 2 (CCC fallback, filtered):** for OSM beaches still nameless, find
   nearest `ccc_access_points` row within 200m **whose name contains a
   beach-y word on a word boundary** — `~* '\m(beach|cove|shore|sand)'`. Use
   that name. Source flag: `name_source = 'ccc'`.
3. **No fallback if neither qualifies** — leave `name = null`.

Migration: `supabase/migrations/20260427_osm_features_reborrow_names.sql`.

**Why.** Cross-checking the prior CCC-first borrow against UBP within 200m
revealed that 21% of CCC borrows (35 of 166) substantially disagreed with
UBP's name (trigram similarity < 0.5). Inspection showed CCC was naming
*access features* — piers, parking lots, parks — not the beach itself
("Ferry Landing" instead of "Coronado Beach", "Malibu Pier" instead of
"Carbon Beach", "Julia Pfeiffer Burns State Park" instead of "McWay Beach").
UBP is beach-focused by source intent, so it gets priority. CCC's names are
only trusted when they explicitly contain a beach-y substring.

**Alternatives considered.**

- *Keep CCC priority.* Rejected: would continue to surface piers/parks as
  beach names, eroding user trust (Tier 1 failure mode in
  `project_failure_modes_trust_budget.md`).
- *Heuristic priority based on word presence on both sides.* More work for
  marginal benefit; UBP is curated to be beachy by default.
- *Flag contested for manual review without auto-picking.* Rejected for
  scale — 35 contested cases multiplied across a future OR/WA expansion
  becomes unmanageable.

**Impact.**

| name_source     | before | after | net  |
|-----------------|-------:|------:|-----:|
| `osm`           |    602 |   602 |   0  |
| `us_beach_points` |  106 |   180 | +74  |
| `ccc`           |    166 |    33 | −133 |
| `null`          |    654 |   713 | +59  |

Total named: 815 / 1,528 (53%). Down 3 percentage points from prior 57%, but
the 59 lost borrows were exactly the wrong ones — non-beach names that would
have lied to users. The 33 remaining CCC borrows are all beach-word-vetted.

**Reversibility.** Migration is idempotent. To revert: re-run an UPDATE that
nullifies `name` and `name_source` on rows where `name_source` in
`('ccc', 'us_beach_points')`, then re-apply prior borrow logic. OSM-original
names (`name_source = 'osm'`) are never touched by this process.

---

## 2026-04-27 — Cross-source agreement check on borrowed names

**Decision.** Audit the original CCC name borrows against UBP within 200m to
quantify how often the two sources disagreed. No data changed — diagnostic
only.

**Why.** Original borrow assumed CCC was authoritative. Wanted to know
whether that assumption held across the 166 borrowed cases.

**Findings.**

| outcome | count | %  |
|---|---:|---:|
| CCC was the only source within 200m | 92 | 55% |
| Exact match (case-insensitive) | 17 | 10% |
| Soft match (trigram ≥ 0.5, not exact) | 22 | 13% |
| **Contested** (similarity < 0.5) | **35** | **21%** |

Contested set inspection showed a clear pattern: CCC names access features,
UBP names beaches. Triggered the re-borrow above.

---

## 2026-04-27 — `osm_features.county` enrichment via nearest-CCC borrow

**Decision.** Add `county` column to `osm_features`. For all 1,528 beach +
dog-friendly-beach features, populate via nearest CCC point regardless of
distance (KNN, no radius cap). For every CA us_beach_points row missing
`canonical_county`, populate the same way.

**Why.** OSM doesn't tag county on features. We needed county to power the
admin map's county dropdown filter. CCC has accurate county on every row;
within ~5km on any CA point, it's a reliable proxy.

**Alternatives considered.**

- *Spatial join with `jurisdictions` polygon table.* Rejected: the
  `county` and `fips_county` columns on `jurisdictions` are not populated
  for CA rows.
- *Add a county polygon table.* Out of scope for this iteration.
- *Bbox-per-county heuristic.* Cruder; nearest-CCC already produces correct
  results.

**Impact.** 100% county coverage on osm_features beach features and CA
us_beach_points. County dropdown on `admin/beaches-all-map.html` now
filters all three sources cleanly.

---

## 2026-04-26 — Spatial name borrow: original CCC-first version

**Decision.** For each `osm_features` beach with no name, find the nearest
named row in CCC (within 200m), then UBP (within 200m), and copy the name.
Tag with `name_source` for provenance.

**Why.** OSM had 926 unnamed beaches in CA. CCC + UBP had ~3,000 named CA
beach rows between them. Cheap recovery via spatial nearest-neighbor.

**Initial impact.** 272 of 926 names recovered (29%). Coverage 39% → 57%.

**Superseded by 2026-04-27 re-borrow.** This version had a flaw: CCC
priority caused 35+ wrong names where CCC's row was an access point, not
the beach. See above.

---

## 2026-04-26 — OSM features: 5-pass loader (`fetch_osm_dog_features_ca.py`)

**Decision.** Pull 5 classes of CA features from OSM Overpass, dedup
between passes, upsert into `osm_features`:

1. `leisure=dog_park` → `feature_type = 'dog_park'`. Synthesize
   `dog_status='unleashed'` if no explicit `dog=*` (OSM convention for
   dedicated dog parks).
2. `leisure=park` with `dog=yes|leashed|unleashed` →
   `feature_type='dog_friendly_park'`.
3. `natural=beach` with `dog=yes|leashed|unleashed` →
   `feature_type='dog_friendly_beach'`.
4. `leisure=park` (everything) → `feature_type='park'`.
5. `natural=beach` (everything) → `feature_type='beach'`.

Dedup: most-specific pass wins. A feature appearing in pass 4 that already
landed in pass 2 keeps its `dog_friendly_park` label; the broad
`leisure=park` query skips it.

**Why polymorphic table?** Mirrors the existing `geo_entity_response`
pattern. One shape across sources, discriminator column. Lets us add
overlay sources (`bcdc`, `or_state_parks`, etc.) without schema churn.

**Tooling decisions.**

- Overpass `out center tags` (centroid, not full polygon) chosen for
  bandwidth. Trade-off documented: we can't do "is this CCC point inside
  this OSM polygon?" PIP queries until/unless we re-fetch with `out geom`.
- 30s sleep between Overpass calls to avoid 429 rate limits.
- All-parks query (`leisure=park` everything) is the heaviest at ~17k
  rows, ~50s response time + ~5MB payload.

**Impact.**

| feature_type | count |
|---|---:|
| `park` | 17,375 |
| `beach` | 1,521 |
| `dog_park` | 833 |
| `dog_friendly_park` | 180 |
| `dog_friendly_beach` | 7 |
| **total** | **19,916** |

**Known caveat to revisit (Tier 3 failure mode).** 799 of 833 `dog_park`
rows have synthesized `dog_status='unleashed'` (no explicit OSM dog tag).
OSM convention is "dog parks are off-leash" but not universal — some
leashed-only dog walking paths are tagged `leisure=dog_park`. Documented
in `project_failure_modes_trust_budget.md`. Fix path: drop synthesis or
downgrade synthesized rows' confidence.

---

## 2026-04-26 — `dogs_verdict` weighted-vote consensus on CCC beaches

**Decision.** For each CCC sandy + name-like-beach point, compute a binary
`dogs_verdict` by weighted vote across three signals:

| signal | weight |
|---|---|
| CCC native (`dog_friendly` column: 'yes'/'no') | **0.66** (constant) |
| CPAD via `geo_entity_response` (rolled to binary: yes/restricted/seasonal → yes; no → no) | response_confidence |
| CCC-LLM via `geo_entity_response` (same rollup) | response_confidence |

Manual `admin://` overrides skip the math entirely (force the verdict).

**Decision rules:**

- Sum yes-weights and no-weights across available signals.
- Verdict = side with more weight.
- Confidence = winning_weight / (winning_weight + losing_weight) — normalized 0–1.
- **Margin guard:** when `|yes_weight − no_weight| < 0.10`, flag
  `meta.review = true`, default to "no" (more restrictive).
- Exact tie → "no", confidence 0.5, review = true.

Stored on `ccc_access_points`:

- `dogs_verdict` text (`'yes'` | `'no'` | null)
- `dogs_verdict_confidence` numeric (0–1)
- `dogs_verdict_meta` jsonb — yes_weight, no_weight, margin, sources[],
  review, computed_at

**Why this shape.** Three conditions:

- **Vocabulary mismatch:** CCC native is binary (yes/no), CPAD/CCC-LLM are
  5-valued. Rolling up to binary reconciles them on equal terms.
- **Asymmetric trust:** manual overrides represent verified human
  knowledge; they should never be out-voted by LLM signals.
- **Conservative on uncertainty:** thin-margin cases default to "no"
  because Tier 1 failure mode #4 (sending dog owner to no-dogs beach)
  is credibility-killing.

**Impact.** 305 of 424 sandy + name-like-beach CCC points have a verdict
(212 yes, 93 no, 12 review-flagged). 119 stay null. The 119 are excluded
from the Working Set on the basis of "we don't know."

**Reversibility.** `compute_dogs_verdict(p_objectid)` is idempotent;
`recompute_all_dogs_verdicts()` re-runs the full sweep. Changing weights
or rules requires editing the function and re-running.

---

## 2026-04-26 — Working Set definition + override mechanism

**Decision.** Add `is_working_set` (generated boolean) and
`is_working_set_override` (manual tri-state) to `ccc_access_points`. Auto-rule:

```
is_working_set = coalesce(
  is_working_set_override,
  (sandy_beach='Yes'
   AND open_to_public='Yes'
   AND lower(name) like '%beach%'
   AND dogs_verdict='yes'
   AND (archived is null or archived <> 'Yes'))
)
```

**Why.** This is the recommendation-eligible set for the consumer app.
Each of the four predicates protects against a Tier 1 failure
(`project_failure_modes_trust_budget.md`):

- `sandy_beach='Yes'` → not a rocky cliff or pier
- `open_to_public='Yes'` → not private land
- name contains "beach" → exclude facilities mis-cataloged as access points
- `dogs_verdict='yes'` → dogs allowed
- archived filter → not closed/removed

Override column lets us force-include known good beaches whose CCC name
lacks the word "beach" (Fort Funston, currently). Or force-exclude in
the future.

**Impact.** 214 rows are currently in the Working Set:
- 212 auto-qualified
- 1 force-included (Fort Funston, objectid 450)
- 1 manual entry (Rosie's Dog Beach, objectid −1, see next entry)

---

## 2026-04-26 — Manual CCC entry for Rosie's Dog Beach (objectid −1)

**Decision.** Insert a manual row into `ccc_access_points` with
`objectid = -1` for Rosie's Dog Beach. Negative objectids are reserved
for manual entries; CCC's source data uses positive integers, so reloads
can never collide with manual rows.

**Why.** CCC's ArcGIS source has zero coverage of Rosie's despite it
being a named off-leash beach in Long Beach. Without manual insertion,
the corresponding gold star on the off-leash map had no working-set CCC
neighbor, breaking the cross-reference.

---

## 2026-04-26 — Off-leash gold-star geom drift fix

**Decision.** Detected that 8 of 15 rows in `off_leash_dog_beaches` had
`geom` columns drifted from their `latitude`/`longitude`. Worst offender:
Huntington Dog Beach at 2,602m off. Fixed via:

```sql
update public.off_leash_dog_beaches
   set geom = st_setsrid(st_makepoint(longitude, latitude), 4326)
 where st_distance(...) > 1;
```

**Why.** The lat/lng columns are canonical (Leaflet renders from them);
the geom was stale. Spatial joins using geom were silently mismatching.

**Impact.** Star-to-Working-Set proximity matches went from 8 of 15 →
14 of 15 after the fix. Only Big Lagoon County Park remains unmatched
(genuine CCC coverage gap).

---

## 2026-04-26 — Polymorphic refactor: `cpad_unit_response` → `geo_entity_response`

**Decision.** Rename + restructure to support multi-entity types
(`cpad`, `ccc`, `off_leash`, `bcdc`) under one shape. New PK:
`(entity_type, entity_id, source_url, response_scope, scraped_at)`.

**Why.** We were extending CCC LLM-knowledge answers and starting to
need a polymorphic response store. Replicating the table per entity
type would have exploded schema and the `_current` view set.

**Reversibility.** SQL migration only; no data lost. Reverse via
`alter rename` if ever needed.

---

## Live processes (not one-shot)

These are repeatable operations, not historical events:

- `recompute_all_dogs_verdicts()` — sweeps every CCC point in the
  sandy + name-like-beach universe, re-runs `compute_dogs_verdict()`.
  Re-run when scoring rules change or signals are added.
- `fetch_osm_dog_features_ca.py` — re-fetches all 5 OSM passes from
  Overpass and upserts into `osm_features`. Idempotent. Re-run for OSM
  data refreshes (every few months).
- `20260427_osm_features_reborrow_names.sql` — re-runs the borrow if
  CCC or UBP gain new rows. Idempotent.
- `20260427_osm_features_county.sql` (concept — embedded in earlier
  ad-hoc query) — county borrow. Re-run when CCC adds new CA points.

---

## How to add an entry

When you make a non-trivial data-quality decision, append a section here
with: date, title, decision, why, alternatives considered, impact,
reversibility. The newest entry goes immediately under the title block.
Keep older entries unchanged — this is an audit log, not a wiki.
