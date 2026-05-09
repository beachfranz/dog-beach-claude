# Time chunking in the dog-beach pipeline

**Status:** design discussion (Franz, 2026-05-09). Captures the time axes the pipeline has to model, where they leak across each other today, and the proposed clean separation.

---

## TL;DR

Time enters the dog-beach pipeline at four different cadences. Today they're entangled: seasonal closures get applied as direct UPDATE statements on the daily-consumed `beach_dog_policy` table, time-of-day rules live in jsonb that nobody reads, and the tier classifier ignores seasonal data entirely.

**Proposal:** treat time as **four explicit axes**, each with its own table/column convention, refresh cadence, and consumer-query consultation order. The big shift is that `beach_location_tier()` becomes season-AGNOSTIC (it represents the beach's overall character) and a new `beach_dog_status_today(fid, date)` returns the season-aware "can-I-bring-my-dog right now" verdict.

---

## The four time axes

| Axis | Cadence | Validity scope | Where it lives today | Where it should live |
|---|---|---|---|---|
| **A. Static** | quarterly / on-event | "Until the law changes" | `state_dogs_policy`, `pad_us_unit_dogs_policy`, operator extractions, policy_research | Same tables + add `valid_from` / `valid_until` |
| **B. Seasonal** | yearly recurring | MM-DD start/end, every year | `beach_dog_policy.dogs_prohibited_start/end` (single window) + `operator_dogs_policy.seasonal_closures` jsonb | New `seasonal_closures` table per beach (multiple windows + types) |
| **C. Time-of-day** | daily recurring | HH:MM start/end, days_of_week | `operator_dogs_policy.time_windows` jsonb (unparsed by consumers) | New `time_of_day_restrictions` table per beach |
| **D. Daily** | nightly | One row per beach per day | `beach_day_recommendations` (location_id + local_date) | Same |

Axes A → C are policy. Axis D is conditions. They combine differently.

---

## Where the leaks are today

### Leak 1: tier classifier ignores Axis B

`beach_location_tier(dogs_allowed, has_off_leash, has_on_leash, dogs_prohibited_start)` takes a `dogs_prohibited_start` argument **but doesn't use it**. A beach prohibited Apr 1 – Sep 30 still classifies as Tier 2 (`dogs_allowed='yes'`), which is correct in the off-season but misleading in summer.

The fix isn't to make `beach_location_tier()` time-aware (it should remain a stable structural classifier). The fix is:
- Tier classifier → "what is this beach's character year-round?"
- New `beach_dog_status_today(fid, date)` → "is dogs allowed AT THIS BEACH ON THIS DATE?"

Consumer surfaces (find page, beach detail page) should call `beach_dog_status_today(fid, current_date)` for the in-season verdict and surface `tier` only for category badges and filtering.

### Leak 2: Axis B has only one window per beach

`beach_dog_policy.dogs_prohibited_start/end` is a single MM-DD pair. Real beaches have multiple overlapping closures:
- Plover season Apr 1 – Aug 31
- Sea turtle nesting May 1 – Oct 31
- Beach grooming Memorial Day – Labor Day
- Town swim-beach summer ban Jun 15 – Sep 15

Today's schema collapses these to one window. The earliest start + latest end wins, which is wrong (the consumer can't tell which restriction is active in any given week). The proposed `seasonal_closures` table (per-beach, multiple rows, one closure_type per row) fixes this. Already partially scaffolded in `20260509_policy_seed_phase_helpers.sql` as `seasonal_closure_seed` — it's the seed table; the consumer table parallel to it should be `beach_seasonal_closures(arena_group_id, closure_type, start_md, end_md, source_url, source_quote)`.

### Leak 3: Axis C is unparsed jsonb

`operator_dogs_policy.time_windows` looks like `[{"days":"M-F","start":"10:00","end":"18:00","kind":"prohibited"}]`. No consumer reads it. No populator emits it to BEP. No resolver touches it. The data is captured but not surfaced.

The fix is small: a populator that parses the jsonb and writes structured rows to a new `beach_time_of_day_restrictions(arena_group_id, days_of_week, start_time, end_time, kind, source)` table, then the `daily-beach-refresh` Edge Function consults it when rendering today's rec.

### Leak 4: Axis A has no valid-from/valid-until

If RI changes its DEM rule on 2027-01-01, today's schema can't represent "the old rule was X until 2026-12-31, the new rule is Y starting 2027-01-01." Adding `valid_from` / `valid_until` to `state_dogs_policy`, `pad_us_unit_dogs_policy`, `city_dog_policy`, etc. (defaulting `valid_until=NULL` for "still in force") makes point-in-time queries possible — both for backfill audits ("when did this stop being correct?") and for advance scheduling ("the new ordinance takes effect 2027-01-01, post-date the row now").

This is low-cost (six columns added across four tables). The populators don't need to change for the no-time-travel case (they'd default to `valid_from=NULL OR valid_from <= current_date` plus `valid_until IS NULL OR valid_until > current_date`), but the schema is now ready when laws change.

---

## Consumer query: the combined evaluation

For "Can I bring my dog to Beach X today at 11am?", evaluate the four axes in order:

```
1. Axis A (static):  read beach_dog_policy.dogs_allowed, has_off_leash, has_on_leash
                     → base_verdict ∈ {yes_off_leash, yes_on_leash, mixed, no, unknown}

2. Axis B (seasonal): SELECT closure_type FROM beach_seasonal_closures
                       WHERE arena_group_id = $fid
                         AND today_md BETWEEN start_md AND end_md
                     → if any closure_type in (plover, sea_turtle, swim_ban):
                         seasonal_verdict = prohibited_today
                       else:
                         seasonal_verdict = no_seasonal_restriction

3. Axis C (time-of-day): SELECT kind FROM beach_time_of_day_restrictions
                          WHERE arena_group_id = $fid
                            AND $day_of_week IN days_of_week
                            AND $current_time BETWEEN start_time AND end_time
                        → if any kind = prohibited:
                            tod_verdict = prohibited_now
                          else:
                            tod_verdict = no_tod_restriction

4. COMBINE: if seasonal_verdict OR tod_verdict says prohibited → "no, not right now"
            else → base_verdict
```

This is the function `beach_dog_status_today(fid, datetime)` should implement. Today, only step 1 is consumed.

The find page can also expose a "show me beaches that allow dogs THIS WEEK" filter, which is just step 1 ∩ step 2 against `current_date`.

---

## Curation cadence and chunking the work

Each axis is curated by humans/LLM at different rates:

| Axis | Total rows expected | Curation effort | Refresh trigger |
|---|---|---|---|
| **A** | ~50 state + ~150 federal-unit + ~thousands city/county | Months of LLM-assisted curation per state. State_dogs_policy is the cheapest (~50 LLM calls). NPS/NWR (~150 calls). City/county is the long tail. | Quarterly review per state; ad-hoc on news of legal change |
| **B** | ~100-300 species/region/site combinations | Once per species per habitat region; mostly static after seeded. | Annual review (Feb–March, before nesting season); USFWS recovery-plan updates |
| **C** | ~500 (subset of operators that publish hours) | Inherits from operator extraction; needs a parser to lift jsonb into rows. | Re-run when operator extraction re-fires |
| **D** | ~all scoreable beaches × 365 = millions of rows/year | Fully automated nightly. | `daily-beach-refresh` Edge Function on a schedule |

**Chunking the curation work into commit-able units:**

1. **Single state, single seed** — adds a row to `state_dogs_policy` with state-statute research. ~30-min effort. Unblocks the state launch. *This is the unit that the new `state_policy_seed` phase enforces.*
2. **Single federal unit, single seed** — adds a row to `pad_us_unit_dogs_policy` with NPS/NWR rules research. ~30-min effort. Unblocks all beaches contained in that polygon (could be hundreds, e.g. Cape Cod NS).
3. **Single species/region seed** — extends `seasonal_closure_seed` with the recovery-plan dates and habitat list. ~1-hr per species. *Plover for Atlantic states is one such unit; sea turtle for SE/Gulf is another.*
4. **Tier classifier upgrade** — implements `beach_dog_status_today(fid, date)` and changes consumer surfaces to use it. One-shot SQL migration + Edge Function update. Then `align_scoreable` re-runs across all states free.

These can ship independently. (1) is a per-state launch blocker; (2) and (3) are per-region efforts; (4) is a one-time platform investment.

---

## What changes in the pipeline tomorrow

After today's work (`20260509_policy_seed_phase_helpers.sql` + the three new orchestrator phases), a state-launch run looks like:

```
precheck                  ← (existing) external sources loaded
state_policy_seed         ← NEW — assert state_dogs_policy has row; halt if missing
federal_policy_seed       ← NEW — advisory pending count for federal coastal units
seasonal_closure_seed     ← NEW — assert no pending seasonal-closure seeds; halt if pending
operators                 ← (existing) per-state seed
arena_seed                ← (existing)
... rest unchanged ...
```

The three new phases sit BEFORE the data-emission phases (`promote` and onward), so the BEP populator chain runs with full Axis A + B context the first time. Today's RI fresh-state-quiet-zero (issue #19) becomes a halted canon at `state_policy_seed` instead, with a clear template for the operator to fill out.

Future work to fully realize the chunking model:
- **Axis A**: Add `valid_from`/`valid_until` to the four static tables.
- **Axis B**: Migrate `beach_dog_policy.dogs_prohibited_start/end` (single window) to `beach_seasonal_closures` (multiple windows). Wire `populate_from_seasonal_closures_gold(fid)` into `promote_to_gold` + `refire_bep_cascade`.
- **Axis C**: Parser populator for `operator_dogs_policy.time_windows` jsonb → `beach_time_of_day_restrictions`. Wire into populator chain.
- **Tier classifier**: Implement `beach_dog_status_today(fid, datetime)` combining the four axes. Switch consumer surfaces to call it for "right-now" verdict; reserve `beach_location_tier()` for category badges.

---

## Open questions

- **Time zones for Axis C:** beach times are local. `beaches_gold.timezone` is set; the time-of-day evaluator should use it. Worth double-checking when the parser populator is written.
- **Multi-year transitions for Axis A:** if a state ordinance changes mid-year, the `valid_from`/`valid_until` model handles it cleanly, but the populator needs to filter by `current_date` between those bounds. Today's emitters don't filter by validity at all (they assume exactly one row per state).
- **Caching for Axis D + B combination:** the find page wants "beaches dog-friendly this week." That's Axis A ∩ Axis B against `current_date`, which is cheap to evaluate but expensive at scale. Probably fine to do at query time, but if it gets hot a materialized view keyed on `current_date` is a reasonable optimization.
- **What's a "closure_type" worth distinguishing?** Plover, sea turtle, swim-ban, beach-grooming, salmon, least tern. Probably 6-8 enum values is enough. The reason to make them an enum vs free-text is to surface the closure cause to the user ("Closed for snowy plover nesting") rather than just "closed."
