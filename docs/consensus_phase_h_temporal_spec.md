# Phase H — temporal extraction + structured shape population

Spec for the **zones / seasons / day-parts** extension to the
consensus engine. Producer-side gap identified 2026-05-17: ~744 CA
beaches have zero structured temporal data despite extensive
seasonal/day-part rules in their source text. Consumer surface
already handles the shape; producer doesn't write it.

Status: spec draft. Not started.

---

## What the consumer already expects

`beach.html` + `mobile-beach.html` (identical shape) ALREADY consume:

```jsonc
{
  "seasons": [
    {
      "name":  "Summer",                  // optional season label (rendered)
      "dates": "Jun 15-Labor Day",         // optional dates string (rendered)
      "regions": [
        {
          "name":  "North Beach",          // optional zone label (rendered)
          "rule":  "off_leash",            // optional region-level fallback
          "time_windows": [                 // day-parts at region level
            {"start": "06:00", "end": "20:00", "rule": "off_leash"},
            {"start": "20:00", "end": "06:00", "rule": "not_allowed"}
          ],
          "sections": {
            "sand": {
              "rule": "off_leash",
              "time_windows": [...],         // OR day-parts at section level
              "evidence": {...},
              "modifier": {...}
            }
          }
        }
      ]
    }
  ],
  "global_notes": "..."
}
```

Plus flat columns the consumer ALSO reads:
- `beach_dog_policy.dogs_prohibited_start` (HH:MM) — daily prohibition window
- `beach_dog_policy.dogs_prohibited_end` (HH:MM)
- `has_on_leash`, `has_off_leash` (booleans for fallback availability)

Consumer functions that consume this shape (`beach.html`):

| Function | Reads | Why it matters |
|---|---|---|
| `_hourlyAvailability(hour, zr, pref)` | `seasons[].regions[].sections{}.time_windows[]`, falls through to `region.rule`, `section.rule` | The per-hour bar chart's per-hour color (no_go vs go vs avail). The DAY-PART value-delivery. |
| `_effectiveRuleAt(node, hour)` | `node.time_windows[]` then `node.rule` | Picks the rule applicable at a given hour. Handles wrap-around (10pm-6am). |
| `_tw_contains(tw, hour)` | `tw.start`/`tw.end` HH-prefix | The actual hour-in-window test. |
| `_sandOrWaterProhibited(zr)` | walks every season/region/section, checks each `time_windows[]` | The "no-go" page-level verdict. Any zone or time_window permitting dogs un-blocks the whole beach. |
| `_ruleMapFromZoneRules(zr)` | walks seasons → regions → sections, picks most-restrictive per section | Multi-zone section tiles on the page. |
| `_zrRegions(zr)` | returns regions across all seasons | Multi-zone GRID layout (one card per region). |
| Flat-column reader (line 1971+) | `dogs_prohibited_start/end` | The daily prohibition advisory ("6:00am–9:00am closed"). |

**So the consumer is fully spec'd to render multi-region + multi-season + day-parts.** It's been ready; producer just hasn't fed it.

---

## What the producer writes today (the gap)

| Shape | Producer | Today's state |
|---|---|---|
| `regions[0].sections{}` keys | both legacy + entity injectors | ✓ Works |
| `regions[1+]` (multi-region) | nothing | ✗ Always 1 region |
| `seasons[]` populated | legacy `_zr_inject_sand_from_policy` only | 18 beaches; all single "All year" season |
| `seasons[].name` named | nothing | All null or "All year" |
| `seasons[].dates` populated | nothing | All null |
| `region.time_windows[]` | nothing | Empty |
| `section.time_windows[]` | nothing | Empty |
| `dogs_prohibited_start/end` flat cols | nothing | 0 of 744 populated |
| `region.name` | nothing | All null |

Every place where we have temporal/zone information in source text (`policy_source.full_text`, `beach_policy_source.evidence_verbatim`, `beach_policy_source.status_note`, `beach_policy_source.rule_modifier`), it stays as prose — never extracted to the structured shape.

---

## Scope of the gap (the evidence is there)

Concrete examples from today's session where prose captures temporal/zone nuance:

| Beach (fid) | Source | Temporal in prose | Structured? |
|---|---|---|---|
| 8560 Del Mar Dog Beach | DMMC §4.08.020(B)(1) | "day after Labor Day through June 15, AND dawn-8am year-round" | ✗ |
| 8992 Del Mar Beach (main) | DMMC §4.08.020(C) | "June 15th through Labor Day, dogs prohibited" | ✗ |
| 6411 Rosie's Dog Beach | LB published rules | `modifier.hours: "6 AM - 8 PM"` (modifier IS populated) | partial — modifier exists but no time_windows[] |
| 8264 Hermosa City Beach | §6.08.020 | year-round prohibition | trivially structured |
| 9059 Imperial Beach | §12.60.100 | "central IB Blvd-Palm Ave strip prohibited; rest on-leash" | ✗ (sub-area, no zones) |
| Various Pismo / Oceano / Vandenberg | snowy plover rules | "March 15 – September 15 closure" | ✗ (cross-state pattern) |
| Carpinteria State Beach 8673 | §6.04.240 | "public beach between Linden Avenue and Ash Avenue" | ✗ (sub-area, no zones) |
| Cannon Beach / Seaside / Rockaway (OR) | OAR §736-030-0010 | named voice-control cities | ✗ |
| HBDB 6212 | §13.08.070 + MOU | seasonal HBDB designation + city carve-outs | partial via legacy seasons[] |

So Del Mar / Pismo / Imperial Beach / Carpinteria / OR voice-control cities — all have rich nuance captured in prose, none flowing to structured form.

---

## Phase H — proposed work

### H1. Schema extensions (small)

Either add columns to `beach_policy_source` OR a sibling temporal table.
Recommend **sibling table** (`beach_policy_source_temporal`) to keep bps
clean for the M:M base case:

```sql
CREATE TABLE public.beach_policy_source_temporal (
  beach_policy_source_id bigint NOT NULL REFERENCES public.beach_policy_source(id) ON DELETE CASCADE,
  -- Seasonal (recurring annual; MM-DD ints to skip year)
  effective_from_md text NULL,   -- e.g., '06-15', '09-08' (day after Labor Day; computed)
  effective_to_md   text NULL,
  season_label      text NULL,   -- e.g., 'Summer', 'Snowy Plover Closure'
  -- Daily window (24h, supports wrap-around — end < start means crosses midnight)
  daily_start       time NULL,
  daily_end         time NULL,
  -- Categorical (rule applies only ON this exception window; outside = base rule)
  window_kind       text NOT NULL CHECK (window_kind IN
                      ('seasonal_carve_out','daily_carve_out','seasonal_and_daily','year_round')),
  -- For named-anchor dates that change yearly (Labor Day, Memorial Day, etc.)
  anchor_start      text NULL,   -- 'labor_day_after', 'memorial_day', 'dst_start', 'plover_open'
  anchor_end        text NULL,
  notes             text NULL,
  PRIMARY KEY (beach_policy_source_id, window_kind, daily_start)
);
```

Anchor functions for variable dates:
```sql
CREATE FUNCTION public.resolve_anchor_date(anchor text, year int) RETURNS date AS $$
  CASE anchor
    WHEN 'labor_day' THEN ... (first Monday of September)
    WHEN 'labor_day_after' THEN ... (Labor Day + 1)
    WHEN 'memorial_day' THEN ... (last Monday of May)
    WHEN 'dawn' / 'dusk' THEN ... (use beach lat/lon + suncalc)
    ELSE NULL
  END
$$;
```

### H2. LLM extractor (the heavy lift)

New script `scripts/extract_temporal_from_policy_source.py`:

- Input: a `policy_source.id` (or all ps rows pending extraction)
- Pulls `ps.full_text` + all linked `bps.evidence_verbatim` + `bps.status_note` + `bps.rule_modifier`
- Sonnet prompt: "extract temporal carve-outs from this text. Return JSON with structured fields."
- Sonnet returns: `{seasonal_windows: [...], daily_windows: [...], named_anchors: [...]}`
- INSERTs / UPDATEs `beach_policy_source_temporal` rows per bps + window
- Idempotent via composite PK + content hash

Cost: ~$0.005-0.01 per ps row × ~250 ps rows in CA = $1.25-2.50 one-shot.

Same playbook as the existing description generator — uses truststore +
prompt-cached system prompt + chunked execution per [[chunked-subprocess]].

### H3. Injector extension

Extend `_zr_inject_from_policy_sources` to:

1. Group rows by `(beach_fid, section)` AND temporal window
2. Emit multi-season structure when ≥2 distinct seasonal windows present
   for the same (fid, section)
3. Populate `section.time_windows[]` from `beach_policy_source_temporal.daily_*`
4. Populate `seasons[].name` from `season_label` or computed from anchors
5. Populate `seasons[].dates` from resolved `effective_from_md`/`_to_md`
6. Compute `seasons[].regions[].name` from `bps.region_name` or `section_overlay` decomposition (if we add `region_name` to bps — see open question 1)

### H4. Promoter extension

Extend `promote_entity_dogs_to_beach_dog_policy` (via
`_canonical_dogs_from_policy_sources`):

- Compute `dogs_prohibited_start` / `dogs_prohibited_end` from the
  most-restrictive `daily_carve_out` window across all bps rows for the beach
- If multiple windows exist (rare), pick the longest contiguous prohibition

### H5. Backfill

Once H1-H4 land:
1. Run H2 extractor against all existing 250 CA ps rows. Populates
   `beach_policy_source_temporal`.
2. Fire `promote_entity_dogs_to_beach_dog_policy()` globally (existing
   helper from H4) — flat columns populate.
3. Re-fire `_promote_zone_rules_for_fid()` globally — zone_rules
   structure rebuilds with seasons[] + time_windows[].
4. Verify against the 9 example beaches from the table above.

### H6. Quality gates

- **Temporal consistency**: if `daily_start > daily_end`, it's wrap-around (e.g., 22:00–06:00). Validate; flag suspicious patterns.
- **Anchor resolution**: Labor Day / Memorial Day / dawn / dusk must resolve correctly. Add unit tests.
- **Round-trip test**: a beach with known temporal rules (Del Mar §4.08.020) should produce zone_rules that the consumer renders correctly.
- **No regression on year-round beaches**: 90%+ of beaches have no temporal carve-outs; their zone_rules shape should be unchanged after H lands.
- **Manual-curator protection**: respect `source='manual_curator'` everywhere (HBDB still works).

---

## Where it fits in the pipeline

Three options (in [[jurisdiction-playbook-pointer]] tradition):

1. **As a new pipeline phase** — `extract_temporal_from_ps` between
   `operator_merge` and `bep_refire`. Runs incrementally on newly-added
   ps rows.
2. **As a trigger on ps INSERT** — analogous to today's
   `tg_stmt_*_beach_policy_source` triggers, but firing the temporal
   extractor (which is LLM-bound, so async via job queue).
3. **As a one-shot batch** — driven by the playbook
   ([[jurisdiction-playbook-pointer]]) as Step 4.5 ("extract temporal
   patterns"). Adds a `--with-temporal` flag to the playbook script.

Recommend **(1) + (3)** — pipeline phase for incremental + batch for
explicit launches.

---

## Open questions

1. **Multi-region vs sub-area sections**: today we encode sub-area via
   distinct `section` values (`sand` + `sand_north_overlay`). Should we
   ALSO add `bps.region_name`? Or model differently?
   - **My read:** add `bps.region_name TEXT NULL` and let the injector
     group by it when present, fall through to section-based when not.
     Backward-compatible.

2. **Anchor functions in SQL or Python?**
   - SQL is simpler (no extra dependency); Labor Day / Memorial Day are
     basic date math. Dawn/dusk needs lat/lon + a sunrise/sunset library.
   - **My read:** SQL for date anchors; defer dawn/dusk to a Python
     helper that writes back to the resolved-date columns when needed.

3. **How aggressive on the extractor?**
   - **Tier 1 (current):** explicit named windows ("June 15 to Labor Day",
     "6:00 AM to 8:00 PM").
   - **Tier 2 (next):** computed windows ("dawn to 8am" needs anchor +
     beach coords).
   - **Tier 3 (later):** inferred windows ("typically summer" → may not
     warrant structuring).
   - **My read:** ship Tier 1 only. Tier 2/3 are follow-ups.

4. **Cross-state plover closures (Mar 15 – Sep 15)**: a single ps row
   covers many beaches. Should the temporal row attach to the ps once OR
   per-bps? **My read:** per-bps for join simplicity; redundant rows are
   cheap.

5. **Validation gate**: require human review on extracted temporal
   patterns before commit? Some bps rows will have ambiguous prose. Per
   the playbook tenet "defer > fabricate," the extractor should output a
   `confidence` field and flag low-confidence extractions for human review.

---

## Effort estimate

- H1 schema: ~30 min (one migration)
- H2 extractor: ~1.5 hr (LLM script + truststore + chunked runner)
- H3 injector: ~1 hr (function update + tests against the 18 existing seasons[]-beaches + the 9 example beaches)
- H4 promoter: ~30 min (view + function update)
- H5 backfill: ~30 min (run extractor on 250 ps rows + global re-fire)
- H6 quality gates: ~30 min

**Total: ~4-5 hours of focused work.**

---

## Related

- [[handoff-consensus-source-authority]] — the original 4-task plan (Tasks 1-3 done; Task 4 validate)
- [[consensus-source-authority]] — design pin (the WHY)
- `docs/consensus_engine_current_state.md` — discovery output (2026-05-17 addendum)
- `docs/wave3_pipeline_integration_design.md` — Phases A-G (this is the natural Phase H)
- `docs/jurisdiction_policy_source_playbook.md` — the playbook this extends
- `beach.html` lines 2255-2376 — the consumer renderers
- `mobile-beach.html` — identical shape
