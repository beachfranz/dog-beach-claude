# Codify → PIP → Resolver — spatial attribution architecture

State-agnostic architecture for attaching statutes to beaches via spatial polygons. Supersedes implicit assumptions in `docs/codify_cascade_v1_runbook.md` (which framed codify as deciding beach-by-beach) and dissolves the Step 4 "rule-scope inference" from `[[codify-v1-governance-aware-design]]` into the resolver.

Crystallized 2026-05-18 morning during a strategy conversation. Three corrections from prior framing baked in:

1. **CA is already codified.** Codify v1's value is state-agnostic — for WA, OR, MI, MA, and future states.
2. **Title placement ≠ rule scope.** A rule's scope is determined by its TEXT, not the title it lives in. Title heuristics are HINTS at most.
3. **Beach-in beats rule-out.** Don't infer "which beaches does this rule cover." Enumerate "which rule-bearing polygons does this beach fall inside" and let a precedence resolver pick.

---

## The inversion

**Old (rule-out) framing:** start with a rule → infer which beaches it applies to via scope/heuristics → write bps rows.

**New (beach-in) framing:** PIP each beach against all rule-bearing polygons → enumerate candidates → per-domain precedence resolver picks the winning polygon → bps row joins beach to winning rule.

Beach-in handles layered authority naturally. A beach inside city + county + state-park + NWR has 4 candidate authorities; precedence rules pick the winner per authority-domain (dog_policy, water_quality, etc.).

---

## The 4-step pipeline

```
1. Codify (producer)                      ──► (polygon_id, rule_text, subtype, domain) tuples
2. PIP (spatial join, downstream)         ──► beach_polygon_candidates rows
3. Resolver (per-domain precedence)       ──► winning polygon per (beach × domain)
4. bps materialization                    ──► beach_policy_source rows
```

## Cross-cutting layer: web_search bypass for Cloudflare / SPA / thin fetch

Added 2026-05-18 after Franz pushed on a real spec gap: **the previous codify dispatch was using Anthropic's `web_search` tool as the standard escape valve for fetch failures, and the new pipeline didn't carry that pattern over.** Evidence: 15 existing CA ps rows document "captured via web search" in their `full_text` — 6% of CA codification used this bypass.

When the deterministic fetch returns thin content (< 500 chars OR matches Cloudflare-challenge keywords like "performing security verification" / "ray id" / "just a moment"), the rule-decision LLM call is re-invoked with the `web_search_20250305` Anthropic server-side tool enabled. Sonnet searches the web (Google's indexed snapshots typically bypass Cloudflare's per-request CAPTCHA), finds the verbatim rule text from alternate sources, and returns the same `CodifiedRule` shape. The original deep-link URL is preserved as `source_url` for citation provenance.

**Cost:** ~$10/1000 searches + token usage = ~$0.01-0.03 per jurisdiction.

**Validated 2026-05-18 against** vancouver.municipal.codes/VMC/8.24.110 + bellevue.municipal.codes/BCC/3.43.145 — both Cloudflare-walled to direct fetch (Playwright, urllib, WebFetch all 403); both auto-committed at conf 0.88/0.90 via web_search bypass.

This layer addresses the class of platforms that uses Cloudflare Turnstile (the next-gen CAPTCHA that even headless Chromium can't pass): the `*.municipal.codes` family (General Code), some `*.gov` sites, and various agency homepages. It also handles JS-rendered SPA section-anchors where the regex selector extraction can't navigate to the operative section.

## Cross-cutting layer: per-platform fetch routing

Spec gap surfaced 2026-05-18 mid-build: **fetch-mode varies per platform across every step that touches URLs**, not just Step 4 verbatim fetch. The codify driver hits external code-publisher sites for validity checks (Step 2), TOC navigation (Step 3), AND verbatim fetch (Step 4). All three need the right transport per platform.

| Platform | Mode | Why | Selector |
|---|---|---|---|
| Municode | **Playwright** | TOC + content both JS-rendered | `.codes-chunks-pg` for content |
| ecode360 | **Playwright** | Heavy JS rendering | varies |
| Cloudflare-fronted (e.g. some codepublishing pages) | **Playwright** | JS challenge | none |
| codepublishing (standard) | urllib | Static HTML | none |
| amlegal | urllib | Static-ish; `.section-content` for content | `.section-content` |
| qcode | urllib | Static enough | none |
| county.codes | urllib | Mostly static / PDF | none |

**Implementation pattern:**
- A `PLATFORM_FETCH_CONFIG` dict (in the codify driver) maps each platform to `{mode: playwright|urllib, wait_seconds, selector}`.
- A `_smart_fetch(url, platform)` router dispatches to the right transport.
- Both validity checks (Step 2) and TOC fetches (Step 3) call through this router with the platform name passed down.
- The existing `scripts/fetch/fetch_html.py` (the Playwright fetcher used during CA codification) is the implementation for the Playwright path.

**Why this matters for the architecture, not just implementation:**
- Codify v1's cost model depends on it. Playwright is ~12s/call; urllib is ~1s. Per-platform routing keeps wall time tractable.
- Per-platform selectors are first-class metadata, not magic strings buried in fetch code.
- New platforms (state-specific code publishers as we expand to MI, MA, TX, etc.) just need a `PLATFORM_FETCH_CONFIG` row added — the algorithm doesn't change.

**Codify happens BEFORE PIP.** Codify is the gate that activates a polygon for the resolver. Until a polygon has been codified for a given domain, it's invisible to the resolver and PIP doesn't bother joining against it.

This means:
- PIP is cheap — only joins against polygons that actually have rules
- Codify is decoupled from spatial reasoning — it just produces (polygon, rule) tuples; doesn't decide which beaches anything applies to
- Resolver is the only place heuristics live (title-name signals, governance hierarchy, layer specificity)

---

## The 8 polygon sources

| # | Source | Layers it provides | Status |
|---|---|---|---|
| 1 | **PAD-US** (USGS national) | NPS, USFWS NWRs, USFS, BLM, DoD, state parks, county parks, city parks, tribal reservations, special districts (13 layers via mng_type + mng_name) | **29 states loaded** (incl. CA/WA/OR/MI/MA). Heavy hitter. |
| 2 | **TIGER** (Census national) | State boundary, county polygon, incorporated city polygon, CDP polygon (3 layers via `jurisdictions.place_type`) | CA loaded; OR/WA/MI/MA need verification. |
| 3 | **OSM dog parks** (`leisure=dog_park`) | Dog-park polygons nationally | **Loaded 2026-05-18: 6,163 polygons US-wide** (CA 858, WA 336, OR 253). `public.osm_dog_parks`. 7 chunks still need retry (UT/AZ/NM/WY, IL/IN/MI, OH/KY, LA/MS/AL, FL, New England). |
| 4 | **NOAA Marine Sanctuaries** | ~14 sanctuaries nationally (OCNMS, MBNMS, CINMS, etc.) | Not loaded. One-shot ingest from sanctuaries.noaa.gov. |
| 5 | **USFWS Critical Habitat** | ESA-designated polygons (snowy plover seasonal closures, etc.) | Not loaded. ecos.fws.gov per-species. |
| 6 | **State DFW / DSL — MPAs** | CDFW MLPA, ORDFW reserves, WDFW reserves | Per-state; not loaded for non-CA. |
| 7 | **State DNR / Lands — tidelands** | CSLC (CA), WA DNR Aquatic Lands, OR DSL | Per-state; not loaded. |
| 8 | **Bespoke** (concession / land trust / HOA) | Case-by-case | Not centrally sourced; per-place. |

PAD-US + TIGER + OSM dog parks carry the bulk of national coverage. The remaining 5 sources each contribute one layer.

## Entity classes (vs authority polygons)

Two classes of spatial objects in the system:

| Class | Examples | Role |
|---|---|---|
| **Entities** (need attribution) | Beaches, dog parks | PIP'd against authority polygons; each entity gets its own (entity, authority, rule) attribution |
| **Authority polygons** (carry rules) | Cities, counties, state parks, NWRs, NPS units, special districts | Codify attaches rules; entities PIP into them |

**Beaches and dog parks are parallel entity streams** with independent attribution pipelines. They share authority polygons but don't entangle. Dog-park-inside-beach (4 cases across CA/WA/OR — `~0.2%`) and beach-inside-dog-park (rarer still) are too sparse to bake into v1 architecture. Treat as edge cases handled by region_name if they ever matter.

**Deferred cross-class use case:** beaches with no/poor sand access could surface nearby dog parks as a recommendation overlay ("the beach is cliff-only but there's a dog park 0.4 mi inland"). Captured at [[deferred-beach-dogpark-cross-reference]]. Data is now available; UI is the missing piece.

---

## Per-domain precedence (resolver rules)

The resolver runs per-domain. Each domain has its own precedence ordering. Sketches:

| Domain | Precedence (highest wins) |
|---|---|
| `dog_policy` | federal-overlay (NPS/NWR) > state-park (PAD-US STAT) > special-district > city > county > state baseline |
| `water_quality` | state environmental dept > county health dept > city health dept |
| `bacteria` | nearest sampling station regardless of polygon |
| `parking` | city > county > state-park > federal |
| `tidelands` | state lands agency > county > city |

Within a tier, more-specific overlay wins (e.g., NPS Superintendent's Compendium > 36 CFR baseline). Within "same polygon, multiple rules," text-restrictiveness signals can break ties (e.g., a Parks-title rule that scope-narrows to "in county parks" only wins for beaches whose candidate set includes a county-park polygon).

Title-name heuristics from yesterday's governance-aware design live HERE — as resolver tiebreakers, not as codify-time filters.

---

## Prior art (already in DB)

The architecture exists in shape for CPAD/federal; we're generalizing it to all 7 sources.

| Component | Existing implementation | Generalization needed |
|---|---|---|
| PIP enumeration | `beach_cpad_candidates` (one beach → multiple CPAD units) | Extend to all polygon sources → `beach_polygon_candidates` (one beach → multiple polygons across all 7 sources, tagged by layer type) |
| Per-domain attribution | `pad_us_unit_dogs_policy` | Extend to all polygon types + all domains |
| Precedence-picked winner | `cpad_unit_for_beach` | Generalize to per-domain resolver across all polygon types |
| Final attribution | `beach_agency` with `precedence_rank` | Becomes downstream consumer of resolver output |

---

## What this changes vs the existing runbook

`docs/codify_cascade_v1_runbook.md` Track 1 sub-tasks 1-12 were sequenced for a codify script that BOTH produces rules AND maps them to beaches. With beach-in framing:

- Track 1 (Codify v1) **shrinks**. Steps 1-4 + 7-9 still apply (scope check, platform discovery, navigation, fetch, migration emit, commit, temporal extract). Step 5 (beach mapping) is removed — codify doesn't decide which beaches. Step 6 (rule decision) stays but only produces (polygon, rule) tuples.
- Track 2 (Cascade integration) is unchanged.
- **New track 3 — Spatial attribution pipeline:** load 5 net-new polygon sources, build `beach_polygon_candidates`, build state-agnostic per-domain resolver.

---

## What's still load-bearing from the governance-aware design

`[[codify-v1-governance-aware-design]]` had 4-step routing. Re-examined in the beach-in frame:

- **Step 0 (classify jurisdiction)** — STILL load-bearing for codify. Tells you what kind of platform to probe (city → SUBJECT-title platforms; county → DEPARTMENT-title platforms). Lives in codify.
- **Step 0.5 (triage)** — STILL load-bearing. CDP routes to parent county; tribal defers; federal branches to federal-codify path.
- **Step 1 (per-state platform priority)** — STILL load-bearing for codify discovery.
- **Step 2 (per-governance-class title heuristic)** — STILL load-bearing but as a DISCOVERY heuristic, not a scope filter.
- **Step 4 (rule-scope inference)** — **DISSOLVES** into the resolver. The codifier doesn't decide scope; it just attaches the rule to the source jurisdiction's polygon. The resolver's precedence rules + the rule text's restrictiveness handle scope at attribution time.

---

## Net work to ship

| Track | Component | Effort |
|---|---|---|
| Codify v1 | Driver script (shrunk; no beach-mapping) | 3-4 hr |
| Polygon ingestion | Load 5 net-new sources (NOAA NMS, USFWS critical habitat, state MPAs, state tidelands, bespoke) | 1-2 days for first 4; bespoke ongoing |
| PIP generalization | `beach_polygon_candidates` table + per-source PIP runner | 1 day |
| Resolver | Per-domain precedence engine; generalizes `cpad_unit_for_beach` | 2-3 days |
| bps materialization | Update existing materialization to consume resolver output | ~half day |
| End-to-end test on WA | Run new pipeline against WA; validate against existing CA via regression | 1 day |

**Total: ~7-10 days end-to-end** for the spatial-attribution side; Codify v1 itself shrinks to ~½ day of the previous estimate.

---

## Worked example: Cape Disappointment State Park (WA)

**PIP enumeration:**
- WA state (TIGER)
- Pacific County (TIGER)
- (unincorporated — likely no city/CDP)
- Cape Disappointment State Park (PAD-US STAT, mng_name=WSPRC)
- Lewis & Clark NHP boundary overlap (PAD-US FED, mng_name=NPS)

**Codify has separately produced rules attached to:**
- WA state → WAC §352-32-060 (state parks pets baseline)
- Pacific County → ordinance leash rule
- Cape Disappointment SP → (none, unless WSPRC posted per-park override)
- L&C NHP → NPS Compendium dog rules (if any)

**Resolver runs for `domain=dog_policy`:**
- federal-overlay (L&C NHP) wins IF Compendium has restrictive dog rule
- else state-park (Cape Disappointment) wins IF per-park override exists
- else state baseline (WAC §352-32-060) wins because beach is inside a state park
- Pacific County loses (lower tier than state-park)

**bps row:** one row joining fid → winning polygon's policy_source.

**Same beach × `domain=water_quality`** picks a different winner (probably Pacific County health dept). Per-domain precedence is independent.

---

## Open design questions

1. **`beach_polygon_candidates` — table or view?** Table = faster reads + caching; view = always-fresh but expensive joins. Recommend table with refresh trigger on polygon ingest or codify.
2. **Per-domain precedence — hardcoded SQL function, YAML config, or DB table?** Probably DB table (`domain_precedence_rules`) so it's queryable + override-able per layered-authority case. YAML is cleaner for review but harder to query.
3. **Title-name resolver tiebreaker** — how does "rule lives in Parks title AND beach is in a county-park polygon" upweight the county-rule's precedence? Probably a multiplier or explicit override row in `domain_precedence_rules`.
4. **Temporal data (Phase H) flow** — temporal carve-outs attach to the winning polygon's rule. Already works post-resolver via the trigger cascade; verify in regression.
5. **What about beaches that fall in NO codified polygon?** Today's playbook would defer (`operator_posted_policy` fallback). In the new architecture, the resolver returns empty; bps materialization writes nothing; codify catches the gap and flags for human review.
6. **Multi-polygon overlap edge cases** — beach straddles two cities (rare but exists). Resolver picks one via tie-break rule; or writes layered bps rows with `region_name` to disambiguate.

---

## Sequencing

```
1. Load polygon sources (the 5 net-new)
2. (Parallel) Generalize PIP → beach_polygon_candidates
3. (Parallel) Build state-agnostic resolver
4. Rewrite Codify v1 to produce (polygon, rule) tuples only
5. End-to-end test on WA (codify Skagit + run pipeline + verify Cape Disappointment + verify a county-park-only beach + verify a federal-overlay beach)
6. Scale to OR / MI / MA
```

Codify v1 driver can be built in parallel with the spatial work because it doesn't depend on PIP output — but bps rows only materialize once both halves are running. CA serves as the regression suite (we have the right answers already; the new pipeline must reproduce them).

---

## Related

- `docs/codify_cascade_v1_runbook.md` — codify mechanics (Track 1 shrinks per this doc)
- `docs/jurisdiction_policy_source_playbook.md` v3 — codify algorithm (steps 5 + 6 update per this doc)
- `docs/codify_wa_audit_deltas.md` — WA-specific findings that motivated the state-agnostic redesign
- [[codify-cascade-vocabulary]] — naming
- [[codify-v1-governance-aware-design]] — Steps 0/0.5/1/2 still load-bearing; Step 4 dissolves into resolver per this doc
- [[two-pass-refire-pattern]] — trigger-cascade gotcha to honor in resolver-driven bps writes
- [[url-resolution-field-guide]] — per-platform anatomy codify still uses
