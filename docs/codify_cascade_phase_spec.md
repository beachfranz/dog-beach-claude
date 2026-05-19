# Codify-cascade pipeline phase — design spec

**Status:** draft 2026-05-19
**Author:** Claude (Franz directive 2026-05-19 — promote ad-hoc per-jurisdiction codify dispatch to a formal pipeline phase)
**Related:** `[[never-solve-same-problem-twice]]`, `[[promote-ad-hoc-tools-to-process]]`, `[[script-defers-dispatch-agents]]`, `feedback_jurisdiction_playbook_pointer.md`, `docs/jurisdiction_policy_source_playbook.md`

## Why this phase

Today, codify work is ad-hoc per state: Franz says "codify WA federal" → I dispatch 7 parallel agents → 4 of 7 report "already done" because a prior session shipped them. The work is being re-derived because there's no canonical inventory of "what's covered" vs "what's open."

**Today's evidence** (2026-05-19 WA federal codify push):
- Ebey's Landing — ps 278 already shipped (Late 2026-05-18)
- Lake Roosevelt NRA — ps 279 already shipped (covers 4/5 beaches; +1 incremental for Colville Flats)
- Daniel J. Evans Wilderness — covered by ps 276 (Olympic NP parent)
- Olympic NP — ps 276 already shipped
- → 4 of 7 agents redundant. ~15 min of LLM/web spent re-deriving known-good state.

Meanwhile **OR codify** finding from earlier today: 15 of 15 OR coastal jurisdictions (counties + cities) defer via state baseline. Without an automated phase that checks this gate up-front, every new state expansion repeats the same dispatch-then-discover-it's-a-no-op loop.

**Phase intent:** A single `--phase codify_cascade` step in `run_state_pipeline.py` that:
1. Enumerates the codify universe for the state (federal units, counties, cities)
2. Pre-checks existing policy_source coverage for each
3. Dispatches codify agents ONLY for genuinely-uncovered units
4. Applies emitted migrations idempotently
5. Verifies row counts vs the inventory

Net effect: state expansion (MA, FL, MI next per `[[mvp-plus-ca-or-wa]]`) just runs the phase and gets all codify coverage for free, without spawning redundant agents.

## Architecture

```
Phase: codify_cascade
├── Step 1: Enumerate codify universe for state X
│   ├── Federal: derive_policy_source_for_jurisdiction.py --list-federal-coverage --states X
│   ├── Counties: SELECT name FROM counties WHERE state = X AND <has scoreable beaches>
│   └── Cities: SELECT name FROM jurisdictions WHERE state = X AND place_type='C1' AND <has scoreable beaches>
│
├── Step 2: Pre-check coverage per unit
│   ├── For each federal unit: SELECT ps FROM policy_source WHERE agency_id=X AND citation ILIKE %unit%
│   ├── For each county/city: dry-run derive_policy_source_for_jurisdiction.py
│   │     - If outcome=defer_state_baseline_covers → skip
│   │     - If outcome=exists_already → skip
│   │     - If outcome=needs_codify → queue
│   └── Output: codify_queue.json (only units that need agents)
│
├── Step 3: Dispatch codify agents in parallel for queued units
│   ├── Per-unit playbook prompt (federal vs county vs city template)
│   ├── Each agent: find URL + verbatim + rule → run codify command IN FOREGROUND
│   └── Wait for all completions (or per-unit timeout)
│
├── Step 4: Apply emitted migrations
│   ├── Scan supabase/migrations/ for new files matching state pattern
│   ├── For each: apply via psql (per-migration approval required per HARD rule)
│   └── (Or: enqueue for batch human-approval per existing apply gate)
│
└── Step 5: Verify
    ├── Per unit: count beach_policy_source rows; compare to expected
    ├── Per state: count beaches with ANY policy_source link vs scoreable total
    └── Emit phase report: covered / new / blocked / still-missing
```

## Pre-check signals (Step 2)

What "already covered" means per unit type:

**Federal unit:**
- `policy_source` row exists with `agency_id = <unit's federal agency>` AND citation/source_url matches the unit name. The script already does this via `WHERE NOT EXISTS (source_url)`.
- AND `beach_policy_source` has rows linking the expected fids.
- Edge case: parent unit (e.g., Olympic NP) covers child unit (e.g., Daniel J. Evans Wilderness). Check parent-unit coverage too via PAD-US containment.

**County / City:**
- `derive_policy_source_for_jurisdiction.py` already does this via Step 1.5 state-baseline check + Step 1 scope check.
- Today's OR finding: ALL 15 OR jurisdictions returned `defer_state_baseline_covers`. The pre-check is just "run the script in dry-mode and read the outcome."

**Implementation note:** Step 2 can wrap the existing script with `--dry-run` (or read its existing outcome enum). No new SQL needed.

## Federal-unit dispatch templates

The per-unit codify prompt has 4 federal variants (NPS / BLM / USFS / USFWS / NOAA). Each agent prompt is parameterized by:
- `unit_name` — from PAD-US `unit_name`
- `agency_name` + `agency_type` + `pad_mng_name` — from federal coverage map
- `baseline_cfr` — e.g., "36 CFR §2.15" for NPS, "43 CFR §8365" for BLM
- `expected_rule_default` — usually `on_leash` (NPS/BLM/USFS baseline) but `not_allowed` for wilderness areas + USFWS NWRs
- `unit_url_hint` — `nps.gov/<parkcode>/planyourvisit/pets.htm` etc.

Template lives in `scripts/codify_phase/federal_agent_prompts.py`. One function per agency. Mirrors today's hand-written agent prompts.

## County / City dispatch template

Single template since the playbook handles both. Prompt parameterized by:
- `jurisdiction_name`
- `state`
- `platform_hint` — municode / codepublishing / amlegal / etc.

Only fires when Step 2 pre-check returns `needs_codify` (rare per today's evidence).

## Apply step (Step 4)

Per `[[promote-ad-hoc-tools-to-process]]` + the existing per-migration apply gate: the phase should NOT auto-apply. Two options:

**Option A (proposed):** Phase emits migrations + a `migrations_to_apply.json` manifest. A separate `--phase codify_cascade_apply` step processes the manifest with explicit human apply step. This keeps the apply gate intact and lets Franz inspect before applying.

**Option B:** Phase applies inline. Requires per-migration approval mid-pipeline, which breaks the long-running orchestrator pattern.

Go with A.

## Verification (Step 5)

Per `feedback_governance_required_per_beach.md`, the pipeline already has `count_unattributed_beaches(state)`. Add an analog: `count_uncovered_scoreable_beaches(state)` that returns scoreable beaches with zero `beach_policy_source` rows. Phase fails if count > expected_floor (per-state config).

## Integration with run_state_pipeline.py

Phase position: AFTER beach catalog ingestion (GNIS) and PAD-US load — needs the PIP populated to know which units contain scoreable beaches. BEFORE the consensus/scoring phases — codify feeds the consensus engine.

Per `[[use-pipeline-infrastructure]]` HARD rule: this is a phase, not a parallel driver.

## Per-state config

`config/codify_phase_<state>.yaml`:
```yaml
state: WA
federal:
  exclude_agencies: []                # blacklist by agency_id if needed
  expected_rule_overrides:
    "Daniel J. Evans Wilderness Area": not_allowed
counties:
  state_baseline_threshold: 90        # % beaches the baseline must cover to defer (matches script default)
cities:
  state_baseline_threshold: 90
  skip_known_no_codify:               # cities verified to have no separable codify
    - Bandon                          # OR coastal NONE cohort
    - Cannon Beach
    - Manzanita
```

## Open questions

- **Per-beach refinement:** Olympic NP's codify split 12 beaches into 3 on_leash + 9 not_allowed via curator review. Phase needs to either (a) preserve manual splits, or (b) accept that unit-level rules get curator-refined separately. Currently the ON CONFLICT DO NOTHING approach preserves manual refinement.
- **Operate vs Codify boundary:** Operator-posted policies (Cape Lookout, Dabney, Shore Acres) are NOT codify — they're Operate-pipeline. Phase should NOT try to codify them; the pre-check needs to know about the operator-posted exceptions.
- **Step 6.8 web-search rescue:** for the 17 OR coastal NONE cities, the playbook calls for web-search fallback. Out of scope for v1 of this phase (defer per `[[codify-step-6-8-web-search-fallback]]`).

## Effort estimate

- Step 1 (enumerate): ~1 hr (mostly wrapping existing scripts)
- Step 2 (pre-check): ~2 hr (parse script outcomes; coverage SQL)
- Step 3 (dispatch): ~3 hr (federal templates + county/city template)
- Step 4 (apply manifest): ~1 hr (file scan + manifest emit)
- Step 5 (verify): ~1 hr (count_uncovered_scoreable_beaches RPC + assertions)
- Wiring into run_state_pipeline.py: ~1 hr
- Per-state config + docs: ~1 hr

**Total: ~10 hr.** Ships in 2-3 sessions.

## What this UNBLOCKS

- State expansion to MA, FL, MI per `[[mvp-plus-ca-or-wa]]` is a one-command codify run instead of multi-day ad-hoc dispatch.
- The `[[never-solve-same-problem-twice]]` rule for codify is automated; the inventory + pre-check encodes the solution.
- The `[[script-defers-dispatch-agents]]` hard rule becomes a phase step instead of a manual reaction.

## What this DOES NOT solve

- Doesn't replace human verbatim quoting (still LLM/web work for genuinely-new units)
- Doesn't auto-resolve operator-vs-codify boundary disputes (still a per-unit judgment call)
- Doesn't backfill the 7 OR coastal port districts that aren't in PAD-US (need a separate operator-codify lane)
