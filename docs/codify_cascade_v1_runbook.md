# Codify v1 + Cascade integration — next-session runbook

Scope for the work after 2026-05-17. Two complementary tracks per the
[[codify-cascade-vocabulary]] split:

- **Codify v1** — make the per-jurisdiction process runnable as code, not just
  via manual agent dispatch
- **Cascade integration** — wire the Phase B/C/E/H/I machinery into
  `run_state_pipeline.py` as canonical phases (not standalone triggers
  that work but aren't pipeline-resident)

Both are SCOPED here, not started. Read this on session resume, pick a
lane (they're parallelizable), dispatch agents if helpful.

---

## Track 1 — Codify v1

### Goal

```
python scripts/derive_policy_source_for_jurisdiction.py \
  --jurisdiction "City of Goleta" --state CA
```

Runs end-to-end: discover platform → fetch verbatim → map beaches → write
migration → apply → run §9.5 temporal extractor → commit → produce
report. Replaces today's manual + sub-agent process for the per-jurisdiction
backfill.

### Anchors

- `docs/jurisdiction_policy_source_playbook.md` (v3) — the algorithm (steps 1-9.5) the script implements
- `feedback_url_resolution_field_guide.md` (memory pin) — per-platform URL anatomy the discovery step uses
- `scripts/extract_temporal_from_policy_source.py` — the §9.5 follow-up the script must invoke
- `scripts/generate_beach_descriptions.py` — pattern to mirror (truststore + dotenv + supa() helper + Sonnet call structure)

### Deliverables

| # | Artifact | Path | Notes |
|---:|---|---|---|
| 1 | Python driver | `scripts/derive_policy_source_for_jurisdiction.py` | Mirrors `extract_temporal_from_policy_source.py` shape — argparse, truststore, supa(), anthropic_call() |
| 2 | Platform-discovery helper | inline or separate `scripts/derive/discover_platform.py` | Implements the playbook §2 validity-check gauntlet |
| 3 | Verbatim-fetch helper | reuse `scripts/fetch/fetch_html.py` | No change needed |
| 4 | Beach-mapping helper | inline | PIP query per playbook §5 |
| 5 | LLM rule-decider | inline; Sonnet | Per playbook §6 + defer rubric |
| 6 | Migration-template emitter | inline | Per playbook §7 (Carlsbad template structure) |
| 7 | Quality-gate runner | inline | Per playbook §8 (red-flag SQL etc.) |
| 8 | Per-jurisdiction agent prompt template | optional `templates/codify_agent.md` | For parallel dispatch when N jurisdictions queued |
| 9 | Pipeline phase entry | edit to `scripts/run_state_pipeline.py` | New phase `codify_pending_jurisdictions` between operator_llm_extract and bep_refire — see Cascade Track 2 |

### Sub-tasks (sequenced)

1. **Skeleton** — copy `extract_temporal_from_policy_source.py` structure; CLI args (`--jurisdiction`, `--state`, `--state CA` bulk, `--from-csv`, `--dry-run`, `--refresh`); empty stages
2. **Step 1 — Scope check** — DB query for existing ps + agency presence
3. **Step 2 — Platform discovery** — implement the validity gauntlet; cache per-jurisdiction platform decisions
4. **Step 3 — Chapter navigation** — TOC scrape with regex; LLM fallback for unusual TOCs (Sonnet picks the most-likely animal/beach chapter)
5. **Step 4 — Verbatim fetch** — Playwright with the per-platform selector hints from the field guide
6. **Step 5 — Beach mapping** — spatial PIP queries; sub-area encoding via `region_name` per Phase I1
7. **Step 6 — Rule decision** — LLM judgment with HUMAN REVIEW gate for low-confidence cases (`--auto-commit-confidence 0.85` threshold)
8. **Step 7 — Migration emit** — template fill + write to `supabase/migrations/<YYYYMMDD>_<wave>_<jurisdiction>_backfill.sql`
9. **Step 8 — Quality gates** — run the 8a-8f SQL; halt on failures; print remediation
10. **Step 9 — Commit + apply** — git add/commit; psql apply; verify INSERT counts; print summary
11. **Step 9.5 — Temporal follow-up** — invoke `extract_temporal_from_policy_source.py --ps-ids <new>` for any new ps rows
12. **Step 9.6 — Handoff snippet** — emit a one-paragraph addition to the session handoff pin

### Open design questions

1. **Per-jurisdiction LLM cost** — estimate $0.10-0.30 per jurisdiction with platform discovery + rule decision; OK?
2. **Human review threshold** — auto-commit at confidence ≥0.85 vs always-pause-for-review? Recommend the former with explicit `--dry-run` for the first 5 runs per state
3. **Agent dispatch wrapper** — when N jurisdictions queued, spawn 1 sub-agent per cluster of 3-5? Or single-process iterate?
4. **Failure modes** — Cloudflare 403 on first fetch (already covered by curl + ssl-no-revoke + truststore patterns); ambiguous rule (defer + flag); no published code (operator_posted_policy fallback)
5. **Per-state launch sequence** — `--state CA --pilot 10` for first try, then `--state CA --all` (skip already-covered jurisdictions)

### Effort estimate
- Skeleton + steps 1-5: 2 hours
- Steps 6-9 (LLM + migration + apply + audit): 2-3 hours
- Step 9.5 + 9.6: 30 min
- Validation against 5 known CA jurisdictions (Goleta, Carpinteria, Carlsbad, etc.): 1 hour
- **Total: 5-7 hours**

---

## Track 2 — Cascade integration

### Goal

`run_state_pipeline.py --state X` includes Phase B/C/E/H/I as canonical
phases, not just standalone triggers. Adds verification + halts on
regression.

### Anchors

- `docs/wave3_pipeline_integration_design.md` — the 7-phase plan (A-G)
- `docs/consensus_engine_current_state.md` — current state map (the 2026-05-17 addendum)
- `project_pipeline_instantiation.md` (memory pin) — living map of the pipeline
- `scripts/run_state_pipeline.py` — PHASES list to extend

### Deliverables

| # | Phase to add/update | Where in `PHASES` list | What it does |
|---:|---|---|---|
| 1 | `policy_source_coverage_check` | NEW; between `precheck` and `arena_*` | Halts if ≥X% of in-scope jurisdictions have ps rows; surfaces gaps |
| 2 | `extract_temporal` | NEW; after `operator_llm_extract`, before `bep_refire` | Runs `scripts/extract_temporal_from_policy_source.py --state X` for any new ps rows |
| 3 | `consensus_function_integrity_check` | NEW; before `promote` | Asserts `_canonical_dogs_from_policy_sources` has the 10-col signature (H4) and `_zr_inject_from_policy_sources` is the entity-aware version |
| 4 | `operator_llm_extract` | UPDATE existing | Already has Phase E gate from agent's commit `095d6aa` — verify it's right; add log lines showing skip count |
| 5 | `governance_attribution_check` | UPDATE existing | Decision: keep policy_source-blind OR extend? Cleanest answer is keep blind — beach_agency is migration-driven; the gap is OK. Defer the rewrite to a separate session. |
| 6 | `temporal_subsystem_health` | NEW; at end | Runs `tmp/phase_h6_sanity_tests.sql` as a smoke test; fails state launch if any test fails |
| 7 | Per-state cost-savings telemetry | UPDATE | Log Phase E skip-count + temporal-extractor cost + LLM-extractor cost; surface in run summary |

### Sub-tasks (sequenced)

1. **Phase E verification** — read agent's commit `095d6aa`; confirm the predicate works; add explicit log of skip count
2. **`extract_temporal` phase wiring** — argparse the script into a phase action; pass `--state $STATE`
3. **`policy_source_coverage_check` gate** — define "X%": probably "every CA city with ≥1 beach has ≥1 ps OR is explicitly deferred"; build SQL view
4. **`consensus_function_integrity_check`** — assert function signatures + presence of the entity-aware injector; trip on missing
5. **`temporal_subsystem_health`** — execute the H6 sanity SQL as a phase
6. **Run on CA end-to-end** — `run_state_pipeline.py --state CA --plan` to see the new phase ordering; then `--state CA` for real
7. **Update [[pipeline-instantiation]] pin** with the new phase list

### Open design questions

1. **Coverage threshold for `policy_source_coverage_check`** — gate at 80%? 95%? Or "no known city without an explicit defer pin"?
2. **`temporal_subsystem_health` failure mode** — halt state launch OR warn + continue?
3. **Should `extract_temporal` run incrementally** (only new ps since last run) or always (refresh)? Incremental is faster; refresh catches Sonnet improvements
4. **Phase ordering** — where exactly does `codify_pending_jurisdictions` (from Track 1) slot? Probably between `state_policy_seed` and `arena_seed` (codify the rules before ingesting beaches that depend on them)
5. **Per-state Phase E savings telemetry** — surface to where? Just log? Or pin to external_source_status?

### Effort estimate
- Phase additions (1-3): 1.5 hours
- Verification + telemetry: 1 hour
- End-to-end CA test: 1 hour
- [[pipeline-instantiation]] refresh: 30 min
- **Total: 4-5 hours**

---

## Sequencing both tracks

Track 1 (Codify v1) and Track 2 (Cascade integration) are largely
parallelizable:

- Track 1 = new file `scripts/derive_policy_source_for_jurisdiction.py`
- Track 2 = edits to existing `scripts/run_state_pipeline.py`
- Overlap: Track 2's new `codify_pending_jurisdictions` phase calls Track 1's script. Build Track 1 first; Track 2's phase wiring depends on Track 1's CLI being stable.

**Recommended order:**
1. Track 1 skeleton + steps 1-5 (~2 hr) → produces a callable script even if rule-decision is just `--dry-run`
2. Track 2 phases 1-3 (~2 hr) IN PARALLEL with Track 1 finishing
3. Track 1 steps 6-9 (~2 hr) → callable end-to-end
4. Track 2's `codify_pending_jurisdictions` wiring (~30 min) — depends on Track 1 being callable
5. Track 1 validation (~1 hr) + Track 2 CA end-to-end test (~1 hr)

**Total wall time with 1 agent + me: 6-8 hours.**
**Total wall time with 3 parallel agents + me orchestrating: 3-4 hours.**

---

## Dispatchable agent prompts

When the next session opens, two ready-to-fire agent prompts:

**Agent A (Track 1 skeleton):**
```
Build scripts/derive_policy_source_for_jurisdiction.py per
docs/codify_cascade_v1_runbook.md §"Track 1 — Codify v1" sub-tasks
1-5. Mirror scripts/extract_temporal_from_policy_source.py for shape
(truststore + dotenv + supa() + argparse). Implement scope check +
platform discovery + chapter navigation + verbatim fetch + beach
mapping. STOP at step 5 (no rule decision / no migration emit /
no apply). Commit. Report skeleton readiness to the orchestrator.
```

**Agent B (Track 2 phases 1-3):**
```
Add 3 new phases to scripts/run_state_pipeline.py PHASES list per
docs/codify_cascade_v1_runbook.md §"Track 2 — Cascade integration"
deliverables 1-3: policy_source_coverage_check, extract_temporal,
consensus_function_integrity_check. Use the existing phase shape (key,
action, criterion, criterion_text). Insert at the correct PHASES
positions per the spec. Add log lines showing per-phase telemetry.
Commit. Report phase count delta + new criterion-text strings.
```

---

## Related

- [[codify-cascade-vocabulary]] — the naming
- [[session-handoff-2026-05-17-evening]] — where today's work landed
- [[two-pass-refire-pattern]] — gotcha to avoid in any future cascade-side migration
- [[afk-autonomy]] — license to ship without sign-off if Franz authorizes again
- `docs/jurisdiction_policy_source_playbook.md` — the algorithm Track 1 implements
- `docs/wave3_pipeline_integration_design.md` — the broader integration plan Track 2 satisfies
