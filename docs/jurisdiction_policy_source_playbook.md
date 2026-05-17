# Jurisdiction → policy_source playbook

Algorithm to derive codified dog-policy authority for one jurisdiction
and produce a committed migration. Produces five deliverables — the
recipe + Python driver + sub-agent prompt + migration template + quality
gates — from one document. Drives `scripts/derive_policy_source_for_jurisdiction.py`
(planned) and the manual / agent-dispatched work that backfilled CA in
Wave 1–5.

---

## Read first

- **[[url-resolution-field-guide]]** — per-platform URL anatomy; the lookup table for step 2.
- **[[page-level-over-agency-level]]** — tenet 1; the URL discipline you'll enforce in step 8.
- **`docs/wave3_pipeline_integration_design.md` §"URL-quality audit"** — the red-flag SQL you'll run in step 9.

---

## Inputs you need before starting

```
jurisdiction      = (name: str, type: 'city'|'county'|'state_department'|
                                       'federal_department'|'federal_military'|
                                       'tribal'|'special_district',
                     state: 'CA'|'OR'|'WA'|...)
agency_id         = SELECT id FROM agency WHERE name=:name AND type=:type
                    (create canonical bare-name row if missing; see tenet 4)
beaches_gold      = candidate fids whose geom is inside jurisdiction polygon
psql              = postgresql://postgres.ehlzbwtrsxaaukurekau@aws-1-us-east-1.pooler.supabase.com:5432/postgres
                    PGPASSWORD in scripts/pipeline/.env
fetcher           = scripts/fetch/fetch_html.py (Playwright; --wait, --html, --selector)
template          = supabase/migrations/20260516_wave3_carlsbad_city_backfill.sql
```

---

## The algorithm (per jurisdiction)

### 1. Scope check

```
if jurisdiction is operator (HOA / concessionaire / private resort / land trust):
    if beach is fully private:        DEFER (no bps row); record in header
    else:                              attach as `operator_posted_policy` row
                                       under parent gov agency
elif EXISTS (ps WHERE issuing_agency_id = agency_id):
    SUPPLEMENT existing rows (don't dup)
else:
    PROCEED
```

### 2. Discover platform

Try platforms in order until validity check passes:

| Type | Try (in order) |
|---|---|
| city / county | Municode (4 slug variants) → amlegal → qcode → ecode360 → codepublishing → county.codes → encodeplus → city-hosted PDF |
| state agency | per-park / per-unit URL (CA DPR `?page_id=N` via `?page_id=21805`; OPRD `oregon.public.law/rules/oar_<chap>-<div>-<sec>`; WA `app.leg.wa.gov/wac/?cite=`) |
| federal | NPS `nps.gov/<4letter>/planyourvisit/pets.htm` + Compendium PDF; USFS `fs.usda.gov/<forest>/`; BLM Federal Register supplementary rules; USFWS `fws.gov/refuge/<name>/visit/rules-and-policies`; 36 CFR §2.15 or 43 CFR §8365.1-6 as baseline |
| tribal | per-tribe (often codepublishing); may need CPRA request |
| special district | district website; no standard platform |

**Validity check** (ALL four must hold):
1. HTTP 200 + non-empty body
2. Body contains jurisdiction NAME (case-insensitive)
3. Body contains state indicator (`California` / `, CA` / etc.)
4. For counties: body contains literal `"<County> County"`

### 3. Navigate to operative chapter

Probe nodeIds via TOC scrape:

```bash
python scripts/fetch/fetch_html.py "<root_url>" --html --wait 14 \
  | grep -oE 'nodeId=[A-Za-z0-9_.\-]+' | sort -u \
  | grep -iE 'AN|BE|PA'
```

Likely chapter patterns (cities/counties):
- `TIT<N>AN`, `TIT<N>ANRE`, `TIT<N>ANCO`, `CH<N>AN`, `CH<N>ANRE` — Animals chapters
- `TIT<N>SAHE_CH<M>AN`, `TIT<N>HESA_DIV<M>ANSE` — animals nested under health/sanitation
- `TIT<N>PABE`, `TIT<N>BE` — Parks and Beaches chapters where the operative rule sometimes lives

### 4. Fetch verbatim text

```bash
python scripts/fetch/fetch_html.py "<deep_link_url>" --wait 14 --selector ".codes-chunks-pg"
```

Per-platform selector hints: Municode `.codes-chunks-pg` · ecode360 `#contentArticles` · codepublishing body · amlegal `.section-content` · county.codes body.

Capture: **full operative §**, **scope/definitions §** (if they alter reach), **exceptions** (service-dog, designated areas, seasonal windows), **penalty + ordinance number** (proves operativeness), **adoption date**.

Cloudflare? Curl fallback: `curl -sL --ssl-no-revoke -A "Mozilla/5.0..." <url>`.

### 5. Map beaches via spatial containment

```sql
-- City polygon (canonical for city jurisdiction)
SELECT g.fid, g.name FROM beaches_gold g
JOIN jurisdictions j ON ST_Contains(j.geom, g.geom)
WHERE g.state = :state AND g.is_active AND j.name = :jurisdiction_name;

-- Federal/state via CPAD/PAD-US
SELECT g.fid FROM beaches_gold g WHERE g.cpad_unit_id IN (SELECT unit_id FROM cpad_units WHERE ...);

-- Operator (Conservancy, Tribal): enumerate by name match + coord cluster
```

**Sub-area carve-outs** (Del Mar seasonal, LB §6.16.310 dog-exercise-area, Coronado Sand Pebble): encode via `beach_policy_source.region_name` column. ONE bps row per zone; `region_name` is the human-friendly zone label (e.g., 'Sand Pebble Off-Leash Area', 'North Beach (north of 29th Street)'). NULL region_name = default region. Multiple rows with same section but distinct region_name → injector emits multi-region zone_rules.regions[] automatically.

Legacy pattern (`section='sand_<sub_area>_overlay'`) is deprecated; use region_name instead.

### 6. Decide rule per beach

| Code text | rule |
|---|---|
| "on a leash no longer than X feet" | `on_leash` |
| "no dogs" / "prohibited" / "shall not bring" | `not_allowed` |
| "off leash under voice control" | `off_leash_voice_control` |
| "off leash" (no voice-control clause) | `off_leash` |
| Time-of-day / seasonal carve-out | base rule + `status_note` documenting the carve-out; sub-area `section` if major |

**Layered authority** (DPR over city; BLM over state; Conservancy + county): write TWO bps rows with distinct `section` values + appropriate `operative_status`. Pattern: primary `'sand'` + supplement `'sand_<authority>_overlay'`.

**Temporal carve-outs** (Del Mar §4.08.020(B)(1) Labor-Day-to-Jun-15 seasonal, Rosie's 6am-8pm posted hours, Pismo plover Mar-Sep): DO NOT cram into status_note prose. Instead, write the bps row with the BASE rule, then run `scripts/extract_temporal_from_policy_source.py --ps-ids <new_id>` as step §9.5 post-apply. The extractor lifts temporal patterns into structured `beach_policy_source_temporal` rows via Sonnet. Trigger cascade picks them up automatically.

**Defer rather than fabricate** (see defer rubric below).

### 7. Write migration

File: `supabase/migrations/YYYYMMDD_<wave_or_context>_<jurisdiction_or_batch>_backfill.sql`

```sql
-- Header: cite source URL + verbatim text quote, per-beach decisions, deferrals.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT '<subtype>',
       '<Canonical citation: "X Municipal Code §Y.Z (Title)">',
       (SELECT id FROM public.agency WHERE name='<Agency>' AND type='<type>'),
       ARRAY['dog_policy']::text[],
       '<deep-linked URL>',
       '<verbatim quote + ordinance numbers + "[Hosted on X. Fetched via Playwright YYYY-MM-DD.]">'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE '<canonical prefix>%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', '<rule>', 'operative'::operative_status,
       '<per-beach operative quote>',
       ps.source_url,
       '<edge cases, sub-area carve-outs, walkthrough recommendations>',
       NULL  -- or 'Named sub-area' for multi-zone beaches
FROM (values (<fid1>),(<fid2>),...) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE '<canonical prefix>%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
```

Valid `subtype`:
```
state_statute, federal_regulation, state_regulation, municipal_code,
special_district_ordinance, tribal_resolution,
mou, lease_agreement, operating_agreement, concession_lease,
agency_administrative_policy, superintendents_compendium,
operator_posted_policy, withdrawn_rulemaking,
community_attestation, promotional_listing, inferred
```
Verify: `SELECT unnest(enum_range(NULL::policy_source_subtype));`

Valid `operative_status`:
```
operative, non_enforced, superseded_by_lower_tier, inactive
```

### 8. Quality gates (run BEFORE commit)

```sql
-- (8a) Red-flag URL: no source_url shared by ≥2 places
SELECT source_url, count(*) AS n
FROM public.policy_source
WHERE source_url IS NOT NULL
GROUP BY 1 HAVING count(*) >= 2 ORDER BY 2 DESC;
-- Expect: zero rows. Any hit = fix before commit.

-- (8b) Citation-vs-URL coherence: §/Chapter/Title in citation → deep link in URL
SELECT id, substring(citation,1,55), source_url
FROM public.policy_source
WHERE source_url IS NOT NULL
  AND (citation ~ 'Title |Chapter |Section |§' OR citation ILIKE '%code%')
  AND source_url !~ '(nodeId=|#|section/|cite=|page_id=|sectionNum=|/[0-9]{6,}|county\.codes/Code/|codepublishing.*\.html|public\.law/(rules|statutes)/|elaws\.us/code/coor_)'
  AND source_url !~ '\.pdf$';
-- Investigate every row. Many are false positives (regex tightening pending).

-- (8c) Agency lookup returns exactly one row
SELECT count(*) FROM public.agency WHERE name=:name AND type=:type;
-- Expect: 1. Multiple = file dedup; use bare-name canonical.

-- (8d) Homogeneity sanity (per batch)
-- For each new ps row, beaches_linked count should be proportional to the
-- text's actual coverage. 100% of state via one ps = OR homogeneity bug.

-- (8e) Temporal coverage: ps rows with time/season qualifiers in full_text
-- should have ≥1 beach_policy_source_temporal row.
SELECT ps.id, substring(ps.citation, 1, 60)
FROM public.policy_source ps
WHERE ps.full_text ~* '(dawn|dusk|am|pm|hours?|labor day|memorial day|seasonal|jun|jul|aug|sep)'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source_temporal t
    JOIN public.beach_policy_source bps USING (beach_fid, policy_source_id, section)
    WHERE bps.policy_source_id = ps.id
  );
-- For each row, run: python scripts/extract_temporal_from_policy_source.py --ps-ids <id>

-- (8f) region_name presence for known multi-zone beaches.
-- Heuristic: beach with both off_leash and on_leash bps rows should usually
-- have ≥2 distinct region_name values (or NULL + 1 named).
SELECT bps.beach_fid, g.name,
       count(DISTINCT bps.region_name) AS n_distinct_regions,
       bool_or(rule LIKE 'off_leash%') AS any_off_leash,
       bool_or(rule = 'on_leash') AS any_on_leash
FROM public.beach_policy_source bps
JOIN public.beaches_gold g ON g.fid = bps.beach_fid
GROUP BY bps.beach_fid, g.name
HAVING bool_or(rule LIKE 'off_leash%') AND bool_or(rule = 'on_leash')
   AND count(DISTINCT bps.region_name) < 2;
-- Investigate each; consider adding region_name to disambiguate zones.
```

### 9. Commit, apply, audit, handoff

```bash
git add supabase/migrations/<file>.sql && git commit -m "<wave>: <jurisdiction(s)>"
# Apply (requires user go):
PGPASSWORD='...' PGCLIENTENCODING=UTF8 psql "<pooler_url>" -v ON_ERROR_STOP=1 -f <file>

# §9.5 — Extract temporal patterns from NEW ps rows
python scripts/extract_temporal_from_policy_source.py --ps-ids <comma_list_of_new_ids>
# (Sonnet ~$0.005-0.01 per ps; only ps with temporal text in full_text produce rows.)

# Re-run (8a)/(8b)/(8e)/(8f); verify INSERT counts; update session-handoff pin.
```

---

## Defer rubric

| Precondition | Action | Why |
|---|---|---|
| HOA-private (fully private beach) | NO bps row; document operator name in header | bounded-operator ([[operator-not-pseudo-agency]]) |
| AV-flagged source / unsafe URL | NO fetch; defer entirely; pin the gap | safety ([[deferred-canyon-lake]]) |
| Source not online (no Municode / no published code) | `operator_posted_policy` row pointing at city info page IF available, ELSE defer | partial-record > no-record but not fabricated |
| Ambiguous operative authority (multiple plausible issuers) | defer; flag for [[governance-resolver-followups]] | accuracy |
| Source contradicts itself or other authorities | Write the strongest-authority row; document the conflict in `status_note` | layered authority |
| Beach not actually a beach (theme park / lake club) | defer; flag for `beaches_gold` re-classification | data quality |
| Codified text says one thing; recent ordinance amends silently | use the latest amendment; document in `status_note` | freshness |

---

## Tenets (the hard rules)

### 1. Page-level > agency-level
Every `source_url` and `evidence_url` deep-links to the section or chapter, NOT the agency root or catalog.
**Detect:** red-flag SQL (8a). Any URL shared by ≥2 distinct places = fix before commit.
**Reason:** consumer surfaces will eventually deep-link from "this beach's rule" → the codified source; a catalog URL silently breaks that contract.
**Exception:** a single legitimate catchall baseline row may use a catalog URL; per-place rows that override it must use deep links.

### 2. Defer > fabricate
When the source is missing, ambiguous, or contradicts itself, write `status_note` and skip the bps row. Don't guess.
**Detect:** rule classification in step 6; if the text doesn't dictate the rule, you're guessing.
**Reason:** a missing row is recoverable; a wrong row pollutes the consensus engine and drives downstream description / scoring errors.

### 3. No manual `beach_agency` edits
The `beach_agency` table is migration-owned. Do NOT INSERT manual rows when the spatial resolver mis-attributes; instead, flag in [[governance-resolver-followups-2026-05-17]] for the Phase B integration to handle structurally.
**Detect:** if you find yourself wanting to `INSERT INTO beach_agency` to fix attribution, stop.
**Reason:** beach_agency is consumed by `governance_attribution_check`; manual edits diverge from the resolver's source of truth.
**Exception:** the transient backfill from 2026-05-17 was an exception authorized by Franz; subsequent migrations should NOT replicate that pattern.

### 4. Canonical bare-name agency
Agency lookups use the bare name (`"Goleta"`, type=`city`), not the trailing-comma variant (`"Goleta, City of"`). The 31 dupes were removed 2026-05-17 but always verify with `\d agency` lookup before inserting.
**Detect:** quality gate (8c).
**Reason:** historical schema artifact; bare-name is canonical going forward.

### 5. Trust the trigger cascade
Inserting a bps row fires `tg_stmt_ins_beach_policy_source` which calls `promote_entity_dogs_to_beach_dog_policy` which updates `beach_dog_policy` flat columns AND fires `tg_after_change_dogs_refire_zone_rules` which rebuilds `zone_rules`. ONE INSERT covers both flat columns AND zone_rules.

**Detect:** if you find yourself manually calling `_promote_zone_rules_for_fid(fid)` or `promote_entity_dogs_to_beach_dog_policy(fid)` after a bps INSERT, stop. The trigger already did it.

**Reason:** consistency. Manual fires bypass the trigger fan-out and risk leaving downstream tables (scoring_tier refresh) stale.

**Exception:** global re-fire migrations that change the function bodies themselves (H3/H4/I3/I4) need explicit two-pass refire per [[two-pass-refire-pattern]].

---

## Agent prompt template

When dispatching sub-agents to parallelize this work, slot these placeholders:

```
[Goal] Backfill <N> <type> jurisdictions in <region>. Write ONE migration; do NOT apply.

[Beaches]
| fid | beach | jurisdiction | agency_id |
| ... | ... | ... | ... |

[Per-jurisdiction notes]
<HOA defers, layered authority, alternate platform, sub-area carve-outs>

[Read first]
1. docs/jurisdiction_policy_source_playbook.md  ← this doc (the algorithm)
2. memory: feedback_page_level_over_agency_level (tenet 1)
3. memory: feedback_url_resolution_field_guide (per-platform anatomy)
4. memory: project_session_handoff_<latest> (current state)
5. Template: supabase/migrations/20260516_wave3_carlsbad_city_backfill.sql

[Tools]
- python scripts/fetch/fetch_html.py (Playwright)
- psql via PGPASSWORD env (read-only acceptable; writes deferred to user)
- WebSearch / WebFetch for unknown platforms

[Deliverable]
ONE file: supabase/migrations/YYYYMMDD_<context>_<batch>.sql

[Critical]
- Deep-link every URL (tenet 1).
- ON CONFLICT DO NOTHING on bps; NOT EXISTS gate on ps.
- Look up agency_id; don't guess; bare-name canonical.
- Defer rather than fabricate (defer rubric).
- Distinct `section` for layered authority; `region_name` for multi-zone carve-outs (e.g., off-leash strip inside on-leash beach).
- Temporal data goes to beach_policy_source_temporal via H2 extractor — write base bps row first, then run extractor as step §9.5.

[Process]
1. Read first.
2. Per jurisdiction: discover platform → fetch verbatim → map beaches → write migration section.
3. Apply playbook §8 quality gates before commit.
4. git add + commit; DO NOT push, DO NOT apply.

[Output]
<300 words: per-jurisdiction URL + rule + judgment calls + deferrals + governance-resolver flags + commit SHA.
```

**Cross-agent coordination:** mark agent-owned TaskUpdate `owner` slots; brief each agent on what others are doing (prevent fid overlap); never dispatch two agents to the same migration file.

---

## Where the algorithm runs

Three options; pick after the Python driver stabilizes:

- **Pre-pipeline batch** (recommended): per-state expansion — run `derive_policy_source_for_jurisdiction.py --state OR` BEFORE `run_state_pipeline.py --state OR`. Builds ps coverage; pipeline consumes via integration Phases B / C / E ([[handoff-consensus-source-authority]]).
- **New pipeline phase** `ensure_policy_source_coverage` between `populate_governance_from_polygon_gold` and `gold_evidence_resolve_run`. Halts when in-scope jurisdictions are missing ps.
- **Launch-readiness ramp** tied to [[state-expansion-playbook]] — one-shot per state.

---

## Worked example (Goleta, 2026-05-17)

Walks the algorithm end-to-end:

| Step | What happened |
|---|---|
| 1 scope | City of Goleta (id 51, type city), 2 scoreable beaches (8946, 8947). Not already covered. PROCEED. |
| 2 discover | Municode `code_of_ordinances` returned "Codification Search" = 404. qcode `qcode.us/codes/goleta/` validity check passed. Platform: qcode. |
| 3 navigate | Title 6 Animals at `topic=6`; §6.01.020 is the operative section. |
| 4 fetch | §6.01.020 adopts SB County Code Chapter 7 by reference per Ord. 19-05, frozen 2019-02-05. |
| 5 map | jurisdictions PIP → fids 8946 Ellwood + 8947 Haskell's. |
| 6 rule | Both → `on_leash` (substantive rule is SB County's; ps id 93 has verbatim). |
| 7 migration | `20260517_bucket_b_goleta_backfill.sql`: 1 ps INSERT + 2 bps INSERTs. |
| 8 quality gates | URL deep-linked to `?topic=6-01-020`; no URL reuse; citation references §6.01.020 + URL targets it. |
| 9 commit + apply | commit `1036cfa`; applied; INSERT 0 1 + INSERT 0 2; handoff pin updated. |

---

## Worked example (Del Mar Dog Beach — multi-zone + temporal, 2026-05-17)

| Step | What happens |
|---|---|
| 1 scope | Del Mar Dog Beach fid 8560, City of Del Mar (id 48), single-zone off-leash strip — but actually has sub-area + temporal carve-outs. PROCEED. |
| 2 discover | Municode `municipal_code` slug (NOT `code_of_ordinances`). |
| 3 navigate | Title 4 Ch 4.08 §4.08.020 Dogs and Animals. |
| 4 fetch | §4.08.020(B)(1): off-leash dawn-8am year-round, AND day-after-Labor-Day through Jun 15. §4.08.020(C): summer prohibition Powerhouse-Park-to-29th-St Jun 15 – Labor Day. |
| 5 map | fid 8560 (North Beach off-leash strip) + fid 8992 (Main beach with summer prohibition). |
| 6 rule | 8560: base rule `off_leash_voice_control`, region_name='North Beach (north of 29th Street)'; 8992: base rule `on_leash` (summer carve-out captured by temporal extractor). |
| 7 migration | One ps row + 2 bps rows with region_name set. |
| 8 quality | Page-level URL deep-linked to nodeId; no URL reuse. |
| 9 commit + apply | Migration applies, trigger cascades. |
| 9.5 temporal | `python scripts/extract_temporal_from_policy_source.py --ps-ids 199` lifts 4 windows (seasonal off-leash, daily dawn-8am, compound, summer prohibition) into beach_policy_source_temporal. Trigger auto-refires the entity promoter; bdp gets dogs_prohibited_start/end populated; zone_rules.sections.sand gets time_windows[]. |

---

## Related

- [[url-resolution-field-guide]] · [[page-level-over-agency-level]] · [[operator-not-pseudo-agency]] · [[law-as-primary-source-ca]] · [[consensus-source-authority]] · [[handoff-consensus-source-authority]]
- 4 walkthroughs at `dog-beach-claude/docs/walkthrough_*.md`
- `docs/wave3_pipeline_integration_design.md` (downstream consumption)
- Carlsbad template at `supabase/migrations/20260516_wave3_carlsbad_city_backfill.sql`

---

## Revision log

- **2026-05-17 v3** — Absorbed evening-session producer-side additions: `region_name` column for sub-area carve-outs (replaces deprecated `section='sand_<sub_area>_overlay'` hack); `beach_policy_source_temporal` table + `scripts/extract_temporal_from_policy_source.py` Sonnet extractor wired as new step §9.5; trigger cascade (`tg_stmt_ins_beach_policy_source`) auto-fires entity-promote + zone_rules rebuild on bps INSERT/UPDATE/DELETE; quality gates (8e) temporal coverage + (8f) region_name multi-zone heuristic; tenet 5 "Trust the trigger cascade"; Del Mar worked example demonstrating multi-zone + temporal end-to-end. Authors: Franz + Claude.
- **2026-05-17 v2** — Restructured for executable shape (algorithm-first, all values inline, tenets at end, agent template as parameterized recipe). v1 was a 15-section reference manual; v2 is what you'd hand a code path or sub-agent. Authors: Franz + Claude.
