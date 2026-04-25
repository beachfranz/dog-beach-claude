# Policy Research Migration

Spec for retiring `beaches_staging_new` and the OLD v2-* dog-policy edge functions, replacing them with a NEW pipeline-native architecture: a focused `policy_research_extractions` table populated by rewritten edge functions that read from `locations_stage` and write evidence-shaped data.

**Status:** Spec only — not yet implemented. Drafted 2026-04-25 after the three-pipeline audit (see memory `project_three_pipelines_audit.md`).

**Scope:** This migration handles `beaches_staging_new` (1,470 rows, the bridge table `populate_from_research` currently reads). It does NOT handle `beaches_staging` (955 rows, the Phase 1/2 Python orphan) — that table stays orphaned per the audit's recommendation; only ~93 of 955 rows had policy data and ~71 of those are the only methodologically-distinct contribution. Decommission Phase 1/2 Python + `beaches_staging` separately later.

---

## 1. Architecture

```
BEFORE (today):
  Web sources + Claude
       ↓
  v2-* dog-policy edge functions (writes to beaches_staging_new)
       ↓
  beaches_staging_new (1,470 rows; flat policy columns)
       ↓
  populate_from_research (reads beaches_staging_new)
       ↓
  beach_enrichment_provenance (source='research' evidence rows)
       ↓
  resolvers + promoters → locations_stage

AFTER (this migration):
  Web sources + Claude
       ↓
  v2-* dog-policy edge functions, REWRITTEN (read locations_stage,
                                              write policy_research_extractions)
       ↓
  policy_research_extractions (NEW; same shape as park_url_extractions)
       ↓
  populate_from_research, REPLACED (reads policy_research_extractions)
       ↓
  beach_enrichment_provenance (source='old_school_llm' or 'research')
       ↓
  resolvers + promoters → locations_stage

DROPPED in final phase:
  beaches_staging_new (table)
  v2-* OLD edge functions (no longer needed)
  match_beaches_csp_*, match_beaches_nps_*,
  v2_find_dedup_pairs, v2_find_neighbor_inheritance,
  ingest_beaches_batch_with_state_filter (utility functions)
```

Net effect: `beaches_staging_new` becomes obsolete. NEW pipeline owns the entire policy-research flow. Same architectural pattern as `park_url_extractions` + `populate_from_park_url` + `beach_enrichment_provenance`.

---

## 2. New table: `policy_research_extractions`

```sql
create table public.policy_research_extractions (
  id              bigserial primary key,
  fid             integer not null references public.locations_stage(fid) on delete cascade,
  extracted_at    timestamptz not null default now(),

  -- Outcome
  extraction_status text not null check (extraction_status in
    ('success','no_sources','llm_failed','low_confidence','imported_legacy')),

  -- Origin tag — distinguishes data lineage. Currently:
  --   'v2_dog_policy_old'  — imported from beaches_staging_new (legacy v2-* output)
  --   'v2_dog_policy_v2'   — output from the rewritten v2-* edge functions
  --   'manual'             — admin-entered
  -- Each origin maps to a different `source` value when populate_from_research
  -- emits evidence rows: 'old_school_llm', 'research', or 'manual'.
  origin text not null check (origin in
    ('v2_dog_policy_old','v2_dog_policy_v2','manual')),

  -- Research-specific provenance
  research_query   text,                -- search query used (Tavily, etc.)
  source_urls      text[],              -- all URLs consulted
  primary_source_url text,              -- the single most-weighted URL
  source_count     int,                 -- how many distinct sources contributed
  raw_inputs       jsonb,               -- {url: cleaned_text} for replay/audit
  extraction_model text,
  extraction_notes text,
  extraction_confidence numeric(3,2),

  -- Extracted fields (NEW pipeline column names + jsonb shapes)
  dogs_allowed          text check (dogs_allowed is null or dogs_allowed in
    ('yes','no','seasonal','restricted','unknown')),
  dogs_leash_required   text check (dogs_leash_required is null or dogs_leash_required in
    ('required','off_leash_ok','mixed','unknown')),
  dogs_restricted_hours jsonb,  -- [{"start":"HH:MM","end":"HH:MM"}]
  dogs_seasonal_rules   jsonb,  -- [{"from":"MM-DD","to":"MM-DD","notes":"..."}]
  dogs_zone_description text,
  dogs_policy_notes     text,

  hours_text         text,
  open_time          time,
  close_time         time,
  has_parking        boolean,
  parking_type       text check (parking_type is null or parking_type in
    ('lot','street','metered','mixed','none')),
  parking_notes      text,
  has_restrooms      boolean,
  has_showers        boolean,
  has_drinking_water boolean,
  has_lifeguards     boolean,
  has_disabled_access boolean,
  has_food           boolean,
  has_fire_pits      boolean,
  has_picnic_area    boolean,

  -- Idempotency: one row per (fid, primary_source_url, origin)
  unique (fid, primary_source_url, origin)
);

create index pre_fid_idx          on public.policy_research_extractions(fid);
create index pre_status_idx       on public.policy_research_extractions(extraction_status);
create index pre_origin_idx       on public.policy_research_extractions(origin);
create index pre_extracted_at_idx on public.policy_research_extractions(extracted_at desc);

comment on table public.policy_research_extractions is
  'NEW pipeline staging for LLM-extracted dog/practical policy data. Mirrors park_url_extractions shape so populate_from_research can use the same evidence-emission pattern as populate_from_park_url. Holds output from v2-* dog-policy edge functions (after rewrite). Backfilled from beaches_staging_new with origin=v2_dog_policy_old at migration time.';
```

---

## 3. Backfill migration (one-time, no LLM cost)

Copies 953 rows of policy data from `beaches_staging_new` into `policy_research_extractions` with `origin='v2_dog_policy_old'`. Existing canonical resolutions are preserved because the resolver re-picks from the same evidence values.

```sql
insert into public.policy_research_extractions (
  fid, extracted_at, extraction_status, origin,
  primary_source_url, source_urls, source_count,
  extraction_confidence, extraction_notes,
  dogs_allowed, dogs_leash_required,
  dogs_restricted_hours, dogs_seasonal_rules, dogs_zone_description,
  dogs_policy_notes, hours_text,
  has_parking, parking_type, parking_notes,
  has_restrooms, has_showers, has_drinking_water, has_lifeguards,
  has_disabled_access, has_food, has_fire_pits, has_picnic_area
)
select
  bsn.src_fid,
  coalesce(bsn.dogs_policy_updated_at, now()),
  case when bsn.dogs_allowed is not null then 'success'::text
       else 'imported_legacy'::text end,
  'v2_dog_policy_old',
  bsn.dogs_policy_source_url,
  case when bsn.dogs_policy_source_url is not null
       then ARRAY[bsn.dogs_policy_source_url]
       else NULL::text[] end,
  case when bsn.dogs_policy_source_url is not null then 1 else 0 end,
  public.bsn_confidence_to_numeric(bsn.enrichment_confidence),
  'imported from beaches_staging_new on ' || now()::date,
  bsn.dogs_allowed,
  public.bsn_leash_to_enum(bsn.dogs_leash_required),
  bsn.dogs_daily_windows,
  -- Re-shape OLD seasonal_closures {start,end,reason} → NEW {from,to,notes}
  case
    when bsn.dogs_seasonal_closures is null
      or jsonb_typeof(bsn.dogs_seasonal_closures) <> 'array'
      or jsonb_array_length(bsn.dogs_seasonal_closures) = 0
    then null
    else (
      select jsonb_agg(jsonb_build_object(
        'from',  e->>'start',
        'to',    e->>'end',
        'notes', e->>'reason'
      ))
      from jsonb_array_elements(bsn.dogs_seasonal_closures) e
    )
  end,
  coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area),
  bsn.dogs_policy_notes,
  bsn.hours_text,
  bsn.has_parking, bsn.parking_type, bsn.parking_notes,
  bsn.has_restrooms, bsn.has_showers, bsn.has_drinking_water, bsn.has_lifeguards,
  bsn.has_disabled_access, bsn.has_food, bsn.has_fire_pits, bsn.has_picnic_area
from public.beaches_staging_new bsn
join public.locations_stage s on s.fid = bsn.src_fid
where bsn.src_fid is not null
  and bsn.dogs_allowed is not null
on conflict (fid, primary_source_url, origin) do nothing;
```

---

## 4. CHECK constraint + source_precedence updates

```sql
-- Add 'old_school_llm' to source CHECK
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape','research',
    'csp_parks','park_operators','nps_places','tribal_lands','military_bases',
    'pad_us','sma_code_mappings','jurisdictions','csp_places','name',
    'governing_body','park_url','park_url_buffer_attribution',
    'old_school_llm'   -- NEW: v2-* legacy LLM output
  ]));

-- Update source_precedence to slot old_school_llm just below research
create or replace function public.source_precedence(p_source text)
returns int
language sql immutable
as $$
  select case p_source
    when 'manual'             then 0
    when 'cpad'               then 10
    when 'pad_us'             then 11
    when 'park_operators'     then 12
    when 'plz'                then 15
    when 'nps_places'         then 20
    when 'tribal_lands'       then 21
    when 'csp_parks'          then 22
    when 'sma_code_mappings'  then 23
    when 'ccc'                then 30
    when 'tiger_places'       then 40
    when 'military_bases'     then 50
    when 'state_config'       then 60
    when 'web_scrape'         then 70
    when 'research'           then 71  -- NEW: explicit slot
    when 'old_school_llm'     then 75  -- NEW: legacy LLM output, beaten by fresh research
    when 'llm'                then 80
    else                           99
  end;
$$;
```

---

## 5. New `populate_from_research` (reads `policy_research_extractions`)

```sql
create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.policy_research_extractions
    where extraction_status in ('success', 'imported_legacy')
      and (p_fid is null or fid = p_fid)
  ),
  -- Map origin → source value emitted in evidence rows
  tagged as (
    select *,
      case origin
        when 'v2_dog_policy_old' then 'old_school_llm'
        when 'v2_dog_policy_v2'  then 'research'
        when 'manual'            then 'manual'
      end as evidence_source
    from successful
  ),
  dogs_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from tagged
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  practical_built as (
    select fid, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from tagged
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'practical', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;
```

---

## 6. v2-* edge function rewrite — common template

Each of the 7 dog-policy edge functions becomes a self-contained function that reads from `locations_stage` (filtered by canonical governance source) and writes to `policy_research_extractions`.

```typescript
// v2-{state-parks|federal|county|city|...}-dog-policy.ts (rewrite)

Deno.serve(async (req) => {
  // ... CORS + JSON helpers ...

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // 1. Fetch input from NEW pipeline tables
  //    Filter: beaches whose canonical governance source matches what THIS
  //    function targets. e.g. v2-state-parks-dog-policy targets state-source canonical.
  const { data: rows, error } = await supabase
    .from("locations_stage")
    .select(`
      fid, display_name, latitude, longitude,
      governing_body_name, governing_body_type,
      place_name, county_name, state_code,
      review_status
    `)
    .eq("governing_body_type", "state")  // or 'city', 'county', 'federal' per function
    .in("fid",
      // Filter to beaches whose canonical governance came from a relevant source
      supabase.from("beach_enrichment_provenance")
        .select("fid")
        .eq("field_group", "governance")
        .eq("is_canonical", true)
        .in("source", ["cpad", "csp_parks", "park_url"])
    )
    .eq("review_status", "ready")
    .limit(body.limit ?? DEFAULT_LIMIT);

  // 2. Group by governing_body_name (research the entity once,
  //    apply policy to all its beaches)
  const entitySet = new Set<string>();
  for (const r of rows ?? []) entitySet.add(r.governing_body_name);
  const entities = [...entitySet];

  // 3. For each entity: Tavily search + Claude extraction
  const tasks = entities.map(entity => async () => {
    const query   = `${entity} dogs on beach ${rows[0].state_code}`;
    const sources = await tavilySearch(query);
    const policy  = sources.length === 0
      ? defaultPolicy()
      : await extractPolicy(entity, sources);
    return { entity, sources, policy };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  // 4. Write to policy_research_extractions (one row per beach the entity governs)
  for (const r of researched) {
    const beaches = (rows ?? []).filter(b => b.governing_body_name === r.entity);
    for (const b of beaches) {
      await supabase
        .from("policy_research_extractions")
        .upsert({
          fid:                   b.fid,
          extracted_at:          new Date().toISOString(),
          extraction_status:     r.policy.dogs_allowed ? "success" : "low_confidence",
          origin:                "v2_dog_policy_v2",
          research_query:        `${r.entity} dogs on beach ${b.state_code}`,
          source_urls:           r.sources.map(s => s.url),
          primary_source_url:    r.policy.primary_source_url,
          source_count:          r.sources.length,
          extraction_model:      "claude-haiku-4-5-20251001",
          extraction_confidence: r.policy.confidence,
          dogs_allowed:          r.policy.dogs_allowed,
          dogs_leash_required:   r.policy.dogs_leash_required,
          dogs_restricted_hours: r.policy.dogs_restricted_hours,  // jsonb shape
          dogs_seasonal_rules:   r.policy.dogs_seasonal_rules,    // jsonb shape
          dogs_zone_description: r.policy.dogs_zone_description,
          dogs_policy_notes:     r.policy.dogs_policy_notes,
        }, { onConflict: 'fid,primary_source_url,origin' });
    }
  }

  return json({ entities: entities.length, beaches_touched: rows?.length });
});
```

### Per-function filter changes

| v2-* function | OLD source filter | NEW source filter |
|---|---|---|
| `v2-state-parks-dog-policy` | `governing_body_source IN ('state_polygon','state_name_rescue')` | `governing_body_type='state' AND canon source IN ('cpad','csp_parks','park_url')` |
| `v2-city-dog-policy` | `IN ('city_polygon','city_polygon_buffer','state_operator_override')` | `governing_body_type='city' AND canon source IN ('cpad','tiger_places','park_operators','park_url')` |
| `v2-county-dog-policy` | (similar) | `governing_body_type='county' AND canon source IN ('cpad','park_url')` |
| `v2-federal-dog-policy` | (similar) | `governing_body_type='federal' AND canon source IN ('nps_places','cpad','park_url')` |
| `v2-blm-sma-rescue` | `IN ('county_default','state_default')` | `canon source IN ('tiger_places') OR canon is null` (weak/missing attribution) |
| `v2-county-name-rescue` | same | same |
| `v2-ccc-enrich` | `ccc_match_name IS NOT NULL` | spatial join `ccc_access_points` directly, write practical fields |
| `v2-enrich-operational` | (parking + amenity extractor) | similar pattern; write practical fields |
| `v2-parse-temporal-restrictions` | (parses time/season text from existing rows) | reads + writes `policy_research_extractions` row in place |

---

## 7. Migration phases

| Phase | Action | Risk | Rollback |
|---|---|---|---|
| 1 | Create `policy_research_extractions` table + indexes | none — additive | drop table |
| 2 | Add `'old_school_llm'` to source CHECK + source_precedence | low — additive | revert constraint |
| 3 | Run backfill INSERT (953 rows from BSN) | none — additive, idempotent | DELETE WHERE origin='v2_dog_policy_old' |
| 4 | Replace `populate_from_research` with new version (reads policy_research_extractions) | medium — changes evidence emission. Test against golden harness first. | git revert; old definition restored |
| 5 | Re-run `populate_from_research(NULL)` to refresh evidence rows from new table | low — idempotent | re-run with old function |
| 6 | Verify `scripts/test_park_url_pipeline.py` PASSes (or differences are intentional) | none | n/a |
| 7 | Rewrite v2-* edge functions one at a time, deploy via `supabase functions deploy` | low — each function independent | redeploy old version |
| 8 | When all 7 v2-* rewritten + tested: stop calling `v2-run-pipeline` | none | resume calling it |
| 9 | After 30+ days of stability: drop `beaches_staging_new` table + OLD v2-* edge functions + utility functions (`match_beaches_*`, `v2_find_*`, `ingest_beaches_batch_with_state_filter`) | medium — irreversible | restore from snapshot |

---

## 8. What stays orphan (no work in this migration)

Per the audit's recommendation:

- `scripts/pipeline/phase1_classify.py` and `phase2_extract.py` — leave as dead code, don't migrate
- `beaches_staging` table (955 rows; only 93 with policy data) — leave in place, don't bridge
- `seed_jurisdictions.py` — one-shot setup script, leave alone
- `scripts/test_ca_cpad_vs_staging.py` — historical test script, leave alone

When the dead code becomes too noisy, separate cleanup task: delete the Python scripts, drop the `beaches_staging` table.

---

## 9. Effort estimate

| Phase | Estimated effort |
|---|---|
| 1–3 (table + backfill) | ~30 min — straight SQL |
| 4–6 (populator + golden test) | ~1 hour — populate_from_research replacement + verification |
| 7 (v2-* rewrites, ×7 functions) | ~3 hours — TS code per function, similar pattern |
| 8 (stop running v2-run-pipeline) | trivial |
| 9 (decommission) | ~30 min after burn-in period |

**Total ~5 hours of focused work**, spread over whatever timeline you want for the burn-in period before phase 9.

---

## 10. Cross-references

- Memory `project_three_pipelines_audit.md` — the audit that surfaced the dependency this migration resolves
- Memory `project_park_url_pipeline_2026-04-25.md` — the architecture that `policy_research_extractions` mirrors
- Memory `project_resolution_rules_design.md` — source precedence design
- `supabase/migrations/20260424_park_url_architecture.sql` — the pattern for `park_url_extractions`
- `supabase/migrations/20260425_resolver_refactor.sql` — the populator pattern for `populate_from_park_url`
