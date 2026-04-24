-- beach_policy_extractions + extraction_calibration + gold_set + consensus view
-- (2026-04-24) — the storage substrate for the city/CVB policy extraction
-- pipeline. See project_extraction_calibration.md for design.
--
-- Flow per extraction run:
--   1. runner fetches a source URL, strips HTML via BS4, extracts structured
--      data (BS4-truth), feeds cleaned prose + each active prompt variant
--      to the LLM, parses the response.
--   2. writes one beach_policy_extractions row per (fid, source, field, variant)
--   3. after the batch, computes consensus across variants per (fid, field),
--      and writes extraction_calibration rows scoring each extraction.
--   4. beach_policy_consensus view surfaces the canonical value per
--      (fid, field) with confidence tier for downstream consumers.

-- ── 1. Raw extractions (per variant per field per source per beach) ──────
create table if not exists public.beach_policy_extractions (
  id                bigserial primary key,
  fid               int  not null references public.us_beach_points(fid),
  source_id         int  not null references public.city_policy_sources(id),
  variant_id        int  not null references public.extraction_prompt_variants(id),
  -- denormalized for fast filtering
  field_name        text not null,
  source_type       text not null,
  variant_key       text not null,
  -- content
  raw_response      text,                -- full LLM output unchanged
  parsed_value      text,                -- normalized extracted value
  evidence_quote    text,                -- quoted text from source when LLM returned one
  raw_snippet       text,                -- the BS4-cleaned text block fed to the LLM
  parse_succeeded   boolean not null default false,
  -- provenance
  extraction_method text not null default 'llm_hybrid'
                    check (extraction_method in ('llm_hybrid','bs4_only','manual')),
  run_id            text,                -- batch id for grouping a single pipeline run
  model_name        text,                -- e.g. 'claude-opus-4-7'
  input_tokens      int,
  output_tokens     int,
  latency_ms        int,
  error             text,
  extracted_at      timestamptz not null default now()
);

create index if not exists bpe_fid_field_idx  on public.beach_policy_extractions(fid, field_name);
create index if not exists bpe_run_idx        on public.beach_policy_extractions(run_id) where run_id is not null;
create index if not exists bpe_variant_idx    on public.beach_policy_extractions(variant_id);
create index if not exists bpe_source_idx     on public.beach_policy_extractions(source_id);

comment on table public.beach_policy_extractions is
  'Raw LLM extraction output per (beach, source URL, field, prompt variant). Preserves every variant''s claim so consensus and calibration can recompute over time without re-running the pipeline.';

-- ── 2. Calibration scores (per extraction) ───────────────────────────────
create table if not exists public.extraction_calibration (
  id                 bigserial primary key,
  extraction_id      bigint not null references public.beach_policy_extractions(id) on delete cascade,
  -- denormalized for analytics
  variant_id         int  not null references public.extraction_prompt_variants(id),
  field_name         text not null,
  -- signals (all nullable — not every signal is available for every extraction)
  parse_succeeded    boolean,             -- duplicated from parent for fast SQL
  matches_consensus  boolean,             -- value = majority for (fid, field)
  matches_bs4_truth  boolean,             -- only populated when BS4 had a structured extraction
  matches_gold_set   boolean,             -- only populated when a human-verified gold value exists
  consensus_group_id text,                -- groups extractions that should consensus together
  scored_at          timestamptz not null default now()
);

create index if not exists ec_variant_idx on public.extraction_calibration(variant_id);
create index if not exists ec_field_idx   on public.extraction_calibration(field_name);

comment on table public.extraction_calibration is
  'Per-extraction scoring — fuels the rollup that promotes winning variants to is_canon. Rows inserted after a run computes consensus + optional BS4/gold-set comparisons.';

-- ── 3. Human-verified gold set for calibration anchoring ─────────────────
create table if not exists public.beach_policy_gold_set (
  id             bigserial primary key,
  fid            int  not null references public.us_beach_points(fid),
  field_name     text not null,
  verified_value text,                     -- canonical value per admin inspection
  source_url     text,                     -- the page the admin verified against
  notes          text,
  verified_by    text not null,
  verified_at    timestamptz not null default now(),
  unique (fid, field_name)
);

create index if not exists bpgs_field_idx on public.beach_policy_gold_set(field_name);

comment on table public.beach_policy_gold_set is
  'Human-verified canonical (fid, field) → value. Small curated sample (~20 beaches across diverse cities) used to anchor calibration. Variants that match gold values are trusted more than those that merely match cross-variant consensus.';

-- ── 4. Consensus view: canonical value per (fid, field) ──────────────────
-- Picks the most common successful parsed_value per (fid, field), weighted
-- by source_type (city_muni_code > city_official > city_beaches >
-- city_dog_policy > visitor_bureau_beaches > visitor_bureau > other).
-- Returns a confidence tier based on source agreement.
create or replace view public.beach_policy_consensus as
with source_weight as (
  select id as source_id, source_type,
    case source_type
      when 'city_muni_code'          then 10
      when 'city_dog_policy'         then 8
      when 'city_official'           then 7
      when 'city_beaches'            then 6
      when 'visitor_bureau_beaches'  then 4
      when 'visitor_bureau'          then 3
      else 1
    end as weight
  from public.city_policy_sources
),
valid_extractions as (
  select e.fid, e.field_name, e.parsed_value, e.variant_id, e.source_id,
         w.weight, w.source_type
  from public.beach_policy_extractions e
  join source_weight w on w.source_id = e.source_id
  where e.parse_succeeded = true
    and e.parsed_value is not null
    and e.parsed_value <> 'unclear'
),
value_scores as (
  select
    fid, field_name, parsed_value,
    count(*)                                             as support_count,
    count(distinct source_id)                            as source_count,
    count(distinct source_type) filter (where source_type like 'city%')    as distinct_city_sources,
    count(distinct source_type) filter (where source_type like 'visitor%') as distinct_cvb_sources,
    sum(weight)                                           as total_weight,
    array_agg(distinct source_type)                       as contributing_sources
  from valid_extractions
  group by fid, field_name, parsed_value
),
ranked as (
  select *,
    row_number() over (partition by fid, field_name
                       order by total_weight desc, support_count desc) as rnk
  from value_scores
),
canonical as (
  select fid, field_name, parsed_value as canonical_value,
         support_count, source_count, total_weight,
         contributing_sources
  from ranked where rnk = 1
),
disagreements as (
  select fid, field_name, count(*) as distinct_values
  from value_scores
  group by fid, field_name
)
select
  c.fid,
  c.field_name,
  c.canonical_value,
  c.support_count,
  c.source_count,
  c.total_weight,
  c.contributing_sources,
  d.distinct_values,
  case
    when d.distinct_values = 1 and c.source_count >= 2 then 'high'     -- agreement across ≥2 sources
    when d.distinct_values = 1                          then 'medium'  -- single source agrees with itself
    when d.distinct_values = 2                          then 'low'     -- one disagreement
    else                                                     'conflict' -- 3+ distinct values
  end as confidence
from canonical c
join disagreements d using (fid, field_name);

comment on view public.beach_policy_consensus is
  'Canonical value per (fid, field) with confidence tier. Weighted by source precedence: muni_code > city_official > city_beaches > CVB. Downstream consumers (enrichment jobs, beaches-staging-new ingest, display layer) should read from here, not the raw extractions.';

grant select on public.beach_policy_consensus to service_role;
