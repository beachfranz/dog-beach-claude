-- 20260504_unified_pipeline_phase1_consensus_layer1.sql
--
-- Consensus ensemble Phase A (Layer 1 — within-source URL voting). Replaces
-- the "first conclusive" picker in populate_from_unified_v1_gold with
-- weighted URL voting. See docs/consensus-ensemble.md.
--
-- Design questions locked 2026-05-04:
--   1. Wilson 90% lower-bound for calibration weights (penalizes small samples)
--   2. URL voting: authority_score only (model agreement validated)
--   3. Tie-break: highest-individual-confidence + disagreement_flag
--   4. Manual = heavy-weight vote (calibration_weight=1.0); not hard override
--   ★ Disagreement flag = (margin between top two < binomial_sd of vote distribution)
--     binomial_sd = sqrt(total_weight × winner_share × (1-winner_share))

begin;

-- ─────────────────────────────────────────────────────────────────────────
-- Wilson lower bound (90% confidence) — penalizes small samples
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public._wilson_lower_bound(
  p numeric, n integer, z numeric default 1.645
) returns numeric language sql immutable as $$
  select case
    when n is null or n <= 0 or p is null then 0
    when p < 0 or p > 1 then null
    else greatest(0::numeric, least(1::numeric,
      (p + (z*z)/(2.0*n) - z * sqrt(greatest(0::numeric,
        (p*(1-p) + (z*z)/(4.0*n)) / n
      )))
      / (1 + (z*z)/n)
    ))
  end;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- field_source_calibration — per-(field, source) raw accuracy + sample
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists public.field_source_calibration (
  field_name      text not null,
  source          text not null,
  raw_accuracy    numeric not null check (raw_accuracy between 0 and 1),
  sample_size     integer not null check (sample_size >= 0),
  computed_at     timestamptz default now(),
  notes           text,
  primary key (field_name, source)
);

create or replace function public._calibration_weight(
  p_field text, p_source text
) returns numeric language sql stable as $$
  -- Manual override: full weight (heavy-weight vote, not hard short-circuit)
  select case
    when p_source = 'manual' or p_source = 'manual_curator' then 1.0::numeric
    else (
      select coalesce(public._wilson_lower_bound(c.raw_accuracy, c.sample_size), 0.5)
        from public.field_source_calibration c
       where c.field_name = p_field and c.source = p_source
       limit 1
    )
  end;
$$;

-- Seed (from project_extraction_calibration.md cross-field findings 2026-05-03)
insert into public.field_source_calibration (field_name, source, raw_accuracy, sample_size, notes) values
  ('dogs_allowed',  'research',        0.914, 35, 'Research wins dogs_allowed'),
  ('dogs_allowed',  'park_url',        0.857, 35, 'Strong second'),
  ('dogs_allowed',  'governing_body',  0.750, 24, 'Solid mid'),
  ('dogs_allowed',  'old_school_llm',  0.694, 49, 'Baseline'),
  ('dogs_allowed',  'json_explode',    0.694, 49, 'Same source-family as old_school_llm'),
  ('dogs_allowed',  'unified_v1',      0.700, 30, 'Estimate from probe; recalibrate'),
  ('leash_policy',  'research',        0.833,  6, 'Small sample; Wilson penalizes'),
  ('leash_policy',  'old_school_llm',  0.773, 22, ''),
  ('leash_policy',  'park_url',        0.750,  8, 'Small sample'),
  ('leash_policy',  'governing_body',  0.500, 10, 'Coin-flip; Wilson-90 → ~0.27'),
  ('leash_policy',  'json_explode',    0.773, 22, ''),
  ('leash_policy',  'unified_v1',      0.700, 14, 'Estimate'),
  ('has_lifeguards', 'park_url',       0.900, 10, '90% — best amenity source'),
  ('has_lifeguards', 'old_school_llm', 0.818, 11, ''),
  ('has_lifeguards', 'unified_v1',     0.700,  4, 'Tiny sample'),
  ('has_restrooms',  'park_url',       0.900, 16, 'Estimate (close to lifeguards)'),
  ('has_restrooms',  'old_school_llm', 0.850, 16, ''),
  ('has_parking',    'park_url',       0.900, 18, ''),
  ('has_parking',    'old_school_llm', 0.850, 18, '')
on conflict (field_name, source) do update
  set raw_accuracy = excluded.raw_accuracy,
      sample_size = excluded.sample_size,
      notes = excluded.notes,
      computed_at = now();

-- ─────────────────────────────────────────────────────────────────────────
-- Field normalizers — bucket equivalent values (Yes/yes/TRUE/true → "yes")
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public._consensus_normalize(
  p_field text, p_value text
) returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes' when 'no' then 'no'
        when 'restricted' then 'yes' when 'seasonal' then 'seasonal'
        when 'mixed' then 'mixed'
        else null end
      when 'leash_policy' then case lower(trim(p_value))
        when 'on_leash' then 'on_leash' when 'off_leash' then 'off_leash'
        when 'mixed' then 'mixed' when 'mixed_by_zone' then 'mixed'
        when 'required' then 'on_leash' when 'optional' then 'off_leash'
        when 'varies_by_time' then 'varies_by_time'
        else null end
      when 'has_lifeguards' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_restrooms' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_parking' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_showers' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      when 'has_disabled_access' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false'
        when 'yes' then 'true' when 'no' then 'false' else null end
      else lower(trim(p_value))
    end
  end;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- Disagreement test (1-SD rule)
--   Given total weight + winner share + margin between top two:
--     binomial_sd = sqrt(total * p * (1-p))
--     disagreement = (margin < binomial_sd)
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public._consensus_disagreement(
  p_total_weight numeric,
  p_winner_weight numeric,
  p_runner_up_weight numeric
) returns boolean language sql immutable as $$
  select case
    when p_total_weight is null or p_total_weight <= 0 then false
    when p_runner_up_weight is null then false  -- only one bucket, no disagreement
    else (
      (p_winner_weight - p_runner_up_weight) <
      sqrt(greatest(0::numeric,
        p_total_weight
        * (p_winner_weight / nullif(p_total_weight, 0))
        * (1 - p_winner_weight / nullif(p_total_weight, 0))
      ))
    )
  end;
$$;

comment on function public._consensus_disagreement(numeric, numeric, numeric) is
  'Returns TRUE when the gap between the winning vote bucket and the '
  'runner-up is less than the binomial standard deviation of the vote '
  'distribution. Used to flag close consensus calls for curator review. '
  'Examples: 9v1=false (clear), 7v3=false, 6v4=false, 5v5=true.';

-- ─────────────────────────────────────────────────────────────────────────
-- Layer 1: rewrite populate_from_unified_v1_gold with weighted URL voting
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.populate_from_unified_v1_gold(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint) as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin
  with extractions as (
    select e.arena_group_id as gold_fid,
           e.field_name,
           public._consensus_normalize(e.field_name, e.parsed_value) as norm_value,
           e.parsed_value     as raw_value,
           e.evidence_quote,
           e.raw_response,
           coalesce(e.source_authority_score, 1) as auth,
           e.id,
           public._unified_field_group(e.field_name) as field_group
      from public.beach_policy_extractions e
      join public.beaches_gold g on g.fid = e.arena_group_id
     where e.variant_key = 'unified_v1'
       and e.arena_group_id is not null
       and (p_fid is null or e.arena_group_id = p_fid)
       and public._unified_field_group(e.field_name) is not null
  ),
  votes as (
    select gold_fid, field_name, field_group, norm_value,
           sum(auth) as weight,
           count(*)  as urls,
           max(auth) as max_auth,
           (array_agg(id order by auth desc, id desc))[1] as best_id
      from extractions
     where norm_value is not null
     group by gold_fid, field_name, field_group, norm_value
  ),
  ranked as (
    select v.*,
           sum(weight) over (partition by gold_fid, field_name) as total_weight,
           row_number() over (
             partition by gold_fid, field_name
             order by weight desc, max_auth desc, urls desc, best_id desc
           ) as rnk
      from votes v
  ),
  -- Pull winner + runner-up weight in one shot
  winner_with_runner_up as (
    select w.gold_fid, w.field_name, w.field_group, w.norm_value,
           w.weight as winner_weight,
           w.total_weight,
           w.urls, w.best_id, w.max_auth,
           (select ru.weight from ranked ru
             where ru.gold_fid = w.gold_fid and ru.field_name = w.field_name
               and ru.rnk = 2 limit 1) as runner_up_weight
      from ranked w
     where w.rnk = 1
  ),
  with_signals as (
    select wr.*,
           wr.winner_weight / nullif(wr.total_weight, 0) as agreement,
           public._consensus_disagreement(
             wr.total_weight, wr.winner_weight, wr.runner_up_weight
           ) as disagreement
      from winner_with_runner_up wr
  ),
  best_per_field as (
    select w.gold_fid, w.field_name, w.field_group, w.norm_value as parsed_value,
           e.evidence_quote, e.raw_response,
           w.agreement, w.urls, w.max_auth, w.disagreement,
           w.max_auth as auth
      from with_signals w
      join extractions e on e.id = w.best_id
  ),
  per_field_payload as (
    select gold_fid, field_group, field_name, auth, agreement, urls, disagreement,
           case
             when field_name = 'dogs_off_leash_area' then jsonb_build_object(
               'off_leash_exists',     public._safe_jsonb(raw_response)->'off_leash_area_exists',
               'off_leash_area_name',  public._safe_jsonb(raw_response)->'area_name',
               'off_leash_hours',      public._safe_jsonb(raw_response)->'hours',
               'off_leash_notes',      public._safe_jsonb(raw_response)->'notes')
             when field_name = 'dogs_time_restrictions' then jsonb_build_object(
               'time_has_restriction',  public._safe_jsonb(raw_response)->'has_time_restriction',
               'time_allowed_hours',    public._safe_jsonb(raw_response)->'allowed_hours',
               'time_prohibited_hours', public._safe_jsonb(raw_response)->'prohibited_hours',
               'time_evidence',         public._safe_jsonb(raw_response)->'evidence')
             when field_name = 'dogs_seasonal_restrictions' then jsonb_build_object(
               'seasonal_has',         public._safe_jsonb(raw_response)->'has_seasonal_restriction',
               'seasonal_description', public._safe_jsonb(raw_response)->'description',
               'seasonal_period',      public._safe_jsonb(raw_response)->'affected_period')
             when field_name = 'dogs_allowed_areas' then jsonb_build_object(
               'areas_coverage',   public._safe_jsonb(raw_response)->'coverage',
               'areas_zones',      public._safe_jsonb(raw_response)->'zones',
               'areas_boundaries', public._safe_jsonb(raw_response)->'boundaries',
               'areas_evidence',   public._safe_jsonb(raw_response)->'evidence')
             when field_name = 'hours_text' then jsonb_build_object(
               'hours_open',        public._safe_jsonb(raw_response)->'open',
               'hours_close',       public._safe_jsonb(raw_response)->'close',
               'hours_notes',       public._safe_jsonb(raw_response)->'notes',
               'hours_is_24_hours', public._safe_jsonb(raw_response)->'is_24_hours')
             when field_name = 'dogs_allowed' then
               jsonb_build_object('allowed', parsed_value, 'allowed_evidence', evidence_quote)
             when field_name = 'leash_policy' then
               jsonb_build_object('leash_required', parsed_value, 'leash_evidence', evidence_quote)
             when field_name = 'has_lifeguards' then
               jsonb_build_object('has_lifeguards', parsed_value::boolean)
             when field_name = 'has_restrooms' then
               jsonb_build_object('has_restrooms', parsed_value::boolean)
             when field_name = 'has_parking' then
               jsonb_build_object('has_parking', parsed_value::boolean)
             when field_name = 'has_showers' then
               jsonb_build_object('has_showers', parsed_value::boolean)
             when field_name = 'has_disabled_access' then
               jsonb_build_object('has_disabled_access', parsed_value::boolean)
           end as payload
      from best_per_field
  ),
  merged as (
    select gold_fid, field_group,
           jsonb_strip_nulls(
             coalesce(jsonb_object_agg(k, v) filter (where k is not null), '{}'::jsonb)
           ) as claimed_values,
           greatest(0.30, least(0.95,
             (0.50 + (max(auth) * 0.10)) * min(agreement)
           )) as confidence,
           bool_or(disagreement) as has_disagreement,
           count(*) as field_count,
           max(auth) as max_auth,
           round(min(agreement), 2) as min_agreement,
           sum(urls) as total_url_votes
      from per_field_payload,
           lateral jsonb_each(payload) as kv(k, v)
     where v is not null and v <> 'null'::jsonb
     group by gold_fid, field_group
  ),
  upsert as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, claimed_values, confidence, notes, updated_at)
    select gold_fid, field_group, 'unified_v1', claimed_values, confidence,
           format('Layer 1 consensus: %s fields, %s URL votes, min agreement=%s, max_auth=%s%s',
                  field_count, total_url_votes, min_agreement, max_auth,
                  case when has_disagreement then ' [DISAGREEMENT]' else '' end),
           now()
      from merged
     where claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          notes = excluded.notes,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
         or beach_enrichment_provenance.notes is distinct from excluded.notes
    returning (xmax = 0) as inserted, gold_fid
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted),
         count(distinct gold_fid)
    into ins_count, upd_count, beaches_count
    from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$$ language plpgsql;

comment on function public.populate_from_unified_v1_gold(bigint) is
  'Phase 1 unified pipeline + Consensus Layer 1 (2026-05-04). Per (beach, '
  'field), votes are normalized values weighted by URL authority_score. '
  'Winning value comes from highest weighted bucket; agreement = winning_weight/'
  'total_weight. Disagreement_flag fires when margin < binomial SD of vote '
  'distribution (the "1-SD rule"). Both signals end up in the notes field for '
  'downstream consumers (Layer 2 ensemble + curator UI).';

commit;
