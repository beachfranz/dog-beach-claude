-- 20260504_unified_pipeline_phase1_consensus_layer2.sql
--
-- Consensus ensemble Phase B (Layer 2 — cross-source weighted voting).
-- For each (beach, field), gathers claims across all sources from
-- beach_enrichment_provenance, applies Wilson-bounded calibration weights
-- and source confidences, picks the winning value, and writes to
-- beach_field_consensus.
--
-- Auto-promoters then read from beach_field_consensus for voted fields,
-- canonical evidence for non-voted payload fields (areas, hours, etc.).
--
-- Spec: docs/consensus-ensemble.md

begin;

-- ─────────────────────────────────────────────────────────────────────────
-- Configuration: which (field_group, claim_key) we vote on, and what
-- field_kind to use for normalization + calibration lookup.
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists public.consensus_field_config (
  field_group text not null,
  claim_key   text not null,
  field_kind  text not null,
  primary key (field_group, claim_key)
);

insert into public.consensus_field_config (field_group, claim_key, field_kind) values
  ('dogs',      'allowed',             'dogs_allowed'),
  ('dogs',      'leash_required',      'leash_policy'),
  ('practical', 'has_lifeguards',      'has_lifeguards'),
  ('practical', 'has_restrooms',       'has_restrooms'),
  ('practical', 'has_parking',         'has_parking'),
  ('practical', 'has_showers',         'has_showers'),
  ('practical', 'has_disabled_access', 'has_disabled_access'),
  ('practical', 'has_picnic_area',     'has_picnic_area'),
  ('practical', 'has_fire_pits',       'has_fire_pits'),
  ('practical', 'has_food',            'has_food'),
  ('practical', 'has_drinking_water',  'has_drinking_water')
on conflict (field_group, claim_key) do update
  set field_kind = excluded.field_kind;

comment on table public.consensus_field_config is
  'Phase B Layer 2 voted fields. Maps (field_group, claim_key) → field_kind '
  'for _consensus_normalize + _calibration_weight lookups. Add a row to '
  'enable consensus on a new field. Non-listed payload keys (areas_zones, '
  'time_prohibited_hours, etc.) flow through via the canonical evidence row.';

-- ─────────────────────────────────────────────────────────────────────────
-- beach_field_consensus — per-(beach, field) winning value + agreement
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists public.beach_field_consensus (
  gold_fid       bigint  not null references public.beaches_gold(fid) on delete cascade,
  field_name     text    not null,        -- the claim_key (e.g. 'allowed', 'has_lifeguards')
  field_group    text    not null,
  field_kind     text    not null,        -- e.g. 'dogs_allowed', 'leash_policy'
  winning_value  text,
  confidence     numeric,                  -- winning_weight / total_weight
  disagreement   boolean default false,
  vote_breakdown jsonb,                    -- {value: {weight, voters: [{source, weight}]}}
  source_count   integer,
  computed_at    timestamptz default now(),
  primary key (gold_fid, field_name)
);

create index if not exists beach_field_consensus_disagreement_idx
  on public.beach_field_consensus(disagreement) where disagreement = true;

comment on table public.beach_field_consensus is
  'Phase B Layer 2 output: per-(beach, field) winning value from cross-source '
  'weighted voting. confidence = winning_weight / total_weight; disagreement = '
  'TRUE when margin to runner-up < binomial SD. vote_breakdown jsonb shows the '
  'full vote tally for curator review. Read by auto-promoters for voted fields.';

-- ─────────────────────────────────────────────────────────────────────────
-- compute_beach_field_consensus — the Layer 2 ensemble function
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.compute_beach_field_consensus(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint) as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin
  with claims as (
    -- Per (beach, voted-field, source) raw claim
    select e.gold_fid,
           c.field_group, c.claim_key, c.field_kind,
           e.source,
           coalesce(e.confidence, 0.5) as source_confidence,
           e.claimed_values->>c.claim_key as raw_value,
           public._consensus_normalize(c.field_kind, e.claimed_values->>c.claim_key) as norm_value
      from public.beach_enrichment_provenance e
      join public.consensus_field_config c on c.field_group = e.field_group
     where e.gold_fid is not null
       and (p_fid is null or e.gold_fid = p_fid)
       and e.claimed_values ? c.claim_key
  ),
  weighted as (
    select c.*,
           c.source_confidence * public._calibration_weight(c.field_kind, c.source) as vote_weight
      from claims c
     where c.norm_value is not null
  ),
  votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value,
           sum(vote_weight) as bucket_weight,
           jsonb_agg(jsonb_build_object(
             'source', source,
             'weight', round(vote_weight::numeric, 3),
             'src_conf', round(source_confidence::numeric, 3)
           ) order by vote_weight desc) as voters
      from weighted
     group by gold_fid, field_group, claim_key, field_kind, norm_value
  ),
  ranked as (
    select v.*,
           sum(bucket_weight) over (partition by gold_fid, claim_key) as total_weight,
           row_number() over (
             partition by gold_fid, claim_key
             order by bucket_weight desc
           ) as rnk
      from votes v
  ),
  -- For each (beach, field), gather winner + runner-up + full breakdown
  winners as (
    select gold_fid, field_group, claim_key, field_kind,
           norm_value as winning_value,
           bucket_weight as winner_weight,
           total_weight,
           (select bucket_weight from ranked r2
             where r2.gold_fid = ranked.gold_fid and r2.claim_key = ranked.claim_key
               and r2.rnk = 2 limit 1) as runner_up_weight,
           (select jsonb_object_agg(r3.norm_value,
                     jsonb_build_object('weight', round(r3.bucket_weight::numeric, 3),
                                        'voters', r3.voters))
              from ranked r3
             where r3.gold_fid = ranked.gold_fid and r3.claim_key = ranked.claim_key
           ) as breakdown,
           (select count(*)::int from ranked r4
             where r4.gold_fid = ranked.gold_fid and r4.claim_key = ranked.claim_key
           ) as bucket_count
      from ranked
     where rnk = 1
  ),
  upsert as (
    insert into public.beach_field_consensus
      (gold_fid, field_name, field_group, field_kind, winning_value,
       confidence, disagreement, vote_breakdown, source_count, computed_at)
    select w.gold_fid, w.claim_key, w.field_group, w.field_kind, w.winning_value,
           round((w.winner_weight / nullif(w.total_weight, 0))::numeric, 3),
           public._consensus_disagreement(w.total_weight, w.winner_weight, w.runner_up_weight),
           w.breakdown, w.bucket_count, now()
      from winners w
    on conflict (gold_fid, field_name) do update
      set winning_value  = excluded.winning_value,
          confidence     = excluded.confidence,
          disagreement   = excluded.disagreement,
          vote_breakdown = excluded.vote_breakdown,
          source_count   = excluded.source_count,
          field_group    = excluded.field_group,
          field_kind     = excluded.field_kind,
          computed_at    = now()
      where beach_field_consensus.winning_value is distinct from excluded.winning_value
         or beach_field_consensus.confidence is distinct from excluded.confidence
         or beach_field_consensus.disagreement is distinct from excluded.disagreement
         or beach_field_consensus.source_count is distinct from excluded.source_count
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

comment on function public.compute_beach_field_consensus(bigint) is
  'Phase B Layer 2 cross-source weighted voting. For each (beach, voted-field), '
  'gathers claims from beach_enrichment_provenance, applies '
  '_calibration_weight(field, source) × source.confidence weights, picks '
  'winner. Manual gets weight 1.0 (heavy vote, not hard override). '
  'Disagreement_flag uses 1-SD rule.';

-- ─────────────────────────────────────────────────────────────────────────
-- Add confidence + disagreement_flag columns to consumer tables
-- ─────────────────────────────────────────────────────────────────────────

alter table public.beach_dog_policy
  add column if not exists consensus_confidence numeric,
  add column if not exists disagreement_flag    boolean default false;

alter table public.beach_amenities
  add column if not exists consensus_confidence numeric,
  add column if not exists disagreement_flag    boolean default false;

-- ─────────────────────────────────────────────────────────────────────────
-- Update auto-promoters to read from beach_field_consensus for voted fields,
-- canonical evidence for non-voted payload (areas, hours, etc.)
-- ─────────────────────────────────────────────────────────────────────────

create or replace function public.promote_canonical_dogs_to_beach_dog_policy(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint) as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with consensus_dogs as (
    -- Pivot consensus to per-beach (allowed, leash_required) shape
    select gold_fid,
      max(winning_value) filter (where field_name = 'allowed')        as v_allowed,
      max(confidence)    filter (where field_name = 'allowed')        as conf_allowed,
      bool_or(disagreement) filter (where field_name = 'allowed')     as dis_allowed,
      max(winning_value) filter (where field_name = 'leash_required') as v_leash,
      max(confidence)    filter (where field_name = 'leash_required') as conf_leash,
      bool_or(disagreement) filter (where field_name = 'leash_required') as dis_leash
      from public.beach_field_consensus
     where field_group = 'dogs' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  -- Non-voted payload fields from canonical evidence (off_leash_flag, time_*, areas_*)
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'dogs' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  derived as (
    select c.gold_fid as fid,
           public._norm_dogs_allowed(c.v_allowed)        as new_dogs_allowed,
           public._norm_leash_policy(c.v_leash)          as new_leash_policy,
           public._norm_bool(p.claimed_values->'off_leash_exists') as new_off_leash_flag,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',1))
             as new_prohib_start,
           public._norm_time_text(split_part(p.claimed_values->>'time_prohibited_hours','-',2))
             as new_prohib_end,
           coalesce(p.claimed_values->>'areas_evidence',
                    p.claimed_values->>'areas_boundaries') as new_dogs_allowed_areas,
           greatest(c.conf_allowed, c.conf_leash) as new_confidence,
           coalesce(c.dis_allowed, false) or coalesce(c.dis_leash, false) as new_disagreement
      from consensus_dogs c
      left join canon_payload p on p.fid = c.gold_fid
  ),
  protected as (
    select arena_group_id from public.beach_dog_policy where source = 'manual_curator'
  ),
  upsert as (
    insert into public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select d.fid, d.new_dogs_allowed, d.new_leash_policy, d.new_off_leash_flag,
           d.new_prohib_start, d.new_prohib_end, d.new_dogs_allowed_areas,
           'auto_promoted_from_consensus', now(),
           format('Phase B consensus: dogs_allowed=%s (conf=%s%s), leash=%s (conf=%s%s)',
                  d.new_dogs_allowed, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end,
                  d.new_leash_policy, coalesce(d.new_confidence, 0),
                  case when d.new_disagreement then ', DISAGREEMENT' else '' end),
           d.new_confidence, d.new_disagreement
      from derived d
     where d.fid not in (select arena_group_id from protected)
       and (d.new_dogs_allowed is not null or d.new_leash_policy is not null
            or d.new_off_leash_flag is not null or d.new_prohib_start is not null
            or d.new_dogs_allowed_areas is not null)
       and coalesce(d.new_confidence, 1) >= p_min_confidence
    on conflict (arena_group_id) do update
      set dogs_allowed=excluded.dogs_allowed,
          leash_policy=excluded.leash_policy,
          off_leash_flag=excluded.off_leash_flag,
          dogs_prohibited_start=excluded.dogs_prohibited_start,
          dogs_prohibited_end=excluded.dogs_prohibited_end,
          dogs_allowed_areas=excluded.dogs_allowed_areas,
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_dog_policy.source <> 'manual_curator'
        and (beach_dog_policy.dogs_allowed is distinct from excluded.dogs_allowed
          or beach_dog_policy.leash_policy is distinct from excluded.leash_policy
          or beach_dog_policy.off_leash_flag is distinct from excluded.off_leash_flag
          or beach_dog_policy.dogs_prohibited_start is distinct from excluded.dogs_prohibited_start
          or beach_dog_policy.dogs_prohibited_end is distinct from excluded.dogs_prohibited_end
          or beach_dog_policy.dogs_allowed_areas is distinct from excluded.dogs_allowed_areas
          or beach_dog_policy.consensus_confidence is distinct from excluded.consensus_confidence
          or beach_dog_policy.disagreement_flag is distinct from excluded.disagreement_flag)
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upsert;
  return query select ins::bigint, upd::bigint, skp::bigint;
end;
$$ language plpgsql;


create or replace function public.promote_canonical_practical_to_beach_amenities(
  p_fid bigint default null,
  p_min_confidence numeric default 0.5
)
returns table(rows_inserted bigint, rows_updated bigint, gold_open_close_set bigint) as $$
declare ins int := 0; upd int := 0; gold_set int := 0;
begin
  with consensus_practical as (
    -- Pivot consensus to per-beach amenity bools
    select gold_fid,
      max(winning_value) filter (where field_name = 'has_lifeguards')      as v_lifeguards,
      max(winning_value) filter (where field_name = 'has_restrooms')       as v_restrooms,
      max(winning_value) filter (where field_name = 'has_parking')         as v_parking,
      max(winning_value) filter (where field_name = 'has_showers')         as v_showers,
      max(winning_value) filter (where field_name = 'has_disabled_access') as v_disabled,
      max(winning_value) filter (where field_name = 'has_food')            as v_food,
      max(winning_value) filter (where field_name = 'has_fire_pits')       as v_fire,
      max(winning_value) filter (where field_name = 'has_picnic_area')     as v_picnic,
      max(winning_value) filter (where field_name = 'has_drinking_water')  as v_water,
      max(confidence) as max_confidence,
      bool_or(disagreement) as any_disagreement
      from public.beach_field_consensus
     where field_group = 'practical' and (p_fid is null or gold_fid = p_fid)
     group by gold_fid
  ),
  canon_payload as (
    select e.gold_fid as fid, e.claimed_values
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
     where e.field_group = 'practical' and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  protected as (
    select arena_group_id from public.beach_amenities where source = 'manual_curator'
  ),
  upsert_amen as (
    insert into public.beach_amenities
      (arena_group_id, has_restrooms, has_lifeguards, has_showers,
       has_drinking_water, has_disabled_access, has_food, has_fire_pits,
       has_picnic_area, parking_type, hours_text, source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    select c.gold_fid,
      case lower(c.v_restrooms)  when 'true' then true when 'false' then false end,
      case lower(c.v_lifeguards) when 'true' then true when 'false' then false end,
      case lower(c.v_showers)    when 'true' then true when 'false' then false end,
      case lower(c.v_water)      when 'true' then true when 'false' then false end,
      case lower(c.v_disabled)   when 'true' then true when 'false' then false end,
      case lower(c.v_food)       when 'true' then true when 'false' then false end,
      case lower(c.v_fire)       when 'true' then true when 'false' then false end,
      case lower(c.v_picnic)     when 'true' then true when 'false' then false end,
      p.claimed_values->>'parking_type',
      coalesce(p.claimed_values->>'hours_text',
        case when (p.claimed_values->>'hours_open') is not null
              or (p.claimed_values->>'hours_close') is not null
        then trim(both '-' from format('%s-%s',
                  coalesce(p.claimed_values->>'hours_open',''),
                  coalesce(p.claimed_values->>'hours_close','')))
        end),
      'auto_promoted_from_consensus', now(),
      format('Phase B consensus: max_confidence=%s%s', coalesce(c.max_confidence, 0),
             case when c.any_disagreement then ' DISAGREEMENT' else '' end),
      c.max_confidence, c.any_disagreement
      from consensus_practical c
      left join canon_payload p on p.fid = c.gold_fid
     where c.gold_fid not in (select arena_group_id from protected)
    on conflict (arena_group_id) do update
      set has_restrooms=excluded.has_restrooms,
          has_lifeguards=excluded.has_lifeguards,
          has_showers=excluded.has_showers,
          has_drinking_water=excluded.has_drinking_water,
          has_disabled_access=excluded.has_disabled_access,
          has_food=excluded.has_food,
          has_fire_pits=excluded.has_fire_pits,
          has_picnic_area=excluded.has_picnic_area,
          parking_type=coalesce(excluded.parking_type, beach_amenities.parking_type),
          hours_text=coalesce(excluded.hours_text, beach_amenities.hours_text),
          source=excluded.source, curated_at=now(), notes=excluded.notes,
          consensus_confidence=excluded.consensus_confidence,
          disagreement_flag=excluded.disagreement_flag
      where beach_amenities.source <> 'manual_curator'
    returning (xmax = 0) as inserted, arena_group_id
  ),
  parsed_hours as (
    select c.gold_fid as fid,
           public._norm_time_text(p.claimed_values->>'hours_open')  as t_open,
           public._norm_time_text(p.claimed_values->>'hours_close') as t_close
      from consensus_practical c
      join canon_payload p on p.fid = c.gold_fid
  ),
  upd_gold as (
    update public.beaches_gold g
       set open_time  = coalesce(g.open_time,  ph.t_open),
           close_time = coalesce(g.close_time, ph.t_close)
      from parsed_hours ph
     where g.fid = ph.fid
       and ((g.open_time is null and ph.t_open is not null)
         or (g.close_time is null and ph.t_close is not null))
    returning g.fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         (select count(*) from upd_gold)
    into ins, upd, gold_set from upsert_amen;
  return query select ins::bigint, upd::bigint, gold_set::bigint;
end;
$$ language plpgsql;

commit;
