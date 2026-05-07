-- 20260507_binary_leash_phase_1b_consensus.sql
--
-- Binary leash schema migration — Phase 1B (consensus).
--
-- Adds derivation functions that extract has_on_leash / has_off_leash
-- signals from each source's existing claim shape (allowed, leash_required,
-- off_leash_exists, areas_evidence). Upstream emitters DON'T need to change
-- — the derivation runs at consensus time on whatever each source already
-- writes.
--
-- Two new claim_keys (has_on_leash, has_off_leash) registered in
-- consensus_field_config. compute_beach_field_consensus extended with a
-- UNION-ALL claims_derived CTE that emits per-binary votes for every
-- dogs-field source row. Each binary's votes are aggregated independently,
-- so a beach with both on-leash zones (paths) and off-leash zones (sand)
-- ends up (T, T) without source-vs-source conflict.
--
-- _consensus_normalize gets the two new field_kinds (boolean string
-- normalization).
--
-- Phase 1C (next): extend promote_canonical_dogs_to_beach_dog_policy to
-- write the booleans alongside legacy leash_policy.

begin;

-- ============================================================
-- 1. Per-binary derivation functions
-- ============================================================

-- has_on_leash: does this source's claim assert that leashed dogs are
-- allowed in any zone? Returns 'true', 'false', or null (abstain).
--
-- Asserts TRUE when:
--   * leash_required is 'on_leash' / 'mixed' / 'mixed_by_zone' / 'required'
--   * allowed is 'mixed' (source thinks both modes apply)
--   * areas_evidence contains 'on_leash'
--   * allowed is 'yes' AND no explicit off-leash claim (conservative default)
--
-- Asserts FALSE when:
--   * allowed is 'no' (no dog access at all)
--
-- Otherwise null (abstain) — the source doesn't address this dimension.
create or replace function public._derive_has_on_leash(claim jsonb)
returns text language sql immutable as $$
  select case
    when claim is null then null
    when (claim->>'allowed') = 'no' then 'false'
    when (claim->>'leash_required') in ('on_leash', 'mixed', 'mixed_by_zone', 'required') then 'true'
    when (claim->>'allowed') = 'mixed' then 'true'
    when claim->>'areas_evidence' ~ '\mon_leash\M' then 'true'
    when (claim->>'allowed') = 'yes' and coalesce(claim->>'leash_required', '') <> 'off_leash' then 'true'
    else null
  end
$$;

-- has_off_leash: does this source's claim assert that off-leash dogs are
-- allowed in any zone? Same shape as has_on_leash.
--
-- Asserts TRUE when:
--   * leash_required is 'off_leash' / 'mixed' / 'mixed_by_zone'
--   * off_leash_exists is 'true'
--   * allowed is 'mixed'
--   * areas_evidence contains 'off_leash'
--
-- Asserts FALSE when:
--   * allowed is 'no'
--   * off_leash_exists is 'false' (explicit denial)
--
-- Otherwise null — the source doesn't address off-leash.
create or replace function public._derive_has_off_leash(claim jsonb)
returns text language sql immutable as $$
  select case
    when claim is null then null
    when (claim->>'allowed') = 'no' then 'false'
    when (claim->>'leash_required') in ('off_leash', 'mixed', 'mixed_by_zone') then 'true'
    when (claim->>'off_leash_exists') = 'true' then 'true'
    when (claim->>'off_leash_exists') = 'false' then 'false'
    when (claim->>'allowed') = 'mixed' then 'true'
    when claim->>'areas_evidence' ~ '\moff_leash\M' then 'true'
    else null
  end
$$;

-- ============================================================
-- 2. Register the new claim_keys in consensus_field_config
-- ============================================================

insert into public.consensus_field_config (field_group, claim_key, field_kind)
values
  ('dogs', 'has_on_leash',  'has_on_leash'),
  ('dogs', 'has_off_leash', 'has_off_leash')
on conflict (field_group, claim_key) do nothing;

-- ============================================================
-- 3. Extend _consensus_normalize for the new field_kinds
-- ============================================================

create or replace function public._consensus_normalize(p_field text, p_value text)
returns text language sql immutable as $$
  select case
    when p_value is null then null
    when lower(trim(p_value)) in ('unclear','unknown','null','none','') then null
    else case p_field
      when 'dogs_allowed' then case lower(trim(p_value))
        when 'yes' then 'yes' when 'no' then 'no'
        when 'restricted' then 'mixed' when 'seasonal' then 'mixed' when 'mixed' then 'mixed'
        else null end
      when 'leash_policy' then case lower(trim(p_value))
        when 'on_leash' then 'on_leash' when 'off_leash' then 'off_leash'
        when 'mixed' then 'mixed' when 'mixed_by_zone' then 'mixed'
        when 'required' then 'on_leash' when 'optional' then 'off_leash'
        when 'off_leash_ok' then 'off_leash'
        when 'varies_by_time' then 'mixed'
        else null end
      when 'off_leash_flag' then case lower(trim(p_value))
        when 'true' then 'true' when 'yes' then 'true' when '1' then 'true'
        when 'false' then 'false' when 'no' then 'false' when '0' then 'false'
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
      when 'has_on_leash' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false' else null end
      when 'has_off_leash' then case lower(trim(p_value))
        when 'true' then 'true' when 'false' then 'false' else null end
      else lower(trim(p_value))
    end
  end
$$;

-- ============================================================
-- 4. Extend compute_beach_field_consensus with derived claims CTE
-- ============================================================

create or replace function public.compute_beach_field_consensus(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint)
language plpgsql as $function$
declare
  ins_count bigint := 0; upd_count bigint := 0; beaches_count bigint := 0;
begin
  with claims_direct as (
    -- Existing path: pull claim_key directly from claimed_values
    select e.gold_fid, c.field_group, c.claim_key, c.field_kind, e.source,
           public._source_family(e.source) as family,
           coalesce(e.confidence, 0.5) as source_confidence,
           e.claimed_values->>c.claim_key as raw_value,
           public._consensus_normalize(c.field_kind, e.claimed_values->>c.claim_key) as norm_value
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
      join public.consensus_field_config c on c.field_group = e.field_group
     where e.gold_fid is not null
       and (p_fid is null or e.gold_fid = p_fid)
       and e.claimed_values ? c.claim_key
  ),
  claims_derived as (
    -- New path: derive has_on_leash / has_off_leash from each dogs source's
    -- existing claim shape. Each source emits up to two votes (one per
    -- binary). Abstains (NULL derivation) are filtered out. If the source
    -- has the literal claim_key already, prefer that — the lateral skips
    -- to avoid double-counting.
    select e.gold_fid, 'dogs'::text as field_group,
           d.claim_key, d.claim_key as field_kind,
           e.source,
           public._source_family(e.source) as family,
           coalesce(e.confidence, 0.5) as source_confidence,
           d.derived_value as raw_value,
           public._consensus_normalize(d.claim_key, d.derived_value) as norm_value
      from public.beach_enrichment_provenance e
      join public.beaches_gold g on g.fid = e.gold_fid
      cross join lateral (values
        ('has_on_leash',  public._derive_has_on_leash(e.claimed_values)),
        ('has_off_leash', public._derive_has_off_leash(e.claimed_values))
      ) as d(claim_key, derived_value)
     where e.field_group = 'dogs'
       and e.gold_fid is not null
       and (p_fid is null or e.gold_fid = p_fid)
       and d.derived_value is not null
       and not (e.claimed_values ? d.claim_key)
  ),
  claims as (
    select * from claims_direct
    union all
    select * from claims_derived
  ),
  weighted as (
    select c.*,
           c.source_confidence * public._calibration_weight(c.field_kind, c.source) as vote_weight
      from claims c
     where c.norm_value is not null
  ),
  family_votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value, family,
           max(vote_weight) as family_weight,
           (array_agg(jsonb_build_object(
             'source', source, 'weight', round(vote_weight::numeric, 3),
             'src_conf', round(source_confidence::numeric, 3)
           ) order by vote_weight desc))[1] as top_member,
           count(*) as n_in_family
      from weighted
     group by gold_fid, field_group, claim_key, field_kind, norm_value, family
  ),
  votes as (
    select gold_fid, field_group, claim_key, field_kind, norm_value,
           sum(family_weight) as bucket_weight,
           jsonb_agg(jsonb_build_object(
             'family', family, 'weight', round(family_weight::numeric, 3),
             'top_member', top_member, 'n_in_family', n_in_family
           ) order by family_weight desc) as voters
      from family_votes
     group by gold_fid, field_group, claim_key, field_kind, norm_value
  ),
  ranked as (
    select v.*,
           sum(bucket_weight) over (partition by gold_fid, claim_key) as total_weight,
           row_number() over (partition by gold_fid, claim_key order by bucket_weight desc) as rnk
      from votes v
  ),
  winners as (
    select gold_fid, field_group, claim_key, field_kind,
           norm_value as winning_value, bucket_weight as winner_weight, total_weight,
           (select bucket_weight from ranked r2 where r2.gold_fid=ranked.gold_fid
              and r2.claim_key=ranked.claim_key and r2.rnk=2 limit 1) as runner_up_weight,
           (select jsonb_object_agg(r3.norm_value,
              jsonb_build_object('weight', round(r3.bucket_weight::numeric,3), 'voters', r3.voters))
              from ranked r3 where r3.gold_fid=ranked.gold_fid
                            and r3.claim_key=ranked.claim_key) as breakdown,
           (select count(*)::int from ranked r4 where r4.gold_fid=ranked.gold_fid
              and r4.claim_key=ranked.claim_key) as bucket_count
      from ranked where rnk = 1
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
      set winning_value=excluded.winning_value, confidence=excluded.confidence,
          disagreement=excluded.disagreement, vote_breakdown=excluded.vote_breakdown,
          source_count=excluded.source_count, field_group=excluded.field_group,
          field_kind=excluded.field_kind, computed_at=now()
      where beach_field_consensus.winning_value is distinct from excluded.winning_value
         or beach_field_consensus.confidence is distinct from excluded.confidence
         or beach_field_consensus.disagreement is distinct from excluded.disagreement
         or beach_field_consensus.source_count is distinct from excluded.source_count
         or beach_field_consensus.vote_breakdown::text is distinct from excluded.vote_breakdown::text
    returning (xmax=0) as inserted, gold_fid
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted),
         count(distinct gold_fid)
    into ins_count, upd_count, beaches_count
    from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$function$;

commit;
