-- 20260508_operator_dedup.sql
--
-- Move 2 (Pin #32): arena-style operator-table dedup pass.
--
-- Pattern matches tonight's beach dedup:
--   1. Pair-safety gate: same state, same level, trigram >= 0.85, geom overlap
--   2. Richness scorer: cross-references + activity counts
--   3. run_operator_dedup() picks winner, migrates FK refs, deactivates loser
--
-- The jurisdiction-mismatch guard (level must match) is the operator analog
-- of the dog-suffix guard for beaches: prevents "city of X" merging with
-- "county of X" even when names are otherwise identical.

begin;

-- ============================================================================
-- Pair safety gate
-- ============================================================================
create or replace function public._operator_pair_safe(p_a_id bigint, p_b_id bigint)
returns boolean
language sql
stable
as $$
  with a as (select * from public.operators where id = p_a_id),
       b as (select * from public.operators where id = p_b_id)
  select
    -- Both active
    a.is_active and b.is_active
    -- Same state
    and a.state_code = b.state_code
    -- Same level (city != county trap)
    and a.level = b.level
    -- High name similarity OR explicit cpad_agncy_name match
    and (
      similarity(lower(a.canonical_name), lower(b.canonical_name)) >= 0.85
      or (a.cpad_agncy_name is not null and a.cpad_agncy_name = b.cpad_agncy_name)
    )
    -- When both have geom, require overlap (Paradise vs Tahoe Paradise gate)
    and (
      a.geom is null or b.geom is null
      or ST_Intersects(a.geom, b.geom)
    )
  from a, b;
$$;

-- ============================================================================
-- Richness scorer
-- ============================================================================
create or replace function public._operator_richness_score(p_id bigint)
returns int
language sql
stable
as $$
  select
    (case when cpad_agncy_name is not null then 5 else 0 end)
    + (case when osm_operator_strings is not null and array_length(osm_operator_strings, 1) > 0 then 5 else 0 end)
    + (case when pad_us_mng_name is not null and array_length(pad_us_mng_name, 1) > 0 then 3 else 0 end)
    + (case when website is not null then 3 else 0 end)
    + (case when dog_policy_url is not null then 5 else 0 end)
    + (case when permits_url is not null then 2 else 0 end)
    + (case when phone is not null then 2 else 0 end)
    + (case when email is not null then 2 else 0 end)
    + least(coalesce(array_length(aliases, 1), 0), 5)
    + least(coalesce(cpad_unit_count, 0)
            + coalesce(ccc_point_count, 0)
            + coalesce(osm_feature_count, 0), 10)
    + (case when geom is not null then 2 else 0 end)
    + (case when footprint_area_km2 is not null then 1 else 0 end)
  from public.operators
   where id = p_id;
$$;

-- ============================================================================
-- Dedup orchestrator
-- ============================================================================
create or replace function public.run_operator_dedup()
returns table(clusters_processed bigint, kills bigint, fk_refs_migrated bigint)
language plpgsql
security definer
as $function$
declare
  v_processed bigint := 0;
  v_kills bigint := 0;
  v_fk_migrated bigint := 0;
  v_pair record;
begin
  -- Identify candidate pairs (transitive clustering not yet — handle 1:1 pairs first)
  create temp table _op_candidates on commit drop as
  select a.id as a_id, b.id as b_id,
         public._operator_richness_score(a.id) as a_score,
         public._operator_richness_score(b.id) as b_score
    from public.operators a
    join public.operators b on b.id > a.id
   where public._operator_pair_safe(a.id, b.id);

  -- For each pair, lower-richness loses to higher-richness. Tie → lower id wins.
  create temp table _op_decisions on commit drop as
  select case when a_score >= b_score then a_id else b_id end as winner_id,
         case when a_score >= b_score then b_id else a_id end as loser_id
    from _op_candidates;

  -- Resolve transitively: if loser was supposed to be a winner elsewhere, follow chain
  -- (Simple 2-step resolution — most cases are 1:1 anyway)
  update _op_decisions d
     set winner_id = d2.winner_id
    from _op_decisions d2
   where d2.loser_id = d.winner_id
     and d2.winner_id <> d.loser_id;

  v_processed := (select count(*) from _op_decisions);

  -- Migrate FK refs from loser → winner

  -- 1. operator_dogs_policy
  with applied as (
    update public.operator_dogs_policy odp
       set operator_id = d.winner_id
      from _op_decisions d
     where odp.operator_id = d.loser_id
       and not exists (select 1 from public.operator_dogs_policy odp2
                        where odp2.operator_id = d.winner_id)
    returning 1
  )
  select v_fk_migrated + count(*) into v_fk_migrated from applied;

  -- 2. operator_policy_exceptions (if exists)
  if exists(select 1 from information_schema.tables
             where table_schema='public' and table_name='operator_policy_exceptions') then
    with applied as (
      update public.operator_policy_exceptions ope
         set operator_id = d.winner_id
        from _op_decisions d
       where ope.operator_id = d.loser_id
      returning 1
    )
    select v_fk_migrated + count(*) into v_fk_migrated from applied;
  end if;

  -- 3. beach_enrichment_provenance
  with applied as (
    update public.beach_enrichment_provenance bep
       set operator_id = d.winner_id
      from _op_decisions d
     where bep.operator_id = d.loser_id
    returning 1
  )
  select v_fk_migrated + count(*) into v_fk_migrated from applied;

  -- 4. operator_amenity_claims (if exists)
  if exists(select 1 from information_schema.tables
             where table_schema='public' and table_name='operator_amenity_claims') then
    with applied as (
      update public.operator_amenity_claims oac
         set operator_id = d.winner_id
        from _op_decisions d
       where oac.operator_id = d.loser_id
      returning 1
    )
    select v_fk_migrated + count(*) into v_fk_migrated from applied;
  end if;

  -- Deactivate losers
  with applied as (
    update public.operators o
       set is_active = false,
           inactive_reason = 'dupe_of_'||d.winner_id::text
      from _op_decisions d
     where o.id = d.loser_id
       and o.is_active = true
    returning 1
  )
  select count(*) into v_kills from applied;

  return query select v_processed, v_kills, v_fk_migrated;
end;
$function$;

grant execute on function public._operator_pair_safe(bigint, bigint) to anon, authenticated, service_role;
grant execute on function public._operator_richness_score(bigint) to anon, authenticated, service_role;
grant execute on function public.run_operator_dedup() to anon, authenticated, service_role;

commit;
