-- 20260508_populate_from_operators_generic.sql
--
-- State-agnostic operator → BEP populator. populate_from_park_operators_gold
-- (the existing CA path) joins csp_parks + park_operators which are CA-only;
-- OR/WA need a path that uses PAD-US (state/federal), TIGER counties
-- (county-level), and TIGER places / jurisdictions (city-level) to find
-- the relevant operator for a beach.
--
-- For each (beach, level) lookup:
--   1. State/Federal: beach point inside pad_us_unit; map pad_us_unit.mng_name
--      → operators.canonical_name OR alias. Use that operator's
--      operator_dogs_policy to emit BEP rows.
--   2. County: beach.county_fips → operators where level='county' and
--      county_geoid = '<fips_state><county_fips>'. Same emit.
--   3. City: beach contained in a jurisdictions polygon → operators where
--      level='city' and jurisdiction_id = j.id. Same emit.
--
-- This complements (does not replace) populate_from_park_operators_gold;
-- both can run in promote_to_gold and emit independently into BEP.

begin;

create or replace function public.populate_from_operators_gold(p_fid bigint default null)
returns table(emitted_dogs int, emitted_practical int)
language plpgsql
as $$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── State/Federal via PAD-US containment ───────────────────────────
  with hits as (
    select g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      join public.operators op
        on (op.canonical_name = pu.mng_name
            or pu.mng_name = any(op.aliases))
       and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_pad_us',
           source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed',  case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                                when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy',  case when leash_required is true then 'on_leash'
                                   when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.70, false, 'derived_url_crawl', 'beach_access', now()
      from hits
     where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs;

  -- ── County via county_fips → operators(level=county) ───────────────
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.level = 'county'
       and op.county_geoid = g.county_fips
       and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs2 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_county',
           source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed',  case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                                when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy',  case when leash_required is true then 'on_leash'
                                   when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.55, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs2;

  -- ── City via jurisdictions PIP → operators(level=city) ─────────────
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.jurisdictions j
        on st_contains(j.geom, g.geom)
       and j.place_type like 'C%'
      join public.operators op
        on op.level = 'city'
       and op.jurisdiction_id = j.id
       and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs3 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_city',
           source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed',  case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                                when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy',  case when leash_required is true then 'on_leash'
                                   when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.50, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs3;

  return query select v_dogs, v_pract;
end $$;

grant execute on function public.populate_from_operators_gold(bigint) to anon, authenticated, service_role;

commit;
