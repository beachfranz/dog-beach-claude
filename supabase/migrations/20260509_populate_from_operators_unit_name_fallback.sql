-- 20260509_populate_from_operators_unit_name_fallback.sql
--
-- Pre-MA fix: populate_from_operators_gold's PAD-US arm tries
-- Loc_Mang then Loc_Own when resolving an operator. For federal
-- coastal-rec units (NPS NS, NWR, etc.) the *most specific* field is
-- pad_us_units.unit_name (e.g. "Cape Cod National Seashore"), which
-- matches the canonical_name of the operator seeded by Pass 3 of
-- populate_operators_for_state.
--
-- Loc_Mang is often a generic agency name ("NATIONAL PARK SERVICE")
-- that doesn't disambiguate which specific federal operator within
-- the state should receive the BEP attribution. unit_name does.
--
-- New lookup order: unit_name → Loc_Mang → Loc_Own.
-- All three pass state_code=g.state for single-state filtering.

begin;

create or replace function public.populate_from_operators_gold(p_fid bigint default null)
returns table(emitted_dogs int, emitted_practical int)
language plpgsql
as $function$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── PAD-US containment via resolver (unit_name → Loc_Mang → Loc_Own)
  with hits as (
    select distinct on (g.fid)
           g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      cross join lateral (
        -- unit_name is the most specific field for federal coastal-rec
        -- units; matches Pass-3-seeded operator canonical_name directly.
        select coalesce(
          (select operator_id from public.resolve_agency(
              pu.unit_name, 'pad_us_unit_name', g.state, 0.65) limit 1),
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Mang', 'pad_us_loc_mang', g.state, 0.65) limit 1),
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Own',  'pad_us_loc_own',  g.state, 0.65) limit 1)
        ) as resolved_op_id
      ) r
      join public.operators op
        on op.id = r.resolved_op_id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
     order by g.fid, st_area(pu.geom) asc
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_pad_us', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
                                  when leash_required is false then 'off_leash' end,
             'dogs_policy_notes', summary
           )),
           0.70, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs;

  -- County via county_fips
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.level = 'county' and op.county_geoid = g.county_fips and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs2 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_county', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
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

  -- City via jurisdictions PIP
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.jurisdictions j on st_contains(j.geom, g.geom) and j.place_type like 'C%'
      join public.operators op
        on op.level = 'city' and op.jurisdiction_id = j.id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs3 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_city', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'dogs_allowed', case default_rule when 'yes' then 'yes' when 'no' then 'no'
                                               when 'mixed' then 'mixed' when 'seasonal' then 'seasonal' end,
             'leash_policy', case when leash_required is true then 'on_leash'
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
end $function$;

grant execute on function public.populate_from_operators_gold(bigint) to anon, authenticated, service_role;

commit;
