-- 20260523_populate_from_operators_gold_towns.sql
--
-- Extend populate_from_operators_gold with a fourth block: town-level
-- BEP linkage via tiger_county_subdivisions containment. Mirrors the
-- existing city block ('City via jurisdictions PIP') for the C5/MCD
-- equivalent in town-strong states.
--
-- Per gap #46 — the picker bootstrap path. Without this block, the
-- new NH town operators (221 from gap #46 task 5) sit in the
-- operators table but never get BEP rows tagged with their
-- operator_id, so state_operator_ids_for_scoreable_beaches can't pick
-- them up for subsequent extraction runs.
--
-- Block emits 'operator_town' source rows at confidence 0.50 (matches
-- 'operator_city' confidence — same authority tier).

BEGIN;

CREATE OR REPLACE FUNCTION public.populate_from_operators_gold(p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(emitted_dogs integer, emitted_practical integer)
LANGUAGE plpgsql
AS $function$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── PAD-US containment via resolver (unit_name → Loc_Mang → Loc_Own) ──
  with hits as (
    select distinct on (g.fid)
           g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      cross join lateral (
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

  -- ── County via county_fips ─────────────────────────────────────────
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

  -- ── City via jurisdictions PIP ─────────────────────────────────────
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

  -- ── Town via tiger_county_subdivisions PIP ─────────────────────────
  -- NEW (gap #46). Mirrors the city block above. Joins via PIP
  -- containment on tiger_county_subdivisions (T1 active towns only).
  -- Operator → tcs linkage via slug match: tcs.name 'Wolfeboro' →
  -- slugify_agency('Town of Wolfeboro') = 'town-of-wolfeboro' = op.slug.
  -- Only fires when the town operator has an operator_dogs_policy row
  -- (i.e., extraction has run for it).
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.tiger_county_subdivisions tcs
        on tcs.state = g.state
       and tcs.classfp = 'T1'
       and st_contains(tcs.geom, g.geom)
      join public.operators op
        on op.level = 'town'
       and op.state_code = g.state
       and op.is_active
       and op.slug = public.slugify_agency('Town of ' || tcs.name)
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs4 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_town', source_url,
           -- Consensus-aligned claim keys: 'allowed' (not 'dogs_allowed')
           -- and 'leash_required' (not 'leash_policy'). The peer
           -- populator populate_from_city_dog_policy_gold uses these
           -- keys; populate_from_operators_gold's other blocks
           -- (operator_pad_us/county/city) use the WRONG keys and their
           -- BEP rows never register in consensus voting. Separate
           -- followup needed for those — this block fixes it for towns.
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case default_rule
                          when 'yes' then 'yes' when 'no' then 'no'
                          when 'mixed' then 'mixed' when 'seasonal' then 'yes'
                          else null end,
             'leash_required', case
                          when leash_required is true then 'on_leash'
                          when leash_required is false then 'off_leash'
                          else null end,
             'dogs_policy_notes', summary
           )),
           -- Confidence 0.90: in town-strong states (NH/MA/CT/ME/RI/VT/
           -- NJ/PA) the town IS the primary dog-policy authority and the
           -- tcs_town containment is a 1:1 governance fact (every beach
           -- sits in exactly one town's jurisdiction). Set above the
           -- 0.85 tier (pad_us per-unit, state_dogs_policy_v1) so town
           -- ordinance beats state-default RSA citation. Towns are more
           -- specific authority than state law.
           0.90, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_dogs from ins_dogs4;

  return query select v_dogs, v_pract;
end $function$;

COMMIT;
