-- 20260509_populate_from_operators_pad_us_match.sql
--
-- Drift fix: populate_from_operators_gold's PAD-US join was matching
-- pu.mng_name against op.canonical_name. But PAD-US mng_name in OR/WA
-- is a 3-4 letter agency CODE ('SPR', 'BLM', 'CITY', 'CNTY', 'USFS'),
-- not the full agency name. The full name lives in raw_attrs->>'Loc_Mang'
-- (and 'Loc_Own' as fallback).
--
-- This migration rewrites the PAD-US arm of populate_from_operators_gold
-- to join on the raw_attrs full names instead. Result: state-agency
-- evidence (OPRD, Washington State Parks, ODFW, WDFW, etc.) now flows
-- to beaches contained in their PAD-US polygons.
--
-- The county and city paths (also in this function) are unchanged — they
-- already match correctly via county_geoid and jurisdictions PIP.

begin;

create or replace function public.populate_from_operators_gold(p_fid bigint default null)
returns table(emitted_dogs int, emitted_practical int)
language plpgsql
as $$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── State/Federal via PAD-US containment, joining on raw_attrs ────
  -- raw_attrs->>'Loc_Mang' has the full agency name when known; falls
  -- back to Loc_Own. mng_name is a code and not useful for join.
  with hits as (
    -- distinct on (gold_fid) — a beach can be inside multiple PAD-US
    -- polygons that resolve to the same operator; pick the one with the
    -- smallest polygon area (tightest containment) for canonical evidence.
    select distinct on (g.fid)
           g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      join public.operators op
        on (
            op.canonical_name = pu.raw_attrs->>'Loc_Mang'
         or op.canonical_name = pu.raw_attrs->>'Loc_Own'
         or pu.raw_attrs->>'Loc_Mang' = any(op.aliases)
         or pu.raw_attrs->>'Loc_Own'  = any(op.aliases)
           )
       and op.is_active
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

-- ── Add common PAD-US name variations as aliases on OR/WA state ops ──
-- PAD-US Loc_Mang field is somewhat normalized but has minor variations
-- (presence/absence of 'and', ampersand vs 'and', truncations). Add the
-- known variants so the join above catches them.

update public.operators set aliases = (
  select array_agg(distinct a) from unnest(aliases || array[
    'Oregon Parks and Recreation Department',
    'Oregon Parks Recreation Department',
    'Oregon Parks & Recreation Department',
    'OPRD',
    'Oregon State Parks',
    'Oregon Parks',
    'OREGON PARKS AND RECREATION DEPARTMENT'
  ]) a
) where slug = 'oregon-parks-recreation-department';

update public.operators set aliases = (
  select array_agg(distinct a) from unnest(aliases || array[
    'Washington State Parks and Recreation Commission',
    'Washington State Parks',
    'Washington State Parks Recreation Commission',
    'WA State Parks',
    'WSPRC',
    'WASHINGTON STATE PARKS AND RECREATION COMMISSION'
  ]) a
) where slug = 'washington-state-parks';

update public.operators set aliases = (
  select array_agg(distinct a) from unnest(aliases || array[
    'Oregon Department of Fish and Wildlife',
    'Oregon Department of Fish & Wildlife',
    'Oregon Fish and Wildlife',
    'ODFW',
    'OREGON DEPARTMENT OF FISH AND WILDLIFE'
  ]) a
) where slug = 'oregon-department-fish-wildlife';

update public.operators set aliases = (
  select array_agg(distinct a) from unnest(aliases || array[
    'Washington Department of Fish and Wildlife',
    'Washington Department of Fish & Wildlife',
    'WDFW',
    'WASHINGTON DEPARTMENT OF FISH AND WILDLIFE'
  ]) a
) where slug = 'washington-department-fish-wildlife';

update public.operators set aliases = (
  select array_agg(distinct a) from unnest(aliases || array[
    'Washington Department of Natural Resources',
    'Washington State Department of Natural Resources',
    'WA DNR', 'WDNR', 'DNR',
    'WASHINGTON DEPARTMENT OF NATURAL RESOURCES'
  ]) a
) where slug = 'washington-department-natural-resources';

commit;
