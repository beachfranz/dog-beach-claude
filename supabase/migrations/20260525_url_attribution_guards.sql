-- 20260525_url_attribution_guards.sql
--
-- URL-attribution bug fix per investigation 2026-05-25 LATE. Task #42.
--
-- Bug chain found:
--   1. Upstream operator-extraction stored beach-specific URLs in
--      operators.dog_policy_url for COUNTY-LEVEL operators
--      (e.g. Barnstable County operator's dog_policy_url =
--      barnstable.gov/SandyNeckPark/SNK%20policies.pdf — a SINGLE
--      beach's PDF, treated as county-wide policy).
--   2. populate_from_county_dog_policy_gold + populate_from_city_dog_policy_gold
--      faithfully broadcast that URL to every beach in the county/city,
--      writing 388 BEP rows for Barnstable beaches all pointing at SNK PDF.
--      Same for Plymouth Long Beach (130), Joseph Sylvia State Beach (89),
--      Nantucket Jetties (36), Capistrano Beach → Seal Beach, etc.
--   3. Consensus engine picks the broadcast row as canonical because
--      most beaches have no competing per-beach source.
--
-- Fixes in this migration:
--   A. Helper fn `_url_slug_specific_beach_mismatch(url, beach_name)` —
--      returns true if URL's slug names a SPECIFIC beach that doesn't
--      match the joined beach. Heuristic: slug contains 'beach' or
--      named-place pattern AND similarity to beach_name < 0.30.
--   B. Patch both populators to skip rows where the guard fires.
--   C. Add city-takes-priority guard to county populator (city-incorporated
--      beaches don't get county-level fallback unless they have no city op).
--   D. Mark existing canonical rows as relevance_verified=false when
--      source_url is in the fanout blacklist (lowered threshold 50 → 15).
--   E. Re-fire populators (drops bad broadcasts) + recompute consensus.

begin;

-- ─── A. Slug-mismatch detector ──────────────────────────────────────────

create or replace function public._url_slug_specific_beach_mismatch(
  p_url        text,
  p_beach_name text
) returns boolean
language sql immutable
as $$
  with parts as (
    select
      -- extract last URL path segment (strip query + trailing slash)
      lower(regexp_replace(
        regexp_replace(coalesce(p_url, ''), '\?.*$', ''),
        '.*/([^/]+)/?$', '\1'
      )) as slug,
      lower(regexp_replace(coalesce(p_beach_name, ''), '[^a-zA-Z0-9]+', '-', 'g')) as name_kebab
  )
  select
    p_url is not null
    -- slug looks like a specific beach (contains 'beach' or is a beach-name pattern)
    and (slug ~ 'beach' or slug ~ '[a-z]{3,}-[a-z]{3,}')
    -- exclude generic agency catalog pages
    and slug !~ '^(pets|pet-friendly|parks-trails-pets|rules|regulations|policies|policy|index|home|main|faqs|campgrounds|recreation|news-center|news|state-park-and-state-beach-rules|dogs-in-dcr-parks|dog-off-leash-areas|seasonal|state-park|park-rules)$'
    -- and doesn't match this beach's name
    and similarity(name_kebab, slug) < 0.30
  from parts;
$$;

comment on function public._url_slug_specific_beach_mismatch is
  'Returns true if URL''s slug names a SPECIFIC beach that doesn''t match the joined beach. Used as a guard in city/county policy populators to prevent SNK / Plymouth / Capistrano-style broadcasts.';

-- ─── B+C. Patch county populator: slug guard + city-priority ────────────

create or replace function public.populate_from_county_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.county_geoid = g.county_geoid
       and op.level = 'county'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.county_geoid is not null
       -- GUARD (C): skip beaches that ALSO have a city-level operator with policy.
       -- City jurisdiction wins inside cities — county is fallback only.
       and not exists (
         select 1 from public.operators op_city
           join public.operator_dogs_policy odp_city
             on odp_city.operator_id = op_city.id
            and odp_city.policy_found = true
          where op_city.level = 'city'
            and op_city.jurisdiction_id = g.c1_jurisdiction_id
       )
       -- GUARD (B): skip when source URL slug is specific-beach-shaped
       -- and doesn't match this beach's name.
       and not public._url_slug_specific_beach_mismatch(
             coalesce(odp.source_url, op.dog_policy_url), g.name
           )
  ),
  built as (
    select fid, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'county_policy',
      coalesce(op_conf, 0.85),
      v, coalesce(source_url, dog_policy_url), now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

-- ─── B. Patch city populator: slug guard ────────────────────────────────

create or replace function public.populate_from_city_dog_policy_gold(
  p_fid bigint default null
) returns integer language plpgsql as $$
declare rows_touched int;
begin
  with matches as (
    select g.fid, g.name as beach_name,
           op.canonical_name as operator_name, op.dog_policy_url, op.id as op_id,
           odp.default_rule, odp.leash_required, odp.summary,
           odp.area_sand, odp.area_water, odp.area_picnic_area,
           odp.area_parking_lot, odp.area_trails, odp.area_campground,
           odp.designated_dog_zones, odp.prohibited_areas,
           odp.time_windows, odp.seasonal_closures, odp.spatial_zones,
           coalesce(odp.pass_b_confidence, odp.pass_a_confidence) as op_conf,
           odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.jurisdiction_id = g.c1_jurisdiction_id
       and op.level = 'city'
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id
       and odp.policy_found = true
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.c1_jurisdiction_id is not null
       -- GUARD (B): skip when source URL slug is specific-beach-shaped
       -- and doesn't match this beach's name.
       and not public._url_slug_specific_beach_mismatch(
             coalesce(odp.source_url, op.dog_policy_url), g.name
           )
  ),
  built as (
    select fid, operator_name, dog_policy_url, op_conf, source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case
               when default_rule in ('yes','allowed','restricted','seasonal') then 'yes'
               when default_rule in ('no','prohibited','not_allowed') then 'no'
               else null end,
             'leash_required', case
               when leash_required is true  then 'on_leash'
               when leash_required is false then 'off_leash' else null end,
             'notes', summary,
             'area_sand',         area_sand,
             'area_water',        area_water,
             'area_picnic_area',  area_picnic_area,
             'area_parking_lot',  area_parking_lot,
             'area_trails',       area_trails,
             'area_campground',   area_campground,
             'designated_dog_zones', designated_dog_zones,
             'prohibited_areas',     prohibited_areas,
             'time_windows',         time_windows,
             'seasonal_closures',    seasonal_closures,
             'operator_name',        operator_name
           )) as v
      from matches
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select fid, 'dogs', 'city_policy',
      coalesce(op_conf, 0.85),
      v, coalesce(source_url, dog_policy_url), now()
    from built where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from upserted;
  return rows_touched;
end;
$$;

-- ─── D. Lower fanout threshold (50 → 15) for relevance_verified ─────────
-- Mark URLs that appear on >15 beaches as unverified — they're broadcast
-- candidates that should be re-evaluated by per-beach extraction.

with fanout_blacklist as (
  select source_url
    from public.beach_enrichment_provenance
   where field_group = 'dogs' and source_url is not null and is_canonical
   group by source_url
  having count(distinct gold_fid) > 15
)
update public.beach_enrichment_provenance bep
   set relevance_verified = false
  from fanout_blacklist b
 where bep.source_url = b.source_url
   and bep.field_group = 'dogs'
   and bep.is_canonical
   and bep.relevance_verified is not false;

-- ─── E. Demote bad-attribution rows whose URL slug fires the guard ──────
-- Existing rows in BEP that violate the slug-mismatch guard get demoted
-- to is_canonical=false. The populator re-fire won't bring them back
-- (guard blocks). Consensus will pick the next-best provenance for these
-- beaches (state_dogs_policy_v1 typically).

update public.beach_enrichment_provenance bep
   set is_canonical = false,
       relevance_verified = false,
       notes = coalesce(notes,'') || ' [DEMOTED 2026-05-25: URL slug names specific other beach. See task #42.]'
  from public.beaches_gold g
 where g.fid = bep.gold_fid
   and bep.field_group = 'dogs'
   and bep.is_canonical
   and public._url_slug_specific_beach_mismatch(bep.source_url, g.name);

commit;
