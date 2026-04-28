-- Tier 1 fixes for the operators table:
--   A. slug-merge dups for "X, County of" form (all 58 CA counties had
--      a TIGER row + a CPAD-derived row that didn't merge). Extend
--      slugify to flip ", County of" suffix into " County" when the
--      remaining string doesn't already contain "County".
--   B. Replace "smallest CPAD wins" with "largest absolute overlap,
--      ≥50% coverage threshold" for the OSM polygon cascade. Beaches
--      whose CPAD overlap covers <50% fall through to TIGER place.
--   C. Pass 2 (OSM operator tag) now matches via aliases + canonical
--      name + osm_operator_strings, not just exact slugify match.
--
-- Existing manual overrides (managing_agency_source='manual') are
-- preserved throughout. CCC/UBP/locations_stage are point features —
-- centroid PIP stays correct for them; only osm_features re-resolves.


-- ── A1. Update slugify_agency to emit "X County" for ", County of" ──
create or replace function public.slugify_agency(p_name text) returns text
language sql immutable as $$
  with reordered as (
    select case
      when p_name ~* ', city of$' then
        'City of ' || regexp_replace(p_name, ',\s*city of\s*$', '', 'i')
      when p_name ~* ', county of$' then
        case
          when regexp_replace(p_name, ',\s*county of\s*$', '', 'i') ~* '\mcounty\M' then
            regexp_replace(p_name, ',\s*county of\s*$', '', 'i')
          else
            regexp_replace(p_name, ',\s*county of\s*$', '', 'i') || ' County'
        end
      when p_name ~* ', state of$' then
        regexp_replace(p_name, ',\s*state of\s*$', '', 'i')
      else p_name
    end as nm
  ),
  cleaned as (
    select lower(regexp_replace(nm, '[^a-zA-Z0-9 ]+', ' ', 'g')) as nm from reordered
  ),
  hyphened as (
    select regexp_replace(trim(cleaned.nm), '\s+', '-', 'g') as nm from cleaned
  )
  select nm from hyphened;
$$;


-- ── A2. Merge collision pairs: dup → canonical ──────────────────────
do $$
declare
  collision record;
  canonical_id bigint;
  dup_id       bigint;
  dup_aliases  text[];
  dup_osm      text[];
  dup_cpad     text;
  dup_cpad_lev text;
begin
  for collision in
    select public.slugify_agency(canonical_name) as new_slug,
           array_agg(id order by
             case when origin_source in ('tiger_places','tiger_counties') then 0
                  when origin_source in ('seed_federal','manual') then 1
                  else 2
             end,
             id) as ids
    from public.operators
    group by 1
    having count(*) > 1
  loop
    canonical_id := collision.ids[1];
    foreach dup_id in array collision.ids[2:array_length(collision.ids, 1)]
    loop
      select aliases, osm_operator_strings, cpad_agncy_name, cpad_agncy_level
        into dup_aliases, dup_osm, dup_cpad, dup_cpad_lev
        from public.operators where id = dup_id;

      update public.operators
         set cpad_agncy_name      = coalesce(cpad_agncy_name,  dup_cpad),
             cpad_agncy_level     = coalesce(cpad_agncy_level, dup_cpad_lev),
             aliases              = coalesce(
                                      (select array_agg(distinct a)
                                       from unnest(aliases || coalesce(dup_aliases, '{}'::text[])) a),
                                      '{}'::text[]),
             osm_operator_strings = coalesce(
                                      (select array_agg(distinct s)
                                       from unnest(osm_operator_strings || coalesce(dup_osm, '{}'::text[])) s),
                                      '{}'::text[]),
             updated_at           = now()
       where id = canonical_id;

      -- Re-point source-table FKs from dup → canonical
      update public.ccc_access_points  set operator_id = canonical_id where operator_id = dup_id;
      update public.us_beach_points    set operator_id = canonical_id where operator_id = dup_id;
      update public.osm_features       set operator_id = canonical_id where operator_id = dup_id;
      update public.locations_stage    set operator_id = canonical_id where operator_id = dup_id;

      -- Cache rows are derived; just delete dup's
      delete from public.operator_polygons_cache           where operator_id = dup_id;
      delete from public.operator_polygons_by_county_cache where operator_id = dup_id;

      delete from public.operators where id = dup_id;
    end loop;
  end loop;
end$$;


-- ── A3. Rebuild slugs across the table (in-place rename) ────────────
update public.operators set slug = public.slugify_agency(canonical_name);


-- ── C1. Curated aliases for vague OSM operator tags ─────────────────
-- "State of CA" / "CA US" appear on state-beach polygons; map to CDPR.
update public.operators
   set osm_operator_strings = (
     select array_agg(distinct s) from unnest(
       osm_operator_strings || array['State of CA','State of California','CA Parks','CA State Parks','Cal Parks','CA US']
     ) s
   )
 where slug = 'california-department-of-parks-and-recreation';

update public.operators
   set osm_operator_strings = (
     select array_agg(distinct s) from unnest(
       osm_operator_strings || array['CDFW','California Fish & Wildlife','Cal Fish and Wildlife','CA Fish & Wildlife']
     ) s
   )
 where slug = 'california-department-of-fish-and-wildlife';


-- ── B+C2. Clear non-manual OSM cascade attributions, re-run cascade ─
update public.osm_features
   set operator_id = null
 where coalesce(managing_agency_source, '') <> 'manual';


-- Pass 1: largest absolute overlap wins, ≥50% of OSM polygon area
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'cpad'
from (
  with intersections as (
    select o2.osm_type, o2.osm_id, op.id as op_id,
           st_area(st_intersection(st_makevalid(cu.geom), st_makevalid(o2.geom_full))::geography) as overlap_m2,
           st_area(o2.geom_full::geography) as osm_m2
    from public.osm_features o2
    join public.cpad_units cu on st_intersects(cu.geom, o2.geom_full)
    join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
    where o2.operator_id is null
      and o2.geom_full is not null
      and cu.mng_agncy is not null
      and coalesce(o2.managing_agency_source, '') <> 'manual'
  )
  select distinct on (osm_type, osm_id) osm_type, osm_id, op_id
  from intersections
  where overlap_m2 >= 0.5 * osm_m2
  order by osm_type, osm_id, overlap_m2 desc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;


-- Pass 2: OSM operator tag — match by slug OR aliases OR
-- osm_operator_strings OR canonical_name OR short_name
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'osm_tag'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.operators op on (
    op.slug = public.slugify_agency(o2.tags->>'operator')
    or (o2.tags->>'operator') = any(op.osm_operator_strings)
    or (o2.tags->>'operator') = any(op.aliases)
    or (o2.tags->>'operator') = op.canonical_name
    or (o2.tags->>'operator') = op.short_name
  )
  where o2.operator_id is null
    and o2.tags ? 'operator'
    and o2.tags->>'operator' <> ''
    and coalesce(o2.managing_agency_source, '') <> 'manual'
  order by o2.osm_type, o2.osm_id,
    case  -- prefer exact-slug match over alias
      when op.slug = public.slugify_agency(o2.tags->>'operator')          then 1
      when (o2.tags->>'operator') = op.canonical_name                     then 2
      when (o2.tags->>'operator') = any(op.osm_operator_strings)          then 3
      when (o2.tags->>'operator') = any(op.aliases)                       then 4
      else 5
    end,
    op.id
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;


-- Pass 3: TIGER place via polygon intersection (existing)
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.jurisdictions j on st_intersects(j.geom, o2.geom_full)
  join public.operators op on op.jurisdiction_id = j.id
  where o2.operator_id is null
    and o2.geom_full is not null
    and j.state = 'CA' and j.place_type like 'C%'
    and coalesce(o2.managing_agency_source, '') <> 'manual'
  order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;


-- Refresh denorm counts
update public.operators op set
  ccc_point_count   = (select count(*) from public.ccc_access_points where operator_id = op.id),
  usbeach_count     = (select count(*) from public.us_beach_points    where operator_id = op.id),
  osm_feature_count = (select count(*) from public.osm_features       where operator_id = op.id),
  cpad_unit_count   = (select count(*) from public.cpad_units cu where cu.mng_agncy = op.cpad_agncy_name);


-- Refresh polygon caches
select public.refresh_operator_polygons_cache();
select public.refresh_operator_polygons_for_county('06059');
select public.refresh_operator_polygons_for_county('06037');
