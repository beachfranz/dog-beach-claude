-- Operator unification — Bucket B canonical filter patch.
--
-- The 2026-06-04 operator unification (Phases 1-13) merged the singular
-- `operator` table into `operators` with an `is_canonical` boolean flag.
-- Phase 3 remapped 8 FK columns to canonical IDs only, but 14 other FK
-- tables (osm_features, ccc_access_points, agency_aliases, etc.) still
-- hold tens of thousands of pointers to non-canonical bulk-pool rows.
--
-- Today's audit found 11 SQL functions that JOIN public.operators
-- without filtering is_canonical=true. The hot path is `resolve_agency` +
-- its dependents (`polygon_to_operator`, `canonical_agency_name`), which
-- are called by the `populate_*_gold` pipeline to attribute operators
-- during catalog ingest. Without the filter, fuzzy slug matches can return
-- non-canonical operator IDs which then get stored as the canonical
-- attribution downstream.
--
-- Comments in resolve_agency already SAY "only canonical" — but the
-- author thought `op.is_active AND op.parent_operator_id IS NULL` was the
-- canonical filter. It isn't. That's the bug.
--
-- This migration adds `is_canonical = true` to every operators JOIN site
-- in the 11 Bucket B functions. LEFT JOINs keep the parent row but blank
-- the operator columns when a non-canonical match would have surfaced.
-- INNER JOINs (resolve_agency's name lookups, polygon_to_operator's slug
-- lookups) become canonical-only — no result rather than non-canonical
-- result.
--
-- Bucket A (1 function, _operator_pair_safe) is safe (PK lookup, caller
-- passes specific ID). Bucket C (15 functions, the _op_pass* seeders,
-- run_operator_dedup, refresh_operator_*, fetch_*_for_extraction) operates
-- on the bulk pool intentionally and is NOT touched.

BEGIN;

-- ────────────────────────────────────────────────────────────────────
-- 1. resolve_agency — THE hot-path name resolver.
--    4 stages (exact / normalized / stripped / fuzzy) all need the filter.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.resolve_agency(
  p_alias text,
  p_alias_source text DEFAULT NULL::text,
  p_state_code text DEFAULT NULL::text,
  p_min_confidence numeric DEFAULT 0.65
)
 RETURNS TABLE(operator_id bigint, confidence numeric, method text)
 LANGUAGE plpgsql
 STABLE
AS $function$
declare
  v_norm text := public._normalize_agency_text(p_alias);
  v_stripped text := trim(regexp_replace(
    v_norm,
    '^(the |city of |town of |county of |state of |village of |borough of |township of )',
    '', 'i'
  ));
  v_stripped_with_county text := trim(regexp_replace(
    v_norm,
    ' (city|town|county|borough|township)$',
    '', 'i'
  ));
begin
  if p_alias is null or trim(p_alias) = '' then
    return;
  end if;

  -- Step a: exact alias match
  return query
    select aa.operator_id, 1.0::numeric, 'exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias = p_alias
       and op.is_canonical = true                  -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.parent_operator_id is null
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step b: normalized exact
  return query
    select aa.operator_id, 0.90::numeric, 'normalized_exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized = v_norm
       and op.is_canonical = true                  -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.parent_operator_id is null
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step c: stripped-variant exact
  if v_stripped <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped
         and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
         and op.is_active
         and op.parent_operator_id is null
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;
  if v_stripped_with_county <> v_norm then
    return query
      select aa.operator_id, 0.80::numeric, 'normalized_stripped'::text
        from public.agency_aliases aa
        join public.operators op on op.id = aa.operator_id
       where aa.alias_normalized = v_stripped_with_county
         and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
         and op.is_active
         and op.parent_operator_id is null
         and (p_state_code is null or op.state_code = p_state_code)
       order by aa.confidence desc
       limit 1;
    if found then return; end if;
  end if;

  -- Step d: trigram similarity
  perform set_limit(p_min_confidence::real);
  return query
    select aa.operator_id,
           similarity(aa.alias_normalized, v_norm)::numeric as sim,
           'fuzzy'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized % v_norm
       and op.is_canonical = true                  -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.parent_operator_id is null
       and (p_state_code is null or op.state_code = p_state_code)
     order by sim desc
     limit 1;
end $function$;

-- ────────────────────────────────────────────────────────────────────
-- 2. canonical_agency_name — alias → canonical name lookup.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.canonical_agency_name(p_type text, p_name text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE
AS $function$
declare alias_match text;
begin
  if p_name is null then return null; end if;

  select o.canonical_name
    into alias_match
    from public.agency_aliases a
    join public.operators o on o.id = a.operator_id
   where lower(a.alias) = lower(p_name)
     and o.is_canonical = true                     -- ★ Bucket B fix 2026-06-04
     and (
       o.level = p_type
       or replace(o.level, '-', '_') = p_type
       or replace(o.level, '_', '-') = p_type
     )
   limit 1;
  if alias_match is not null then return alias_match; end if;

  -- Fallback heuristics unchanged
  if p_type = 'state' then
    if p_name ~ '\m(SP|SB|SNR|SRA|SHP|SHM)\M' then
      return 'California Department of Parks and Recreation';
    end if;
    if p_name ~ '\m(SMR|SMCA|SMRMA)\M' then
      return 'California Department of Fish and Wildlife';
    end if;
  end if;

  if p_type = 'city' then
    if p_name ~* '^city of ' then return p_name; end if;
    if p_name ~* ', city of\s*$' then
      return 'City of ' || regexp_replace(p_name, ',\s*[Cc]ity\s+[Oo]f\s*$', '');
    end if;
    if p_name ~* '\mcity\M' or p_name ~* '\mdepartment\M'
       or p_name ~* '\moffice\M' or p_name ~* '\magency\M' then
      return p_name;
    end if;
    return 'City of ' || p_name;
  end if;

  if p_type = 'county' then
    if p_name ~* '^county of ' then return p_name; end if;
    if p_name ~* ', county of\s*$' then
      return 'County of ' || regexp_replace(p_name, ',\s*[Cc]ounty\s+[Oo]f\s*$', '');
    end if;
    if p_name ~* '\mcounty\M' or p_name ~* '\mdepartment\M'
       or p_name ~* '\moffice\M' or p_name ~* '\magency\M' then
      return p_name;
    end if;
    return 'County of ' || p_name;
  end if;

  return p_name;
end
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 3. polygon_to_operator — polygon → operator resolver (Bucket E → B).
--    7 site filters needed; resolve_agency calls inherit the filter.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.polygon_to_operator(
  p_polygon_kind text,
  p_claimed_values jsonb,
  p_state text
)
 RETURNS bigint
 LANGUAGE plpgsql
 STABLE
AS $function$
declare
  v_op_id bigint;
  v_polygon_id text;
  v_polygon_name text;
  v_loc_mang text;
  v_mng_name text;
begin
  if p_polygon_kind is null or p_claimed_values is null or p_state is null then
    return null;
  end if;
  v_polygon_id   := p_claimed_values->>'polygon_id';
  v_polygon_name := p_claimed_values->>'polygon_name';

  case p_polygon_kind
    when 'c1_city' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
      end if;

    when 'cdp' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'city'
           and jurisdiction_id = v_polygon_id::bigint
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
      end if;

    when 'county' then
      if v_polygon_id is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'county'
           and county_geoid = v_polygon_id
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
      end if;

    when 'pad_us_unit' then
      v_loc_mang := p_claimed_values->>'loc_mang';
      v_mng_name := p_claimed_values->>'mng_name';
      if (v_loc_mang is null or v_mng_name is null) and v_polygon_id is not null then
        select coalesce(v_loc_mang, pu.loc_mang),
               coalesce(v_mng_name, pu.mng_name)
          into v_loc_mang, v_mng_name
          from public.pad_us_units pu
         where pu.unit_id::text = v_polygon_id::text
           and pu.state = p_state
         limit 1;
      end if;

      -- (1) Try unit_name slug
      if v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
      end if;

      -- (2) Try loc_mang slug
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and slug = public.slugify_agency(v_loc_mang || ' (' || p_state || ')')
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
      end if;

      -- (3) operator_aliases on loc_mang
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select oa.canonical_operator_id into v_op_id
          from public.operator_aliases oa
          join public.operators o on o.id = oa.canonical_operator_id
         where o.state_code = p_state
           and o.is_canonical = true               -- ★ Bucket B fix 2026-06-04
           and o.is_active
           and (oa.alias_text = v_loc_mang
             or oa.pad_us_mng_name_variant = v_loc_mang)
         limit 1;
      end if;

      -- (4) pad_us_mng_name array containment on loc_mang
      if v_op_id is null and v_loc_mang is not null and trim(v_loc_mang) <> '' then
        select coalesce(o.parent_operator_id, o.id) into v_op_id
          from public.operators o
         where o.state_code = p_state
           and o.is_canonical = true               -- ★ Bucket B fix 2026-06-04
           and o.is_active
           and v_loc_mang = ANY(o.pad_us_mng_name)
         limit 1;
      end if;

      -- (5) Fuzzy fallback on unit_name (resolve_agency inherits the filter)
      if v_op_id is null and v_polygon_name is not null and trim(v_polygon_name) <> '' then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'polygon_pad_us', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'cpad_unit' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'cpad_unit', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'military_base' then
      if v_polygon_name is not null then
        select operator_id into v_op_id
          from public.resolve_agency(v_polygon_name, 'military_base', p_state, 0.65)
         order by confidence desc limit 1;
      end if;

    when 'tribal_land' then
      if v_polygon_name is not null then
        select coalesce(parent_operator_id, id) into v_op_id
          from public.operators
         where state_code = p_state
           and level = 'tribal'
           and slug = public.slugify_agency(v_polygon_name || ' (' || p_state || ')')
           and is_canonical = true                 -- ★ Bucket B fix 2026-06-04
           and is_active
         limit 1;
        if v_op_id is null then
          select operator_id into v_op_id
            from public.resolve_agency(v_polygon_name, 'tribal_land', p_state, 0.65)
           order by confidence desc limit 1;
        end if;
      end if;

    else
      v_op_id := null;
  end case;

  return v_op_id;
end $function$;

-- ────────────────────────────────────────────────────────────────────
-- 4. all_coastal_features_lite — LEFT JOIN: blank operator name on non-canonical.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.all_coastal_features_lite(p_counties text[] DEFAULT NULL::text[])
 RETURNS TABLE(layer text, origin_key text, name text, feature_type text, origin_source text, operator_canonical text, dogs_verdict text, description text, lat double precision, lng double precision)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
  select 'beach'::text,
         'osm/' || o.osm_type || '/' || o.osm_id::text as origin_key,
         o.name, o.feature_type, 'osm'::text,
         op.canonical_name,
         bv.dogs_verdict,
         null::text,
         st_y(o.geom), st_x(o.geom)
  from public.osm_features o
  left join public.operators op
         on op.id = o.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  left join public.beach_verdicts bv
    on bv.origin_key = 'osm/' || o.osm_type || '/' || o.osm_id::text
  where o.feature_type in ('beach','dog_friendly_beach')
    and (o.admin_inactive is null or o.admin_inactive = false)
    and (p_counties is null or o.county_name_tiger = any(p_counties))

  union all

  select 'beach',
         'ubp/' || u.fid::text as origin_key,
         u.name, 'beach', 'ubp',
         op.canonical_name,
         bv.dogs_verdict,
         null::text,
         st_y(u.geom), st_x(u.geom)
  from public.us_beach_points u
  left join public.operators op
         on op.id = u.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  left join public.beach_verdicts bv on bv.origin_key = 'ubp/' || u.fid::text
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and (p_counties is null or u.county_name_tiger = any(p_counties))

  union all

  select case when coalesce(c.inferred_type, '') in ('beach','named_beach') then 'beach'
              else 'access' end,
         'ccc/' || c.objectid::text as origin_key,
         c.name,
         coalesce(c.inferred_type, 'unknown'),
         'ccc',
         op.canonical_name,
         bv.dogs_verdict,
         c.description,
         st_y(c.geom), st_x(c.geom)
  from public.ccc_access_points c
  left join public.operators op
         on op.id = c.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  left join public.beach_verdicts bv on bv.origin_key = 'ccc/' || c.objectid::text
  where (c.archived is null or c.archived <> 'Yes')
    and (c.admin_inactive is null or c.admin_inactive = false)
    and c.latitude is not null
    and (p_counties is null or c.county_name_tiger = any(p_counties));
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 5. nearby_coastal_features — LEFT JOIN, blank on non-canonical.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.nearby_coastal_features(p_lat double precision, p_lng double precision, p_radius_m double precision DEFAULT 3000)
 RETURNS TABLE(layer text, origin_key text, name text, feature_type text, origin_source text, operator_canonical text, dogs_verdict text, description text, lat double precision, lng double precision, geojson jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
  select 'beach'::text,
         b.origin_key,
         b.name,
         b.feature_type,
         b.origin_source,
         op.canonical_name,
         null::text as dogs_verdict,
         null::text as description,
         st_y(b.geom) as lat,
         st_x(b.geom) as lng,
         coalesce(
           st_asgeojson(coalesce(b.geom_full, b.geom))::jsonb,
           st_asgeojson(b.geom)::jsonb
         ) as geojson
  from public.beach_locations b
  left join public.operators op
         on op.id = b.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  where st_dwithin(b.geom::geography,
                   st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography,
                   p_radius_m)

  union all

  select 'access'::text,
         a.origin_key,
         a.name,
         a.feature_type,
         a.origin_source,
         op.canonical_name,
         a.dogs_verdict,
         a.description,
         st_y(a.geom),
         st_x(a.geom),
         st_asgeojson(a.geom)::jsonb
  from public.beach_access_features a
  left join public.operators op
         on op.id = a.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  where st_dwithin(a.geom::geography,
                   st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography,
                   p_radius_m);
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 6/7/8. osm_beach_polys_with_operator_in_bbox / _in_county / sand_geojson
--        All LEFT JOIN operators; same fix.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.osm_beach_polys_with_operator_in_bbox(p_west double precision, p_south double precision, p_east double precision, p_north double precision)
 RETURNS TABLE(osm_type text, osm_id bigint, name text, feature_type text, operator_id bigint, operator_canonical text, operator_level text, operator_slug text, managing_agency_source text, label_lat double precision, label_lng double precision, geojson jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
  select f.osm_type, f.osm_id, f.name, f.feature_type,
         f.operator_id, op.canonical_name, op.level, op.slug,
         f.managing_agency_source,
         st_y(st_pointonsurface(f.geom_full)),
         st_x(st_pointonsurface(f.geom_full)),
         st_asgeojson(f.geom_full)::jsonb
  from public.osm_features f
  left join public.operators op
         on op.id = f.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  where f.feature_type in ('beach','dog_friendly_beach')
    and f.geom_full is not null
    and (f.admin_inactive is null or f.admin_inactive = false)
    and st_intersects(
          f.geom_full,
          st_makeenvelope(p_west, p_south, p_east, p_north, 4326)
        );
$function$;

CREATE OR REPLACE FUNCTION public.osm_beach_polys_with_operator_in_county(p_geoid text)
 RETURNS TABLE(osm_type text, osm_id bigint, name text, feature_type text, operator_id bigint, operator_canonical text, operator_level text, operator_slug text, managing_agency_source text, admin_inactive boolean, label_lat double precision, label_lng double precision, geojson jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
  select f.osm_type, f.osm_id, f.name, f.feature_type,
         f.operator_id, op.canonical_name, op.level, op.slug,
         f.managing_agency_source,
         coalesce(f.admin_inactive, false),
         st_y(st_pointonsurface(f.geom_full)),
         st_x(st_pointonsurface(f.geom_full)),
         st_asgeojson(f.geom_full)::jsonb
  from public.osm_features f
  left join public.operators op
         on op.id = f.operator_id
        and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
  cross join (select geom from public.counties where geoid = p_geoid) c
  where f.feature_type in ('beach','dog_friendly_beach')
    and f.geom_full is not null
    and st_intersects(f.geom_full, c.geom);
$function$;

CREATE OR REPLACE FUNCTION public.osm_beach_sand_geojson(p_county text DEFAULT 'Orange'::text)
 RETURNS TABLE(osm_type text, osm_id bigint, name text, name_source text, feature_type text, operator_id bigint, operator_name text, geom_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
  select o.osm_type, o.osm_id,
         coalesce(nullif(o.name, ''), borrowed.name) as name,
         case
           when o.name is not null and o.name <> '' then 'osm'
           when borrowed.name is not null            then 'borrowed_from_beach'
           else null
         end as name_source,
         o.feature_type,
         o.operator_id, op.canonical_name,
         st_asgeojson(st_simplifypreservetopology(o.geom_full, 0.0001))::jsonb
    from public.osm_features o
    left join public.operators op
           on op.id = o.operator_id
          and op.is_canonical = true             -- ★ Bucket B fix 2026-06-04
    left join lateral (
      select b.name,
             case when st_intersects(b.geom_full, o.geom_full) then 0 else 1 end as rank,
             st_distance(b.geom, o.geom) as dist
        from public.osm_features b
       where b.feature_type in ('beach','dog_friendly_beach')
         and b.name is not null and b.name <> ''
         and b.geom_full is not null
         and b.county_name_tiger = o.county_name_tiger
         and (b.osm_type, b.osm_id) <> (o.osm_type, o.osm_id)
         and (
           st_intersects(b.geom_full, o.geom_full)
           or st_dwithin(b.geom, o.geom, 0.002)
         )
       order by rank asc, dist asc
       limit 1
    ) borrowed on (o.name is null or o.name = '')
   where o.feature_type in ('beach','sand','dog_friendly_beach')
     and o.geom_full is not null
     and o.county_name_tiger = p_county
     and (o.admin_inactive is null or o.admin_inactive = false);
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 9. resolve_all_operator_ids — the FK-leak writer.
--    Every pass picks an operator to attach to a CCC/UBP/OSM row.
--    Without is_canonical, future runs accumulate fresh non-canonical FK
--    pointers. Filter the SELECT in each pass.
--    The final `update operators op set ccc_point_count = ...` row-count
--    refresh is NOT filtered — it intentionally counts across the whole pool.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.resolve_all_operator_ids()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
declare
  v_p1_ccc int := 0;  v_p1_ubp int := 0;  v_p1_osm int := 0;
  v_p2_osm int := 0;
  v_p3_ccc int := 0;  v_p3_ubp int := 0;  v_p3_osm int := 0;
  v_p3b_ccc int := 0; v_p3b_ubp int := 0; v_p3b_osm int := 0;
  v_p4_ccc int := 0;  v_p4_ubp int := 0;  v_p4_osm int := 0;
  v_dist jsonb;
begin
  -- Pass 1: CPAD smallest-containing polygon
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.cpad_units cu on st_contains(cu.geom, c2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where c2.operator_id is null and cu.mng_agncy is not null
     order by c2.objectid, st_area(cu.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where c.objectid = sub.id;
  get diagnostics v_p1_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.cpad_units cu on st_contains(cu.geom, u2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where u2.operator_id is null and cu.mng_agncy is not null
     order by u2.fid, st_area(cu.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where u.fid = sub.id;
  get diagnostics v_p1_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.cpad_units cu on st_contains(cu.geom, o2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where o2.operator_id is null and cu.mng_agncy is not null
     order by o2.osm_type, o2.osm_id, st_area(cu.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p1_osm = row_count;

  -- Pass 2: OSM operator tag (osm_features only)
  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.operators op on op.slug = public.slugify_agency(o2.tags->>'operator')
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where o2.operator_id is null
       and o2.tags ? 'operator' and o2.tags->>'operator' <> ''
     order by o2.osm_type, o2.osm_id, op.id
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'osm_tag'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p2_osm = row_count;

  -- Pass 3: TIGER place strict containment
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.jurisdictions j on st_contains(j.geom, c2.geom)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where c2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by c2.objectid, st_area(j.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where c.objectid = sub.id;
  get diagnostics v_p3_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.jurisdictions j on st_contains(j.geom, u2.geom)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where u2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by u2.fid, st_area(j.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where u.fid = sub.id;
  get diagnostics v_p3_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.jurisdictions j on st_contains(j.geom, o2.geom)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where o2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p3_osm = row_count;

  -- Pass 3-buffered: TIGER place ST_DWithin 200m
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.jurisdictions j
        on st_dwithin(j.geom::geography, c2.geom::geography, 200)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where c2.operator_id is null
       and (c2.archived is null or c2.archived <> 'Yes')
       and (c2.admin_inactive is null or c2.admin_inactive = false)
       and j.state = 'CA' and j.place_type like 'C%'
     order by c2.objectid, st_area(j.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where c.objectid = sub.id;
  get diagnostics v_p3b_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.jurisdictions j
        on st_dwithin(j.geom::geography, u2.geom::geography, 200)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where u2.operator_id is null and u2.state = 'CA'
       and (u2.admin_inactive is null or u2.admin_inactive = false)
       and j.state = 'CA' and j.place_type like 'C%'
     order by u2.fid, st_area(j.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where u.fid = sub.id;
  get diagnostics v_p3b_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.jurisdictions j on st_dwithin(j.geom, o2.geom_full, 0.002)
      join public.operators op on op.jurisdiction_id = j.id
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where o2.operator_id is null
       and o2.feature_type in ('beach','dog_friendly_beach')
       and (o2.admin_inactive is null or o2.admin_inactive = false)
       and o2.geom_full is not null
       and j.state = 'CA' and j.place_type like 'C%'
     order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p3b_osm = row_count;

  -- Pass 4: TIGER county fallback
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.counties co on st_contains(co.geom, c2.geom)
      join public.operators op on op.county_geoid = co.geoid
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where c2.operator_id is null
       and (c2.archived is null or c2.archived <> 'Yes')
       and (c2.admin_inactive is null or c2.admin_inactive = false)
       and co.geoid like '06%'
     order by c2.objectid, st_area(co.geom) asc
  ) update public.ccc_access_points cc
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where cc.objectid = sub.id;
  get diagnostics v_p4_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.counties c on st_contains(c.geom, u2.geom)
      join public.operators op on op.county_geoid = c.geoid
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where u2.operator_id is null and u2.state = 'CA'
       and (u2.admin_inactive is null or u2.admin_inactive = false)
       and c.geoid like '06%'
     order by u2.fid, st_area(c.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where u.fid = sub.id;
  get diagnostics v_p4_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.counties c on st_intersects(c.geom, o2.geom_full)
      join public.operators op on op.county_geoid = c.geoid
       and op.is_canonical = true               -- ★ Bucket B fix 2026-06-04
     where o2.operator_id is null
       and o2.feature_type in ('beach','dog_friendly_beach')
       and (o2.admin_inactive is null or o2.admin_inactive = false)
       and o2.geom_full is not null
       and c.geoid like '06%'
     order by o2.osm_type, o2.osm_id, st_area(c.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p4_osm = row_count;

  -- Refresh denormalized operator counts (NOT filtered — counts span the whole pool)
  update public.operators op set
    ccc_point_count   = (select count(*) from public.ccc_access_points where operator_id = op.id),
    usbeach_count     = (select count(*) from public.us_beach_points    where operator_id = op.id),
    osm_feature_count = (select count(*) from public.osm_features       where operator_id = op.id);

  select jsonb_object_agg(coalesce(src, 'null'), n)
    into v_dist
    from (
      select managing_agency_source as src, count(*) as n from public.us_beach_points
       where state = 'CA' and (admin_inactive is null or admin_inactive = false)
       group by 1
      union all
      select managing_agency_source as src, count(*) as n from public.ccc_access_points
       where (archived is null or archived <> 'Yes')
         and (admin_inactive is null or admin_inactive = false)
       group by 1
    ) x;

  return jsonb_build_object(
    'pass1_cpad',           jsonb_build_object('ccc', v_p1_ccc, 'ubp', v_p1_ubp, 'osm', v_p1_osm),
    'pass2_osm_tag',        jsonb_build_object('osm', v_p2_osm),
    'pass3_tiger_c1',       jsonb_build_object('ccc', v_p3_ccc, 'ubp', v_p3_ubp, 'osm', v_p3_osm),
    'pass3b_tiger_buffered',jsonb_build_object('ccc', v_p3b_ccc, 'ubp', v_p3b_ubp, 'osm', v_p3b_osm),
    'pass4_tiger_county',   jsonb_build_object('ccc', v_p4_ccc, 'ubp', v_p4_ubp, 'osm', v_p4_osm),
    'final_source_distribution', v_dist
  );
end;
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 10. state_operator_ids_for_scoreable_beaches — INNER JOIN, strict filter.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.state_operator_ids_for_scoreable_beaches(p_state text)
 RETURNS TABLE(operator_id bigint)
 LANGUAGE sql
 STABLE
AS $function$
  select distinct coalesce(o.parent_operator_id, o.id)
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
    join public.operators o on o.id = bep.operator_id
   where g.state = p_state
     and g.is_active
     and g.scoring_tier in ('daily','hourly')
     and bep.operator_id is not null
     and o.is_canonical = true;                  -- ★ Bucket B fix 2026-06-04
$function$;

-- ────────────────────────────────────────────────────────────────────
-- 11. state_operator_ids_with_beaches — 5 CTEs joining operators.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.state_operator_ids_with_beaches(p_state text)
 RETURNS TABLE(operator_id bigint)
 LANGUAGE sql
 STABLE
AS $function$
  with beach_counties as (
    select distinct g.county_fips as cfip
      from public.beaches_gold g
     where g.is_active and g.state = p_state and g.county_fips is not null
  ),
  bep_ops as (
    select distinct (bep.claimed_values->>'operator_id')::bigint as operator_id
      from public.beach_enrichment_provenance bep
      join public.beaches_gold g on g.fid = bep.gold_fid
     where g.is_active and g.state = p_state
       and bep.field_group = 'polygon_containment'
       and bep.claimed_values ? 'operator_id'
  ),
  spatial_ops as (
    select distinct op.id as operator_id
      from public.operators op
     where op.state_code = p_state
       and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.level in ('city','county','state','federal')
       and op.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_dwithin(op.geom::geography, g.geom::geography, 1000)
       )
  ),
  county_fips_ops as (
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state
       and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.level = 'county'
       and op.county_geoid in (select cfip from beach_counties)
  ),
  city_via_jurisdiction_ops as (
    select op.id as operator_id
      from public.operators op
      join public.jurisdictions j on j.id = op.jurisdiction_id
     where op.state_code = p_state
       and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.level = 'city'
       and j.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_intersects(j.geom, g.geom)
       )
  ),
  state_or_federal_level_ops as (
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state
       and op.is_canonical = true                -- ★ Bucket B fix 2026-06-04
       and op.is_active
       and op.level = 'state'
  ),
  union_all as (
    select operator_id from bep_ops
    union
    select operator_id from spatial_ops
    union
    select operator_id from county_fips_ops
    union
    select operator_id from city_via_jurisdiction_ops
    union
    select operator_id from state_or_federal_level_ops
  )
  select operator_id from union_all where operator_id is not null;
$function$;

COMMIT;
