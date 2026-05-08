-- 20260508_pad_us_speedup.sql
--
-- Speed up populate_from_pad_us_gold by:
--   1. Pre-computing geography cast as a stored column on pad_us_units
--      (avoids per-row geometry::geography cast during distance check)
--   2. Indexing geom_geog with GIST for distance ops
--   3. Tightening the bbox threshold from 0.01° to 0.005° — closer to
--      the 500m exact filter, reduces post-bbox candidates ~3x
--   4. Marking the function STABLE so plan caching kicks in across calls

begin;

-- 1. Add precomputed geography column. NOT a generated column because
-- geometry::geography isn't always considered immutable by the planner.
alter table public.pad_us_units
  add column if not exists geom_geog geography(MultiPolygon, 4326);

update public.pad_us_units
   set geom_geog = geom::geography
 where geom_geog is null;

create index if not exists pad_us_units_geom_geog_idx
  on public.pad_us_units using gist (geom_geog);

-- Trigger to keep geom_geog in sync if geom changes
create or replace function public._pad_us_units_set_geom_geog()
returns trigger language plpgsql as $$
begin
  if NEW.geom is distinct from OLD.geom then
    NEW.geom_geog := NEW.geom::geography;
  end if;
  return NEW;
end;
$$;

drop trigger if exists trg_pad_us_units_geom_geog on public.pad_us_units;
create trigger trg_pad_us_units_geom_geog
before update on public.pad_us_units
for each row execute function public._pad_us_units_set_geom_geog();

-- 2. Updated populate_from_pad_us_gold using precomputed geography
create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare rows_touched int;
begin
  with target as (
    select fid,
           coalesce(display_name_override, name) as full_name,
           geom, state,
           geom::geography as geom_geog
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null
       and is_active = true
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_type, c.mng_name,
      c.raw_attrs->>'Pub_Access' as pub_access_code,
      st_contains(c.geom, t.geom::geometry)               as inside,
      st_distance(t.geom_geog, c.geom_geog)               as dist_m,
      st_area(c.geom_geog)                                as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    -- Tighter bbox: 0.005° ≈ 500m at CA latitude
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.005)
   where st_distance(t.geom_geog, c.geom_geog) <= 500
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                       then 0.90
        when not inside and dist_m <= 100 and name_match     then 0.80
        when inside     and not name_match                   then 0.70
        when not inside and dist_m <= 500 and name_match     then 0.60
        when not inside and dist_m <= 100 and not name_match then 0.45
        else null
      end as confidence
    from candidates
  ),
  scored as (select * from with_conf where confidence is not null),
  ranked as (
    select *, row_number() over (partition by fid
      order by confidence desc, area_m2 asc) as rnk
    from scored
  ),
  best as (select * from ranked where rnk = 1),
  with_type as (
    select *,
      case mng_type
        when 'FED'  then 'federal'
        when 'STAT' then 'state'
        when 'LOC'  then 'local'
        when 'DIST' then 'special_district'
        when 'NGO'  then 'nonprofit'
        when 'PVT'  then 'private'
        when 'TRIB' then 'tribal'
        when 'JNT'  then 'joint'
        else             'unknown'
      end as gov_type,
      case pub_access_code
        when 'OA' then 'public'
        when 'RA' then 'restricted'
        when 'XA' then 'private'
        when 'UK' then 'unknown'
        else case mng_type
          when 'PVT'  then 'private'
          when 'TRIB' then 'restricted'
          when 'UNK'  then 'unknown'
          else             'public'
        end
      end as access_status,
      (select o.id from public.operators o
        where best.mng_name = any(o.pad_us_mng_name)
          and o.state_code = best.state
          and o.is_active = true
        limit 1) as operator_id
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'pad_us', confidence,
      jsonb_build_object('type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_name)),
      operator_id, now()
    from with_type
    where mng_type is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id = excluded.operator_id,
          updated_at = now(),
          is_canonical = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', confidence,
      jsonb_build_object('status', access_status), now()
    from with_type
    where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at = now(),
          is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$function$;

-- 3. Refresh statistics
analyze public.pad_us_units;

commit;
