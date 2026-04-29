-- Pre-computed origin_key → smallest containing CPAD unit_id map.
-- Powers fast beach-level CPAD policy lookups in the viewer/map RPCs.
-- One row per beach_locations entry that sits inside any CPAD unit.
-- Refresh manually after material changes to beach_locations or
-- cpad_units (rare): truncate + insert.

drop table if exists public.cpad_unit_for_beach cascade;
create table public.cpad_unit_for_beach (
  origin_key   text primary key,
  beach_name   text,
  beach_county text,
  lat          double precision,
  lng          double precision,
  unit_id      integer,
  unit_area_m2 double precision,
  computed_at  timestamptz not null default now()
);

create index cpad_unit_for_beach_unit_idx     on public.cpad_unit_for_beach (unit_id);
create index cpad_unit_for_beach_county_idx   on public.cpad_unit_for_beach (beach_county);

-- Initial population (idempotent — re-run safely).
-- Source = beach_locations UNION all_coastal_features_lite() rows that
-- represent named CCC sandy beaches but are dropped by the 805 dedupe
-- (Rosie's, Huntington Beach Dog Beach, etc. are in CCC but not the
-- deduped view because their same_beach partners take precedence).
with universe as (
  select bl.origin_key, bl.name, bl.geom from public.beach_locations bl
  union
  select 'ccc/' || c.objectid::text as origin_key,
         c.name,
         c.geom
    from public.ccc_access_points c
   where (c.archived is null or c.archived <> 'Yes')
     and (c.admin_inactive is null or c.admin_inactive = false)
     and c.latitude is not null
     and c.sandy_beach = 'Yes'
     and c.inferred_type = 'beach'
     and 'ccc/' || c.objectid::text not in (select origin_key from public.beach_locations)
)
insert into public.cpad_unit_for_beach (origin_key, beach_name, beach_county, lat, lng, unit_id, unit_area_m2)
  select u.origin_key,
         u.name,
         (select c.name from public.counties c
            where st_intersects(c.geom, u.geom) limit 1) as beach_county,
         st_y(u.geom)::float8,
         st_x(u.geom)::float8,
         cu.unit_id,
         cu.area_m2
    from universe u
    left join lateral (
      select cu2.unit_id, st_area(cu2.geom::geography) as area_m2
        from public.cpad_units cu2
       where st_contains(cu2.geom, u.geom)
       order by st_area(cu2.geom) asc
       limit 1
    ) cu on true;


-- Rewrite beach_level_dogs_geojson to use the pre-computed mapping.
create or replace function public.beach_level_dogs_geojson(
  p_counties text[]
) returns table (
  origin_key            text,
  beach_name            text,
  county                text,
  cpad_unit_id          integer,
  cpad_unit_name        text,
  agency_name           text,
  layer                 text,
  dogs_allowed          text,
  default_rule          text,
  leash_required        boolean,
  area_sand             text,
  area_water            text,
  area_picnic_area      text,
  area_parking_lot      text,
  area_trails           text,
  area_campground       text,
  designated_dog_zones  text,
  prohibited_areas      text,
  source_quote          text,
  url_used              text,
  lat                   float8,
  lng                   float8
) language sql stable security definer as $$
  select
    m.origin_key,
    m.beach_name,
    m.beach_county,
    p.cpad_unit_id,
    p.unit_name as cpad_unit_name,
    p.agency_name,
    cu.layer,
    p.dogs_allowed,
    p.default_rule,
    p.leash_required,
    p.area_sand, p.area_water, p.area_picnic_area,
    p.area_parking_lot, p.area_trails, p.area_campground,
    p.designated_dog_zones, p.prohibited_areas,
    p.source_quote, p.url_used,
    m.lat, m.lng
  from public.cpad_unit_for_beach m
  left join public.cpad_units cu on cu.unit_id = m.unit_id
  left join public.cpad_unit_dogs_policy p on p.cpad_unit_id = m.unit_id
  where m.beach_county = any(p_counties);
$$;

grant select on public.cpad_unit_for_beach to anon, authenticated;
grant execute on function public.beach_level_dogs_geojson(text[]) to anon, authenticated;
