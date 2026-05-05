-- 20260505_dog_policy_zones.sql
--
-- Spatial overlays that affect dog access at beaches. Generic schema —
-- holds wildlife critical habitat (snowy plover, least tern), explicit
-- pet-allowed carve-outs (NPS Point Reyes "dogs allowed at Kehoe Beach"),
-- pet-prohibited zones, and seasonal closures.
--
-- Authoritative ground truth that feeds the closed_zones_v3 prompt as
-- input context — when a beach's geom intersects a known plover critical-
-- habitat polygon, the prompt instruction shifts from "infer from prose"
-- to "this beach IS in WSP critical habitat — emit nesting_zones with
-- seasonal closure."

begin;

create table if not exists public.dog_policy_zones (
  id bigserial primary key,
  category text not null check (category in (
    'wildlife_critical_habitat',  -- triggers nesting_zones section + seasonal closure
    'pet_allowed_carveout',       -- explicit "dogs allowed here" inside larger prohibition
    'pet_prohibited_zone',        -- explicit "dogs prohibited here"
    'seasonal_closure'            -- time-bound restriction (non-wildlife)
  )),
  name text,
  species text,                                 -- nullable; for habitat zones
  source_agency text not null,                  -- USFWS | NPS | CDFW | CDPR
  source_url text,
  effective_dates jsonb,                        -- {"start":"MM-DD","end":"MM-DD"} or null
  notes text,
  geom geometry(Geometry, 4326) not null,
  area_m2 double precision,
  loaded_at timestamptz default now()
);
create index if not exists dog_policy_zones_geom_gix on public.dog_policy_zones using gist (geom);
create index if not exists dog_policy_zones_category_idx on public.dog_policy_zones (category);

-- Anon read for map rendering
alter table public.dog_policy_zones enable row level security;
drop policy if exists dog_policy_zones_anon_read on public.dog_policy_zones;
create policy dog_policy_zones_anon_read on public.dog_policy_zones
  for select to anon using (true);

-- Convenience view: which beaches overlap which zones
create or replace view public.beach_dog_policy_zones with (security_invoker = true) as
select g.fid, g.name as beach_name,
       z.id as zone_id, z.category, z.name as zone_name,
       z.species, z.source_agency, z.source_url, z.effective_dates, z.notes,
       st_area(st_intersection(g.geom, z.geom)::geography) as overlap_m2
  from public.beaches_gold g
  join public.dog_policy_zones z on st_intersects(g.geom, z.geom)
 where g.is_active;

grant select on public.beach_dog_policy_zones to anon, authenticated;

commit;
