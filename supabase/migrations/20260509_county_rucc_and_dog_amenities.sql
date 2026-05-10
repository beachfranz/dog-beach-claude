-- 20260509_county_rucc_and_dog_amenities.sql
--
-- Two new signals for tier sub-segmentation (Franz, 2026-05-09):
--
--   1. RUCC code per county. USDA Rural-Urban Continuum Codes (1-9
--      scale). Loaded from a single CSV; one number per county.
--
--   2. Dog amenities (vets + pet shops) by point. Backs a
--      "dogs-active-catchment" signal: count of vets/pet-shops within
--      R miles of a beach. Loaded per-state from OSM Overpass.

begin;

-- ─── 1. RUCC on counties ────────────────────────────────────────────
alter table public.counties
  add column if not exists rucc_code int,
  add column if not exists rucc_label text,
  add column if not exists rucc_vintage int;

comment on column public.counties.rucc_code is
  'USDA Rural-Urban Continuum Code, 1-9. 1=metro 1M+, 9=rural <2.5K not adjacent to metro.';

create index if not exists counties_rucc_idx on public.counties (rucc_code);

-- ─── 2. dog_amenities ───────────────────────────────────────────────
create table if not exists public.dog_amenities (
  osm_id        bigint not null,
  osm_kind      text not null,                 -- 'node' | 'way' | 'relation'
  kind          text not null,                 -- 'veterinary' | 'pet_shop' | 'dog_park'
  raw_amenity   text,                          -- e.g. 'veterinary'
  raw_shop      text,                          -- e.g. 'pet'
  raw_leisure   text,                          -- e.g. 'dog_park'
  name          text,
  state         text,
  raw_tags      jsonb,
  geom          geometry(Point, 4326) not null,
  loaded_at     timestamptz not null default now(),
  primary key (osm_id, osm_kind)
);

create index if not exists dog_amenities_kind_idx
  on public.dog_amenities (kind);
create index if not exists dog_amenities_state_idx
  on public.dog_amenities (state);
create index if not exists dog_amenities_geom_gix
  on public.dog_amenities using gist (geom);

grant select on public.dog_amenities to anon, authenticated, service_role;

-- Bulk insert helper, mirrors the load_jurisdictions_batch pattern.
create or replace function public.load_dog_amenities_batch(p_batch jsonb)
returns int
language plpgsql
as $function$
declare
  v_count int := 0;
  rec jsonb;
begin
  for rec in select * from jsonb_array_elements(p_batch) loop
    insert into public.dog_amenities
      (osm_id, osm_kind, kind, raw_amenity, raw_shop, raw_leisure,
       name, state, raw_tags, geom)
    values (
      (rec->>'osm_id')::bigint,
      rec->>'osm_kind',
      rec->>'kind',
      rec->>'raw_amenity',
      rec->>'raw_shop',
      rec->>'raw_leisure',
      rec->>'name',
      rec->>'state',
      rec->'raw_tags',
      st_setsrid(st_makepoint((rec->>'lon')::float, (rec->>'lat')::float), 4326)
    )
    on conflict (osm_id, osm_kind) do update
      set kind        = excluded.kind,
          raw_amenity = excluded.raw_amenity,
          raw_shop    = excluded.raw_shop,
          raw_leisure = excluded.raw_leisure,
          name        = excluded.name,
          state       = excluded.state,
          raw_tags    = excluded.raw_tags,
          geom        = excluded.geom,
          loaded_at   = now();
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$function$;

grant execute on function public.load_dog_amenities_batch(jsonb) to service_role;

-- Convenience RPC: count dog amenities within R meters of a beach.
-- Uses ST_DWithin on the GIST index for speed; restrict by kind if given.
create or replace function public.dog_amenity_count_within(
  p_fid bigint,
  p_radius_m int,
  p_kind text default null
)
returns int
language sql stable security definer set search_path to public as $$
  select count(*)::int
    from public.dog_amenities a
    join public.beaches_gold g on g.fid = p_fid
   where st_dwithin(g.geom::geography, a.geom::geography, p_radius_m)
     and (p_kind is null or a.kind = p_kind);
$$;

grant execute on function public.dog_amenity_count_within(bigint, int, text) to anon, authenticated, service_role;

commit;
