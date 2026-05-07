-- 20260507_beach_photos.sql
--
-- Photo cache for beaches. One row per (beach, source, image).
-- We don't proxy bytes — just store the source URL + license/
-- attribution metadata. Frontend links directly to the source
-- CDN. License terms (Mapillary = CC-BY-SA, Wikimedia = CC-BY-SA
-- or CC0, etc.) are tracked per-row so the renderer can show
-- the right attribution.
--
-- Sort order is set per-source by the loader; renderer picks the
-- top N by sort_order for each beach.

begin;

create table if not exists public.beach_photos (
  id              bigserial primary key,
  arena_group_id  bigint    not null references public.beaches_gold(fid) on delete cascade,
  source          text      not null
                  check (source in ('mapillary','wikimedia','ccc','nps','unsplash','manual')),
  external_id     text,                     -- Mapillary image_id, Wikipedia file name, etc.
  image_url       text      not null,       -- direct CDN URL we link to
  thumb_url       text,                     -- smaller preview if available
  attribution     text,                     -- "© Mapillary contributor / CC-BY-SA"
  license         text,                     -- 'CC-BY-SA','CC0','public-domain','state-govt'
  captured_at     timestamptz,              -- when the photo was taken (when known)
  width           int,
  height          int,
  lat             double precision,         -- photo's geo location (Mapillary)
  lng             double precision,
  distance_m      int,                      -- distance from beach centroid at capture time
  sort_order      int  default 100,         -- lower = preferred; renderer picks top N
  page_url        text,                     -- link back to source (e.g., Mapillary viewer)
  source_meta     jsonb,                    -- raw fields the loader wants to retain
  loaded_at       timestamptz default now(),
  unique (arena_group_id, source, external_id)
);

create index if not exists beach_photos_beach_idx
  on public.beach_photos(arena_group_id, sort_order);
create index if not exists beach_photos_source_idx
  on public.beach_photos(source);

-- RLS: anon can read (photos are public links); writes via service role only
alter table public.beach_photos enable row level security;
create policy beach_photos_anon_read on public.beach_photos
  for select to anon, authenticated using (true);

-- Extend get_beach_info to return up to 6 photos per beach
create or replace function public.get_beach_info(p_fid bigint)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  select jsonb_build_object(
    'beach', jsonb_build_object(
      'fid',           g.fid,
      'location_id',   g.location_id,
      'name',          g.name,
      'display_name',  coalesce(g.display_name_override, g.name),
      'county',        g.county_name,
      'state',         g.state,
      'address',       g.address,
      'website',       g.website,
      'timezone',      g.timezone,
      'lat',           st_y(g.geom),
      'lng',           st_x(g.geom),
      'geom',          st_asgeojson(g.geom)::jsonb,
      'open_time',     g.open_time,
      'close_time',    g.close_time,
      'is_scoreable',  g.is_scoreable
    ),
    'dog_policy', case when bdp.arena_group_id is not null then
      jsonb_build_object(
        'dogs_allowed',           bdp.dogs_allowed,
        'leash_policy',           bdp.leash_policy,
        'has_on_leash',           bdp.has_on_leash,
        'has_off_leash',          bdp.has_off_leash,
        'off_leash_flag',         bdp.off_leash_flag,
        'zone_rules',             bdp.zone_rules,
        'consensus_confidence',   bdp.consensus_confidence,
        'disagreement_flag',      bdp.disagreement_flag
      ) else null end,
    'amenities', case when ba.arena_group_id is not null then
      jsonb_build_object(
        'has_parking',         ba.has_parking,
        'parking_type',        ba.parking_type,
        'parking_notes',       ba.parking_notes,
        'has_restrooms',       ba.has_restrooms,
        'has_showers',         ba.has_showers,
        'has_picnic_area',     ba.has_picnic_area,
        'has_drinking_water',  ba.has_drinking_water,
        'has_food',            ba.has_food,
        'has_fire_pits',       ba.has_fire_pits,
        'has_lifeguards',      ba.has_lifeguards,
        'has_disabled_access', ba.has_disabled_access,
        'hours_text',          ba.hours_text
      ) else null end,
    'description', case when bd.arena_group_id is not null then
      jsonb_build_object(
        'text',         bd.description,
        'source',       bd.source,
        'generated_at', bd.generated_at
      ) else null end,
    'photos', coalesce((
      select jsonb_agg(jsonb_build_object(
        'source',      p.source,
        'image_url',   p.image_url,
        'thumb_url',   p.thumb_url,
        'attribution', p.attribution,
        'license',     p.license,
        'page_url',    p.page_url,
        'captured_at', p.captured_at
      ) order by p.sort_order, p.id)
      from public.beach_photos p
      where p.arena_group_id = g.fid
      limit 6
    ), '[]'::jsonb)
  )
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_amenities  ba  on ba.arena_group_id  = g.group_id
  left join public.beach_descriptions bd on bd.arena_group_id = g.fid
  where g.fid = p_fid and g.is_active
  limit 1;
$$;

grant execute on function public.get_beach_info(bigint)
  to anon, authenticated, service_role;

commit;
