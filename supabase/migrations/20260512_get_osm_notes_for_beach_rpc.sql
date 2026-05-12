-- 20260512_get_osm_notes_for_beach_rpc.sql
--
-- Returns OSM `note` / `description` text for beach-relevant features
-- near a given beach. Used by generate_beach_descriptions.py to surface
-- crowd-voice nuance (e.g. way/370756170 carries the line "An whole
-- glorious mile of Southern California beach where dogs can run off
-- leash" for Huntington Dog Beach).
--
-- Filters:
--   - within p_radius_m metres of the beach point (default 500m)
--   - feature has at least one of: leisure, natural, place, tourism
--     (excludes amenity nodes, highways, shops — noise)
--   - feature has at least one of: note, description, note:en, description:en
--   - feature is active (not abandoned by OSM)
--
-- Returned ordered by distance, capped at p_limit (default 5).

begin;

drop function if exists public.get_osm_notes_for_beach(bigint, integer, integer);
create or replace function public.get_osm_notes_for_beach(
  p_fid bigint,
  p_radius_m integer default 500,
  p_limit integer default 5
)
returns table(
  osm_type text,
  osm_id   bigint,
  name     text,
  kind     text,
  note     text,
  description text,
  distance_m integer
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $$
  select o.type as osm_type,
         o.id   as osm_id,
         coalesce(o.tags->>'name', '(unnamed)') as name,
         coalesce(o.tags->>'leisure', o.tags->>'natural',
                  o.tags->>'place',   o.tags->>'tourism') as kind,
         coalesce(o.tags->>'note',        o.tags->>'note:en')        as note,
         coalesce(o.tags->>'description', o.tags->>'description:en') as description,
         st_distance(o.geom_full::geography, g.geom::geography)::integer as distance_m
    from public.beaches_gold g
    join public.osm_landing o on
         o.is_active
     and o.geom_full is not null
     and (o.tags ? 'note' or o.tags ? 'description'
       or o.tags ? 'note:en' or o.tags ? 'description:en')
     and (o.tags ? 'leisure' or o.tags ? 'natural'
       or o.tags ? 'place'   or o.tags ? 'tourism')
     and st_dwithin(o.geom_full::geography, g.geom::geography, p_radius_m)
   where g.fid = p_fid
   order by st_distance(o.geom_full, g.geom)
   limit p_limit;
$$;

grant execute on function public.get_osm_notes_for_beach(bigint, integer, integer)
  to anon, authenticated, service_role;

commit;
