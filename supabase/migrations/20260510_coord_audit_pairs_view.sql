-- 20260510_coord_audit_pairs_view.sql
--
-- Surfaces divergent (our coord vs external coord) pairs from
-- truth_external for a coord-audit curation UI. Currently only
-- DogTrekker has coordinates; BringFido / californiabeaches /
-- websearch need geocoding before they qualify (will join in once
-- coords are populated).
--
-- "Divergent" = same beach by spatial proximity within 2km AND
-- our coord differs from theirs by >= 100m. Smaller divergences
-- aren't worth curator attention.

begin;

create or replace view public.coord_audit_pairs as
with pairs as (
  -- For each (DT row × beaches_gold within 2km), compute the pair.
  select b.fid,
         coalesce(b.display_name_override, b.name) as beach_name,
         b.state,
         b.county_name,
         b.is_active,
         b.is_scoreable,
         public.beach_location_tier(p.dogs_allowed, p.has_off_leash,
                                    p.has_on_leash, p.dogs_prohibited_start::text) as tier,
         b.lat as our_lat,
         b.lon as our_lon,
         t.source,
         t.source_id,
         t.name as their_name,
         t.lat  as their_lat,
         t.lng  as their_lng,
         t.source_url as their_url,
         st_distance(b.geom::geography,
                     st_setsrid(st_makepoint(t.lng, t.lat), 4326)::geography)::int
           as divergence_m
    from public.truth_external t
    join public.beaches_gold b
      on b.state = 'CA'
     and st_dwithin(b.geom::geography,
                    st_setsrid(st_makepoint(t.lng, t.lat), 4326)::geography,
                    2000)
    left join public.beach_dog_policy p on p.arena_group_id = b.fid
   where t.source = 'dogtrekker'
     and t.lat is not null
),
-- Keep the closest DT match per fid. One pair per beach.
closest_per_fid as (
  select distinct on (fid) *
    from pairs
   order by fid, divergence_m
)
select * from closest_per_fid
 where divergence_m >= 100
 order by divergence_m desc;

grant select on public.coord_audit_pairs to anon, authenticated, service_role;

comment on view public.coord_audit_pairs is
  'Divergent coord pairs (our beaches_gold vs external sources like '
  'DogTrekker). Used by admin/coord-audit.html to surface beaches '
  'where the curator should adjudicate which coord is at the actual sand.';

commit;
