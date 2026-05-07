-- 20260507_find_alternatives_for_detail.sql
--
-- Deterministic alternatives RPC for detail.html. Returns nearby
-- dog-friendly beaches with their day_status for the requested date.
-- The frontend uses this two ways:
--   1. When the current beach is no-dogs: render an "alternatives" card
--      so the user can switch to a dog-allowed beach.
--   2. When the current beach has no good windows left today: render
--      the same card, filtered to alternatives where day_status is
--      'go' or 'advisory'.
-- When neither condition fires, the card stays hidden.
--
-- Rendered as a card on the page (verified data) instead of fed into
-- Scout's prompt — Scout was hallucinating around the alternatives
-- list when we tried that.

begin;

create or replace function public.find_alternatives_for_detail(
  p_fid    bigint,
  p_date   date,
  p_max_km double precision default 40,
  p_limit  integer default 5
)
returns table(
  fid                bigint,
  location_id        text,
  name               text,
  distance_km        double precision,
  has_on_leash       boolean,
  has_off_leash      boolean,
  day_status         text,
  best_window_label  text
)
language sql stable security definer
set search_path to 'public'
as $$
  with src as (
    select fid, geom from public.beaches_gold where fid = p_fid
  )
  select
    g.fid,
    g.location_id,
    coalesce(g.display_name_override, g.name) as name,
    st_distance(g.geom::geography, src.geom::geography) / 1000.0 as distance_km,
    bdp.has_on_leash,
    bdp.has_off_leash,
    dr.day_status,
    dr.best_window_label
  from public.beaches_gold g
  cross join src
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.group_id and dr.local_date = p_date
  where g.is_active
    and g.is_scoreable
    and g.fid <> p_fid
    and (bdp.has_on_leash is true or bdp.has_off_leash is true)
    and st_dwithin(g.geom::geography, src.geom::geography, p_max_km * 1000)
  order by g.geom::geography <-> src.geom::geography
  limit greatest(p_limit, 1);
$$;

grant execute on function public.find_alternatives_for_detail(bigint, date, double precision, integer)
  to anon, authenticated, service_role;

commit;
