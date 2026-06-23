-- 20260623a_find_alternatives_join_on_fid.sql
--
-- R2 fix (regression audit 2026-06-23). find_alternatives_for_detail (the
-- "more like this" RPC behind detail.html, via get-beach-detail) joined
-- beach_dog_policy + beach_day_recommendations on g.group_id, but those tables
-- key arena_group_id to the FID (per [[arena_group_id-is-fid-not-group]]). For
-- the 513 beaches where fid <> group_id, each alternative card got null
-- best_window_label + a neighbor's policy. Sibling of the get_beach_info bug
-- fixed in 20260620b; this RPC was missed.
--
-- Body reproduced verbatim from the live def; only the two joins change
-- g.group_id -> g.fid. CREATE OR REPLACE, same signature, no edge-fn redeploy
-- (get-beach-detail calls the RPC by name).

CREATE OR REPLACE FUNCTION public.find_alternatives_for_detail(p_fid bigint, p_date date, p_max_km double precision DEFAULT 40, p_limit integer DEFAULT 5)
 RETURNS TABLE(fid bigint, location_id text, name text, distance_km double precision, has_on_leash boolean, has_off_leash boolean, best_window_label text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
    dr.best_window_label
  from public.beaches_gold g
  cross join src
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.fid and dr.local_date = p_date
  where g.is_active
    and (g.scoring_tier in ('daily','hourly'))
    and g.fid <> p_fid
    and (bdp.has_on_leash is true or bdp.has_off_leash is true)
    and st_dwithin(g.geom::geography, src.geom::geography, p_max_km * 1000)
  order by g.geom::geography <-> src.geom::geography
  limit greatest(p_limit, 1);
$function$;

NOTIFY pgrst, 'reload schema';
