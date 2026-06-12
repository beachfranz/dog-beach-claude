-- 20260612b_dedup_distance_default_500m.sql
--
-- Tighten public.find_distance_name_dup_pairs default radius from
-- 1500m → 500m. 1500m is ~max named-beach extent — letting it stand
-- as the default allowed the Carmel-by-the-Sea cascade to merge the
-- river beach (36.537) and the downtown beach (36.551) — 1.5km apart,
-- same name "Carmel River State Beach" (the downtown row was
-- mislabeled upstream). At 500m the river/downtown pair drops out.
--
-- Callers that genuinely want the wider radius can still pass it
-- explicitly. run_distance_name_dedup is the only known caller and
-- does NOT pass an explicit value — it inherits the default.
--
-- Body is unchanged. Just the default arg flips 1500 → 500.

CREATE OR REPLACE FUNCTION public.find_distance_name_dup_pairs(
  p_state          text,
  p_max_distance_m integer DEFAULT 500
)
RETURNS TABLE(f1 bigint, f2 bigint, lname text, county text, dist_m integer)
LANGUAGE sql STABLE
AS $function$
  WITH active AS (
    SELECT fid, lower(coalesce(display_name_override, name)) AS lname,
           county_name, state, lat, lon
      FROM public.beaches_gold
     WHERE is_active
       AND (scoring_tier IN ('daily','hourly'))
       AND lat IS NOT NULL
       AND state = p_state
  )
  SELECT a.fid, b.fid, a.lname, a.county_name,
         round(ST_Distance(
           ST_MakePoint(a.lon, a.lat)::geography,
           ST_MakePoint(b.lon, b.lat)::geography
         ))::int AS dist_m
    FROM active a
    JOIN active b
      ON a.lname = b.lname
     AND a.county_name = b.county_name
     AND a.fid < b.fid
   WHERE ST_Distance(
           ST_MakePoint(a.lon, a.lat)::geography,
           ST_MakePoint(b.lon, b.lat)::geography
         ) < p_max_distance_m;
$function$;

NOTIFY pgrst, 'reload schema';
