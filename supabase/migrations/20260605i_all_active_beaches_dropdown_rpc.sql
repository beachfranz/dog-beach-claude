-- all_active_beaches_for_dropdown — bypass PostgREST 1000-row cap for
-- the index.html location-switcher dropdown.
--
-- Why: get-beach-summary loads "allBeaches" via
--   from("beaches_gold").select("fid, location_id, name, display_name_override")
--     .eq("is_active", true).order("name")
-- This returns up to PostgREST's db-max-rows cap (1000). With 3,709
-- active beaches, the dropdown silently truncates to the first 1000
-- (PostgREST PK ASC ordering). Users see only ~27% of switchable
-- beaches.
--
-- Fix shape: return a SINGLE jsonb row containing the full array.
-- PostgREST's row cap is on row COUNT, not jsonb element count, so a
-- 1-row response with 3,709 array entries bypasses the cap entirely.
-- 3,709 entries × ~100 bytes ≈ 370KB — well under Supabase's response
-- size limit.

CREATE OR REPLACE FUNCTION public.all_active_beaches_for_dropdown()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT coalesce(
    jsonb_agg(
      jsonb_build_object(
        'arena_group_id', fid,
        'location_id',    location_id,
        'display_name',   coalesce(display_name_override, name)
      )
      ORDER BY coalesce(display_name_override, name)
    ),
    '[]'::jsonb
  )
  FROM public.beaches_gold
  WHERE is_active;
$$;

COMMENT ON FUNCTION public.all_active_beaches_for_dropdown() IS
'Returns the full active-beaches list as a single jsonb array for the index.html location-switcher dropdown. Bypasses PostgREST db-max-rows=1000 cap by returning a single row containing the array. Each element: {arena_group_id, location_id, display_name}. Per [[paired-functions-port-fixes-both-sides]] consider a dog-park equivalent if dog-park.html grows a similar switcher.';
