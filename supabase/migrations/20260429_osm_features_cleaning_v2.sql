-- v2 of OSM cleaning rules.
--
-- Fix: rule 3 was flagging all polygon-less beach rows as needs_review.
-- But 168 of 169 such rows are osm_type='node' (single-point tagged
-- beaches) — nodes don't HAVE polygon geometry by definition, so they
-- can never have geom_full. Treating them as needs_review is wrong;
-- they're already as complete as they can be.
--
-- New rule 3: only osm_type IN ('way','relation') with NULL geom_full
-- is needs_review (genuine missing-polygon data).

create or replace function public.classify_osm_features_cleanliness()
returns jsonb
language plpgsql
security definer
as $function$
declare
  v_dropped_inactive int := 0;
  v_dropped_tiny     int := 0;
  v_review_centroid  int := 0;
  v_clean            int := 0;
  v_total            int := 0;
begin
  update public.osm_features
     set cleaning_status = 'unknown',
         cleaning_reason = null,
         cleaning_classified_at = now();
  get diagnostics v_total = row_count;

  -- Rule 1: admin_inactive
  update public.osm_features
     set cleaning_status = 'dropped',
         cleaning_reason = 'admin_inactive'
   where admin_inactive = true;
  get diagnostics v_dropped_inactive = row_count;

  -- Rule 2: tiny unnamed beach polygons
  update public.osm_features
     set cleaning_status = 'dropped',
         cleaning_reason = 'tiny_unnamed_beach'
   where cleaning_status = 'unknown'
     and (tags->>'natural') = 'beach'
     and name is null
     and geom_full is not null
     and st_area(geom_full::geography) < 1000;
  get diagnostics v_dropped_tiny = row_count;

  -- Rule 3 (v2): polygon-feature missing its polygon (genuine bug).
  -- osm_type='node' is excluded — nodes are points, never have polygons.
  update public.osm_features
     set cleaning_status = 'needs_review',
         cleaning_reason = 'way_or_relation_missing_polygon'
   where cleaning_status = 'unknown'
     and (tags->>'natural') = 'beach'
     and osm_type in ('way', 'relation')
     and geom_full is null;
  get diagnostics v_review_centroid = row_count;

  -- Rule 0 (default): everything else is clean
  update public.osm_features
     set cleaning_status = 'clean',
         cleaning_reason = null
   where cleaning_status = 'unknown';
  get diagnostics v_clean = row_count;

  return jsonb_build_object(
    'total',                 v_total,
    'dropped_inactive',      v_dropped_inactive,
    'dropped_tiny_unnamed',  v_dropped_tiny,
    'needs_review_missing_polygon', v_review_centroid,
    'clean',                 v_clean
  );
end;
$function$;
