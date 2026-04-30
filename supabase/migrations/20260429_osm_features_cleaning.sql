-- OSM features cleaning — annotation-in-place.
--
-- Adds cleaning_status + cleaning_reason columns to public.osm_features
-- and a classifier function that labels each row as clean / dropped /
-- needs_review based on a set of rules. Surfaces a view
-- public.osm_features_clean that filters to clean rows only.
--
-- Non-destructive: doesn't move or delete data. Re-running the
-- classifier is idempotent. Add/tune rules and re-classify.
--
-- Cleaning rules (initial; iterate after running once):
--   1. admin_inactive=true → 'dropped' (reason: 'admin_inactive')
--   2. natural=beach, no name, polygon area < 1,000 sq m → 'dropped'
--      (reason: 'tiny_unnamed_beach')
--   3. natural=beach, no geom_full (centroid only) → 'needs_review'
--      (reason: 'centroid_only_no_polygon')
--   4. duplicate way+relation pair (same name, overlap > 50%) → keep
--      relation, drop way → 'dropped' (reason: 'dup_way_of/<relation_id>')
--      [deferred — needs second pass; not in v1 of the function]
--   5. otherwise → 'clean'

alter table public.osm_features
  add column if not exists cleaning_status text
    check (cleaning_status in ('clean','dropped','needs_review','unknown')),
  add column if not exists cleaning_reason text,
  add column if not exists cleaning_classified_at timestamptz;

create index if not exists osm_features_cleaning_status_idx
  on public.osm_features (cleaning_status);

comment on column public.osm_features.cleaning_status is
  'Cleaning classification — clean | dropped | needs_review | unknown. Set by public.classify_osm_features_cleanliness().';
comment on column public.osm_features.cleaning_reason is
  'Free-text reason matched to cleaning rule (e.g. tiny_unnamed_beach, admin_inactive, centroid_only_no_polygon).';


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
  -- Reset prior classification so re-running is deterministic
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

  -- Rule 3: centroid-only beach (missing polygon) → review
  update public.osm_features
     set cleaning_status = 'needs_review',
         cleaning_reason = 'centroid_only_no_polygon'
   where cleaning_status = 'unknown'
     and (tags->>'natural') = 'beach'
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
    'needs_review_centroid', v_review_centroid,
    'clean',                 v_clean
  );
end;
$function$;

comment on function public.classify_osm_features_cleanliness() is
  'Classifies each row in osm_features as clean / dropped / needs_review based on rules. Returns jsonb counts. Wrapped by Dagster asset osm_features_clean_run.';


create or replace view public.osm_features_clean as
  select * from public.osm_features
   where cleaning_status = 'clean';

comment on view public.osm_features_clean is
  'Filtered view of osm_features showing only rows where cleaning_status = ''clean''. Use this in joins instead of public.osm_features when you want noise-free data.';
