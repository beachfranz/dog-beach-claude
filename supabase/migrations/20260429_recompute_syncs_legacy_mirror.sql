-- recompute_all_dogs_verdicts_by_origin now syncs the legacy
-- ccc_access_points.dogs_verdict mirror at the end.
--
-- Without this, by_origin only writes to beach_verdicts and the
-- legacy mirror column on ccc_access_points goes stale. Several
-- display callers still read the mirror directly:
--   admin/oc-beach-sand-polygons.html
--   admin/location-editor.html
--   beach_access_features_view
--   beach_neighbor_classification
--   ubp_ccc_smart_match
--   dogs_verdict_override
-- Until those are migrated to read from beach_verdicts, the mirror
-- has to stay in sync after every batch recompute.
--
-- One-shot sync (the 135 stale rows that surfaced this morning) was
-- run inline before this migration.

create or replace function public.recompute_all_dogs_verdicts_by_origin()
returns integer language plpgsql security definer as $$
declare
  n integer := 0;
  rec record;
begin
  for rec in (
    select bl.origin_key from public.beach_locations bl
    union
    select 'osm/' || o.osm_type || '/' || o.osm_id::text
      from public.osm_features o
     where o.feature_type in ('beach','dog_friendly_beach')
       and (o.admin_inactive is null or o.admin_inactive = false)
    union
    select 'ccc/' || c.objectid::text
      from public.ccc_access_points c
     where (c.archived is null or c.archived <> 'Yes')
       and (c.admin_inactive is null or c.admin_inactive = false)
       and c.latitude is not null
  ) loop
    perform public.compute_dogs_verdict_by_origin(rec.origin_key);
    n := n + 1;
  end loop;

  -- Sync legacy ccc_access_points.dogs_verdict mirror so display
  -- callers reading the column directly see fresh values.
  update public.ccc_access_points c
     set dogs_verdict            = bv.dogs_verdict,
         dogs_verdict_confidence = bv.dogs_verdict_confidence,
         dogs_verdict_meta       = bv.dogs_verdict_meta
    from public.beach_verdicts bv
   where bv.origin_key = 'ccc/' || c.objectid::text
     and c.dogs_verdict is distinct from bv.dogs_verdict;

  return n;
end;
$$;
