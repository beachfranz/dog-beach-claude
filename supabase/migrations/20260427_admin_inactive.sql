-- Admin "mark inactive" flag on all three beach-location sources, plus
-- a single SECURITY DEFINER RPC that lets the admin map page write the
-- flag without granting UPDATE on the underlying tables to anon.
--
-- Design: orthogonal to each source's own archived/inactive concept.
--   - CCC has its own `archived` column (CCC's source-of-truth)
--   - UBP has `inactive_reason` (us_beach_points-specific)
-- `admin_inactive` is OUR override — null/false = active, true = hide.

alter table public.ccc_access_points add column if not exists admin_inactive boolean default false;
alter table public.us_beach_points  add column if not exists admin_inactive boolean default false;
alter table public.osm_features     add column if not exists admin_inactive boolean default false;

create or replace function public.set_feature_inactive(
  p_source   text,             -- 'ccc' | 'osm' | 'usbeach'
  p_inactive boolean,
  p_objectid bigint default null,   -- for ccc
  p_fid      bigint default null,   -- for usbeach
  p_osm_type text   default null,   -- for osm
  p_osm_id   bigint default null    -- for osm
) returns boolean
language plpgsql security definer as $$
declare v_count int;
begin
  if p_source = 'ccc' then
    update public.ccc_access_points
       set admin_inactive = p_inactive
     where objectid = p_objectid;
    get diagnostics v_count = row_count;
  elsif p_source = 'osm' then
    update public.osm_features
       set admin_inactive = p_inactive
     where osm_type = p_osm_type and osm_id = p_osm_id;
    get diagnostics v_count = row_count;
  elsif p_source = 'usbeach' then
    update public.us_beach_points
       set admin_inactive = p_inactive
     where fid = p_fid;
    get diagnostics v_count = row_count;
  else
    raise exception 'Unknown source: %', p_source;
  end if;
  return v_count > 0;
end;
$$;

grant execute on function public.set_feature_inactive(
  text, boolean, bigint, bigint, text, bigint
) to anon, authenticated;
