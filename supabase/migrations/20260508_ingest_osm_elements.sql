-- 20260508_ingest_osm_elements.sql
--
-- Server-side OSM batch ingest. Takes a JSONB array of Overpass elements
-- and inserts them into osm_landing with state-polygon clip. Replaces
-- the per-row Python loop in load_state.py for the case where the
-- launcher runs from an edge function (admin-launch-state).
--
-- Element shape (matches Overpass JSON):
--   { type: 'node'|'way'|'relation', id: number, lat?, lon?,
--     geometry?: [{lat,lon},...], tags: {...} }

begin;

create or replace function public.ingest_osm_elements(
  p_elements jsonb,
  p_state    text
)
returns table(
  fetched bigint,
  inserted bigint,
  skipped_oos bigint,
  skipped_no_geom bigint
)
language plpgsql
security definer
as $function$
declare
  v_state_geom geometry;
  v_state_fp text := case p_state
                       when 'CA' then '06'
                       when 'OR' then '41'
                       when 'WA' then '53'
                     end;
  v_fetched bigint := 0;
  v_inserted bigint := 0;
  v_oos bigint := 0;
  v_no_geom bigint := 0;
  el jsonb;
  etype text;
  eid bigint;
  geom_wkt text;
  geom geometry;
  tags jsonb;
  lat_v double precision;
  lon_v double precision;
  coords text;
begin
  -- Resolve state polygon from counties union
  select st_union(c.geom) into v_state_geom
    from public.counties c where c.state_fp = v_state_fp;

  if p_elements is null or jsonb_typeof(p_elements) <> 'array' then
    return query select 0::bigint, 0::bigint, 0::bigint, 0::bigint;
    return;
  end if;

  for el in select * from jsonb_array_elements(p_elements)
  loop
    v_fetched := v_fetched + 1;
    etype := el->>'type';
    eid := (el->>'id')::bigint;
    tags := coalesce(el->'tags', '{}'::jsonb);
    geom_wkt := null;
    lat_v := null;
    lon_v := null;

    if etype = 'node' then
      lat_v := (el->>'lat')::double precision;
      lon_v := (el->>'lon')::double precision;
      if lat_v is not null and lon_v is not null then
        geom_wkt := format('POINT(%s %s)', lon_v, lat_v);
      end if;
    elsif etype in ('way','relation') and jsonb_typeof(el->'geometry') = 'array' then
      select string_agg(format('%s %s', g->>'lon', g->>'lat'), ',')
        into coords
        from jsonb_array_elements(el->'geometry') g;
      if coords is not null and length(coords) > 0 then
        -- Polygon if first==last, else linestring
        if (el->'geometry'->0) = (el->'geometry'->-1) then
          geom_wkt := format('POLYGON((%s))', coords);
        else
          geom_wkt := format('LINESTRING(%s)', coords);
        end if;
      end if;
    end if;

    if geom_wkt is null then
      v_no_geom := v_no_geom + 1;
      continue;
    end if;

    geom := st_setsrid(st_geomfromtext(geom_wkt), 4326);

    -- State-polygon clip
    if v_state_geom is not null and not st_intersects(v_state_geom, geom) then
      v_oos := v_oos + 1;
      continue;
    end if;

    begin
      insert into public.osm_landing
        (type, id, lat, lon, tags, geom_full, fetched_at, fetched_by, is_active)
      values
        (etype, eid, lat_v, lon_v, tags, geom, now(), 'admin-launch-state', true)
      on conflict (type, id, fetched_at) do nothing;
      v_inserted := v_inserted + 1;
    exception when others then
      -- Don't poison the loop; count as no-geom-equiv
      v_no_geom := v_no_geom + 1;
    end;
  end loop;

  return query select v_fetched, v_inserted, v_oos, v_no_geom;
end;
$function$;

grant execute on function public.ingest_osm_elements(jsonb, text) to anon, authenticated, service_role;

commit;
