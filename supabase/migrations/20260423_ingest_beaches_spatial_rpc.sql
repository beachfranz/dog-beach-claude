-- RPC used by the rewritten load-beaches-staging to ingest rows with a
-- coordinate-based state filter instead of the ADDR2 regex that was
-- silently dropping ~30% of landmark beaches.
--
-- Takes an array of candidate rows + a target state code. For each row,
-- checks whether its point lies in the state polygon (ST_Contains) or
-- within 50 km of it (ST_DWithin with geography cast for accurate meters)
-- — the 50km buffer catches coastal points that fall just outside our
-- simplified-polygon GeoJSON edge. Matched rows get inserted via
-- ON CONFLICT (src_fid) DO NOTHING so re-runs are idempotent.

create or replace function public.ingest_beaches_batch_with_state_filter(
  p_target_state_code text,
  p_rows              jsonb
) returns jsonb
language plpgsql
security definer
as $$
declare
  v_state_name text;
  v_total      int;
  v_matched    int;
  v_inserted   int;
begin
  select state_name into v_state_name from public.states where state_code = p_target_state_code;
  if v_state_name is null then
    return jsonb_build_object('error', 'unknown state_code: ' || p_target_state_code);
  end if;

  v_total := jsonb_array_length(p_rows);

  with candidates as (
    select
      (r->>'src_fid')::int                as src_fid,
      r->>'display_name'                  as display_name,
      (r->>'latitude')::double precision  as latitude,
      (r->>'longitude')::double precision as longitude,
      r->>'raw_address'                   as raw_address
    from jsonb_array_elements(p_rows) as r
  ),
  matched_points as (
    select c.* from candidates c
    where exists (
      select 1 from public.states s
      where s.state_code = p_target_state_code
        and (
          ST_Contains(s.geom, ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326))
          or ST_DWithin(
               s.geom::geography,
               ST_SetSRID(ST_MakePoint(c.longitude, c.latitude), 4326)::geography,
               50000
             )
        )
    )
  ),
  inserted_rows as (
    insert into public.beaches_staging_new (
      src_fid, display_name, latitude, longitude, raw_address, state
    )
    select
      src_fid, display_name, latitude, longitude, raw_address, v_state_name
    from matched_points
    on conflict (src_fid) do nothing
    returning 1
  )
  select
    (select count(*)::int from matched_points),
    (select count(*)::int from inserted_rows)
  into v_matched, v_inserted;

  return jsonb_build_object(
    'total',    v_total,
    'matched',  v_matched,
    'inserted', v_inserted
  );
end;
$$;

revoke all on function public.ingest_beaches_batch_with_state_filter(text, jsonb) from public, anon, authenticated;
grant  execute on function public.ingest_beaches_batch_with_state_filter(text, jsonb) to service_role;
