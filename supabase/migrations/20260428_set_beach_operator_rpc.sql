-- RPC for fixing wrong operator_id values on beach_locations rows.
-- Routes to the correct source table by origin_key prefix:
--   "ubp/<fid>"      → us_beach_points
--   "ccc/<objectid>" → ccc_access_points
-- Logs every successful + failed call to admin_audit. Used by the
-- new admin/beach-operator-editor.html page.

create or replace function public.set_beach_operator(
  p_origin_key text,
  p_operator_id bigint,
  p_actor_ip text default null
) returns table (origin_key text, before_operator_id bigint, after_operator_id bigint)
language plpgsql security definer as $$
declare
  v_kind         text;
  v_id           text;
  v_before_op    bigint;
  v_after_op     bigint;
  v_before_row   jsonb;
  v_after_row    jsonb;
begin
  if p_origin_key is null or p_origin_key not like 'ubp/%' and p_origin_key not like 'ccc/%' then
    raise exception 'origin_key must start with "ubp/" or "ccc/"';
  end if;

  v_kind := split_part(p_origin_key, '/', 1);
  v_id   := split_part(p_origin_key, '/', 2);

  if v_kind = 'ubp' then
    select to_jsonb(t.*), t.operator_id into v_before_row, v_before_op
      from public.us_beach_points t where t.fid = v_id::integer;
    if v_before_row is null then
      raise exception 'no us_beach_points row with fid=%', v_id;
    end if;

    update public.us_beach_points
       set operator_id = p_operator_id,
           managing_agency_source = coalesce(managing_agency_source, '') || ' admin_set_operator'
     where fid = v_id::integer
     returning to_jsonb(us_beach_points.*), us_beach_points.operator_id
     into v_after_row, v_after_op;

  elsif v_kind = 'ccc' then
    select to_jsonb(t.*), t.operator_id into v_before_row, v_before_op
      from public.ccc_access_points t where t.objectid = v_id::integer;
    if v_before_row is null then
      raise exception 'no ccc_access_points row with objectid=%', v_id;
    end if;

    update public.ccc_access_points
       set operator_id = p_operator_id,
           managing_agency_source = coalesce(managing_agency_source, '') || ' admin_set_operator'
     where objectid = v_id::integer
     returning to_jsonb(ccc_access_points.*), ccc_access_points.operator_id
     into v_after_row, v_after_op;
  end if;

  insert into public.admin_audit(
    actor_ip, function_name, action, location_id,
    before, after, changed_fields, success
  ) values (
    p_actor_ip, 'set_beach_operator', 'update', p_origin_key,
    v_before_row, v_after_row, array['operator_id','managing_agency_source'], true
  );

  return query select p_origin_key, v_before_op, v_after_op;
end $$;

grant execute on function public.set_beach_operator(text, bigint, text) to anon, authenticated;
