-- inactive_reason column on us_beach_points (2026-04-24)
-- Captures WHY a beach was marked inactive — useful for future review
-- (e.g., "show me all beaches marked inactive as 'catalina_island' so we
--  can tackle that backlog when we have time")

alter table public.us_beach_points
  add column if not exists inactive_reason text;

comment on column public.us_beach_points.inactive_reason is
  'Short reason tag for why a beach was marked is_active=false. Free-form text but typically one of: catalina_island, inland, remote, not_a_real_beach, other. NULL for active beaches.';

-- Update mark_beach_inactive to accept optional reason
drop function if exists public.mark_beach_inactive(int);

create or replace function public.mark_beach_inactive(
  p_fid    int,
  p_reason text default null
)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
begin
  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  before := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'is_active',        b.is_active,
    'inactive_reason',  b.inactive_reason,
    'validation_status', b.validation_status,
    'validation_flags',  b.validation_flags
  );

  update public.us_beach_points set
    is_active       = false,
    inactive_reason = p_reason,
    validation_flags = coalesce((
      select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
      where (f->>'check') is distinct from 'orphan_geocode'
    ), '[]'::jsonb),
    validation_status = case
      when jsonb_array_length(coalesce((
        select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
        where (f->>'check') is distinct from 'orphan_geocode'
      ), '[]'::jsonb)) = 0 then 'valid'
      else validation_status
    end,
    validated_at = now()
  where fid = p_fid;

  after := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'is_active', false,
    'inactive_reason', p_reason,
    'resolution', 'marked_inactive'
  );
  return next;
end;
$$;

revoke all on function public.mark_beach_inactive(int, text) from public, anon, authenticated;
grant  execute on function public.mark_beach_inactive(int, text) to service_role;
