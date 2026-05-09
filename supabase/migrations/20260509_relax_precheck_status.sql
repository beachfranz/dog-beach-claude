-- 20260509_relax_precheck_status.sql
--
-- assert_state_upstream_loaded(state) used to require status='ok' for
-- each of pad_us / osm_landing / osm_amenities / tiger_places. The bulk
-- loaders mark a state 'skipped' when they detect existing data and
-- decline to re-fetch — that's also a fully-loaded state, just from a
-- prior run. RI hits this: osm_landing was loaded earlier with
-- status='skipped' and would fail precheck despite having 70 rows.
--
-- Relaxes the check to accept both 'ok' and 'skipped'.

begin;

create or replace function public.assert_state_upstream_loaded(p_state text)
returns table(source text, status text, row_count int, last_loaded_at timestamptz)
language plpgsql
as $$
declare
  v_required text[] := array['pad_us', 'osm_landing', 'osm_amenities', 'tiger_places'];
  v_missing  text[] := array[]::text[];
  v_src text;
begin
  foreach v_src in array v_required loop
    if not exists (
      select 1 from public.external_source_status ess
       where ess.source = v_src and ess.state = p_state
         and ess.status in ('ok','skipped')
    ) then
      v_missing := v_missing || v_src;
    end if;
  end loop;
  if array_length(v_missing, 1) > 0 then
    raise exception
      'state %: missing upstream sources in external_source_status: % (status in (ok, skipped) required)',
      p_state, v_missing;
  end if;
  return query
    select s.source, s.status, s.row_count, s.last_loaded_at
      from public.external_source_status s
     where s.state = p_state
       and s.source = any(v_required)
     order by s.source;
end $$;

commit;
