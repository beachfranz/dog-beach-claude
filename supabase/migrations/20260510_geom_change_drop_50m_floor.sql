-- 20260510_geom_change_drop_50m_floor.sql
--
-- The geom-change trigger silently dropped curator moves <50m. The
-- original intent was to skip arena-clustering jitter, but that's
-- already covered by the `app.arena_clustering_active` setting check.
--
-- Sub-50m moves DO change polygon containment when the original point
-- sat near a polygon edge — concretely, several Bay Area / NorCal
-- beaches Franz fine-tuned via coord-audit on 2026-05-10 / 11 never
-- enqueued, so their cpad_unit_id / amenities / governance stayed stale.
--
-- Drop the 50m floor entirely. Curator moves are deliberate; let the
-- BEP cascade run. The arena-clustering guard remains.

begin;

create or replace function public.tg_invalidate_dog_policy_on_geom_change()
returns trigger
language plpgsql
as $function$
begin
  -- Skip during arena clustering re-runs (safe-mode flag set by the
  -- clustering job; never set during ordinary curator writes).
  if current_setting('app.arena_clustering_active', true) = 'true' then
    return NEW;
  end if;

  -- Enqueue for async processing. Any geom change reaches here because
  -- the trigger only fires WHEN old.geom IS DISTINCT FROM new.geom.
  insert into public.beach_geom_change_queue (fid)
    values (NEW.fid)
   on conflict (fid) do update set queued_at = excluded.queued_at,
                                    last_error = null,
                                    attempts = 0;
  return NEW;
end;
$function$;

commit;
