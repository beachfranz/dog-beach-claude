-- 20260508_geom_change_async_queue.sql
--
-- Pin #21: re-enable beaches_gold geom-change cascade with async/threshold
-- semantics so curator drags propagate to BEP without blocking the UI.
--
-- Before tonight, the geom-change trigger ran the full populate cascade
-- synchronously and hit the Supabase HTTP 524 timeout on curator drags
-- (~30-60s per fid pre-cpad-fix). We disabled the trigger 2026-05-08.
--
-- After cpad geom-index fix the cascade is ~0.5s per fid in batch, ~4s
-- solo. Still noticeable in a curator UI but not catastrophic. Async
-- queue keeps it imperceptible.
--
-- Design:
--   1. beach_geom_change_queue table (fid PK, queued_at timestamp)
--   2. Trigger fires on UPDATE OF geom but only enqueues when:
--      - geom actually moved > 50m (skips parking-lot fine-tuning)
--      - app.arena_clustering_active is NOT 'true' (skips clustering noise)
--   3. process_geom_change_queue(p_max_fids int) function pulls fids from
--      queue, runs refire_bep_cascade, removes processed rows
--   4. pg_cron schedules the worker every 5 minutes

begin;

create table if not exists public.beach_geom_change_queue (
  fid bigint primary key references public.beaches_gold(fid) on delete cascade,
  queued_at timestamptz not null default now(),
  attempts int default 0,
  last_error text
);

create or replace function public.tg_invalidate_dog_policy_on_geom_change()
returns trigger
language plpgsql
as $function$
declare
  v_dist_m double precision;
begin
  -- Skip during arena clustering re-runs (safe-mode flag)
  if current_setting('app.arena_clustering_active', true) = 'true' then
    return NEW;
  end if;

  -- Skip very small moves (within 50m — likely curator parking-lot fine-tune)
  v_dist_m := st_distance(OLD.geom::geography, NEW.geom::geography);
  if v_dist_m is null or v_dist_m < 50 then
    return NEW;
  end if;

  -- Enqueue for async processing
  insert into public.beach_geom_change_queue (fid)
    values (NEW.fid)
   on conflict (fid) do update set queued_at = excluded.queued_at,
                                    last_error = null,
                                    attempts = 0;
  return NEW;
end;
$function$;

create or replace function public.process_geom_change_queue(p_max_fids int default 50)
returns table(fids_processed bigint, errors bigint, queue_remaining bigint)
language plpgsql
security definer
as $function$
declare
  fid_iter bigint;
  err_msg text;
  v_processed bigint := 0;
  v_errors bigint := 0;
  v_remaining bigint := 0;
begin
  for fid_iter in
    select fid from public.beach_geom_change_queue
     where attempts < 3
     order by queued_at asc
     limit p_max_fids
  loop
    begin
      perform public.refire_bep_cascade(array[fid_iter]);
      delete from public.beach_geom_change_queue where fid = fid_iter;
      v_processed := v_processed + 1;
    exception when others then
      err_msg := SQLERRM;
      update public.beach_geom_change_queue
         set attempts = attempts + 1,
             last_error = err_msg
       where fid = fid_iter;
      v_errors := v_errors + 1;
    end;
  end loop;

  select count(*) into v_remaining from public.beach_geom_change_queue;
  return query select v_processed, v_errors, v_remaining;
end;
$function$;

grant execute on function public.process_geom_change_queue(int) to anon, authenticated, service_role;

-- Re-enable the trigger (it was DISABLED earlier tonight per Pin #21)
alter table public.beaches_gold enable trigger tg_beaches_gold_geom_change;

-- Schedule pg_cron worker — every 5 minutes
do $$
begin
  if exists(select 1 from pg_extension where extname = 'pg_cron') then
    perform cron.unschedule('process_geom_change_queue');
  end if;
exception when others then null;
end $$;

select cron.schedule(
  'process_geom_change_queue',
  '*/5 * * * *',
  'select public.process_geom_change_queue(50);'
);

commit;
