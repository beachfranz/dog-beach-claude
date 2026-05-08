-- 20260507_scoring_config_fire_refresh.sql
--
-- Pin #3 remaining scope (1/2): scoring_config-change trigger.
--
-- When the active scoring_config row changes (weights or thresholds
-- update), all scoreable beaches need to be rescored against the new
-- config. Without this trigger, admin must manually fire
-- daily-beach-refresh after editing weights — easy to forget.
--
-- Fires on INSERT or UPDATE of any row where is_active=true. Reuses
-- the existing _fire_daily_beach_refresh() (empty body = rescore all
-- scoreable beaches via the edge function's fan-out).
--
-- Statement-level (one fire per transaction, regardless of how many
-- rows touched). Avoids storms when an admin updates multiple fields
-- in one statement.
--
-- Cost: ~5-10 min of edge-fn work + Open-Meteo/NOAA/BestTime calls
-- across ~600 scoreable beaches. Admins editing config should expect
-- this to fire automatically.

begin;

create or replace function public.tg_scoring_config_fire_refresh()
returns trigger
language plpgsql
security definer
as $$
declare
  affected boolean;
  request_id bigint;
begin
  -- Were any active rows touched in this statement?
  if (TG_OP = 'INSERT') then
    select bool_or(is_active) into affected from new_table;
  elsif (TG_OP = 'UPDATE') then
    -- Active in EITHER old or new state — covers activation, deactivation,
    -- and edits to currently-active rows
    select bool_or(coalesce(o.is_active, false) or coalesce(n.is_active, false))
      into affected
      from new_table n
      left join old_table o on o.id = n.id;
  else
    affected := false;
  end if;

  if affected then
    select public._fire_daily_beach_refresh() into request_id;
    raise notice 'scoring_config change → fired daily-beach-refresh (request_id=%)', request_id;
  end if;
  return null;
end $$;

revoke all on function public.tg_scoring_config_fire_refresh() from public, anon, authenticated;
grant  execute on function public.tg_scoring_config_fire_refresh() to service_role;

drop trigger if exists tg_after_change_fire_refresh on public.scoring_config;

create trigger tg_after_insert_fire_refresh
  after insert on public.scoring_config
  referencing new table as new_table
  for each statement
  execute function public.tg_scoring_config_fire_refresh();

create trigger tg_after_update_fire_refresh
  after update on public.scoring_config
  referencing old table as old_table new table as new_table
  for each statement
  execute function public.tg_scoring_config_fire_refresh();

commit;
