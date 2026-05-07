-- 20260507_pin3_score_on_is_scoreable_flip.sql
--
-- Pin #3 (scope-1): fire daily-beach-refresh when a beach's is_scoreable
-- flips to true. Lets newly-promoted beaches start scoring immediately
-- instead of waiting up to 24 hours for the next nightly cron.
--
-- Triggers in two cases:
--   (a) AFTER UPDATE: is_scoreable was false/null, now true
--   (b) AFTER INSERT: row inserted with is_scoreable=true
--
-- Doesn't touch the existing daily cron (`daily_beach_refresh_nightly`)
-- — that remains as the fan-out backstop. This trigger is just for the
-- newly-promoted-beach case.
--
-- Out of scope for this migration (deferred):
--   * scoring_config changes (rescore all beaches when weights change)
--   * weather/tide upstream changes (we don't get push events from those)
--   * is_scoreable flips OFF (no rescoring needed)
--
-- Secret handling: same SECURITY DEFINER wrapper pattern as the existing
-- _fire_daily_beach_refresh function.
--
-- Cost: each trigger fire ≈ 1 Open-Meteo + 1 NOAA + 1 BestTime + 1 Claude
-- narrative ≈ $0.05-0.15. Bulk flips could spike costs — document the
-- "DISABLE TRIGGER for bulk operations" pattern below.

begin;

create or replace function public._fire_daily_beach_refresh_for_fid(p_fid bigint)
returns bigint
language plpgsql
security definer
as $$
declare
  request_id bigint;
  v_location_id text;
begin
  -- Look up the location_id for this fid. If the row doesn't have a
  -- location_id (rare, e.g., partially-seeded), skip silently.
  select location_id into v_location_id
    from public.beaches_gold where fid = p_fid;
  if v_location_id is null then return null; end if;

  select net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/daily-beach-refresh',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body    := jsonb_build_object('location_ids', jsonb_build_array(v_location_id)),
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

revoke all on function public._fire_daily_beach_refresh_for_fid(bigint) from public, anon, authenticated;
grant  execute on function public._fire_daily_beach_refresh_for_fid(bigint) to service_role;

-- ====================================================================
-- Trigger function — fires daily-beach-refresh for a single fid.
-- Fire-and-forget; failures don't roll back the inserting txn.
-- ====================================================================

create or replace function public.tg_fire_refresh_on_score_flip()
returns trigger
language plpgsql
security definer
as $$
begin
  -- Defensive: never fire on inactive rows (would just no-op anyway since
  -- daily-beach-refresh skips is_scoreable=false beaches).
  if NEW.is_active is true and NEW.is_scoreable is true then
    perform public._fire_daily_beach_refresh_for_fid(NEW.fid);
  end if;
  return NEW;
end $$;

drop trigger if exists tg_beaches_gold_score_on_insert on public.beaches_gold;
drop trigger if exists tg_beaches_gold_score_on_flip   on public.beaches_gold;

create trigger tg_beaches_gold_score_on_flip
  after update of is_scoreable on public.beaches_gold
  for each row
  when (OLD.is_scoreable is distinct from NEW.is_scoreable
        and NEW.is_scoreable is true)
  execute function public.tg_fire_refresh_on_score_flip();

create trigger tg_beaches_gold_score_on_insert
  after insert on public.beaches_gold
  for each row
  when (NEW.is_scoreable is true)
  execute function public.tg_fire_refresh_on_score_flip();

comment on function public.tg_fire_refresh_on_score_flip() is
  'Pin #3: fire daily-beach-refresh when a beach is_scoreable flips to true. Bulk operations should DISABLE TRIGGER tg_beaches_gold_score_on_flip + tg_beaches_gold_score_on_insert, do the bulk work, ENABLE TRIGGER, then explicitly call _fire_daily_beach_refresh() once at the end.';

commit;
