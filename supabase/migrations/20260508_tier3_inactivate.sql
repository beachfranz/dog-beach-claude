-- 20260508_tier3_inactivate.sql
--
-- Inactivate any beach whose curated dog policy resolves to dogs_allowed='no'.
-- Tier 3 = no dogs + low amenity score. We don't want these in find feeds,
-- scoring fan-outs, or the consumer surface generally.
--
-- Design: side-effect trigger on beach_dog_policy. Whenever a row is upserted
-- with dogs_allowed='no', set the corresponding beaches_gold.is_active=false.
-- Idempotent — re-running on already-inactive rows is a no-op.
--
-- Also a one-shot SQL function so we can fire it in batch (post-publish_canonical).

begin;

create or replace function public.inactivate_tier3_beaches(p_state text default null)
returns table(deactivated_count bigint, fids bigint[])
language plpgsql
security definer
as $function$
declare v_count bigint := 0; v_fids bigint[];
begin
  with picks as (
    select g.fid
      from public.beaches_gold g
      join public.beach_dog_policy bdp on bdp.arena_group_id = g.fid
     where g.is_active = true
       and bdp.dogs_allowed = 'no'
       and (p_state is null or g.state = p_state)
  ),
  upd as (
    update public.beaches_gold g
       set is_active = false,
           is_scoreable = false
      from picks
     where g.fid = picks.fid
    returning g.fid
  )
  select count(*), array_agg(fid) into v_count, v_fids from upd;
  return query select v_count, coalesce(v_fids, '{}'::bigint[]);
end;
$function$;

grant execute on function public.inactivate_tier3_beaches(text) to anon, authenticated, service_role;

-- Trigger on beach_dog_policy: any time dogs_allowed becomes 'no', flip the beach.
create or replace function public.tg_inactivate_on_no_dogs()
returns trigger
language plpgsql
as $function$
begin
  if NEW.dogs_allowed = 'no' then
    update public.beaches_gold
       set is_active = false, is_scoreable = false
     where fid = NEW.arena_group_id and is_active = true;
  end if;
  return NEW;
end;
$function$;

drop trigger if exists tg_beach_dog_policy_inactivate_on_no_dogs on public.beach_dog_policy;
create trigger tg_beach_dog_policy_inactivate_on_no_dogs
  after insert or update of dogs_allowed on public.beach_dog_policy
  for each row execute function public.tg_inactivate_on_no_dogs();

commit;
