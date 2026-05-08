-- 20260508_gate_auto_scoreable_to_ca.sql
--
-- Stop auto-flipping is_scoreable=true on every state's new beaches.
-- Only CA continues to auto-score (560 grandfathered rows + ongoing
-- CA promotions). New states (OR, WA, etc.) land with is_scoreable=false
-- and require explicit opt-in via the admin curator flow.
--
-- Why this matters: tg_beaches_gold_score_on_insert fires daily-beach-
-- refresh on every is_scoreable=true insert, and the nightly cron re-
-- runs it for all is_scoreable rows. LLM narrative cost is per-beach,
-- per-day. Auto-flagging hundreds of fresh-launch beaches with no
-- curation review pumps spend before we know the data is good.
--
-- Existing OR rows stay is_scoreable=true (forward-only behavior).
-- Tier 3 inactivation still runs separately.

begin;

create or replace function public.tg_auto_scoreable_socal_gold()
returns trigger
language plpgsql
as $function$
begin
  if NEW.is_active is true
     and NEW.state = 'CA'                                  -- ★ CA-only now
     and (NEW.is_scoreable is null or NEW.is_scoreable = false)
  then
    NEW.is_scoreable := true;
  end if;
  return NEW;
end;
$function$;

commit;
