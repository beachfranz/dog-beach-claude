-- 20260513_gold_set_queue_counts_rpc.sql
--
-- Helper RPC for the curator UI: returns queue progress counts for a state.
-- Used by admin-gold-queue-list edge function.

begin;

create or replace function public.gold_set_queue_counts_for_state(p_state text)
returns table(pending bigint, reviewed bigint, skipped bigint, total bigint)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $function$
  select
    count(*) filter (where status='pending')  as pending,
    count(*) filter (where status='reviewed') as reviewed,
    count(*) filter (where status='skipped')  as skipped,
    count(*)                                  as total
  from public.gold_set_curation_queue
 where state = p_state;
$function$;

grant execute on function public.gold_set_queue_counts_for_state(text)
  to anon, authenticated, service_role;

commit;
