-- 20260508_refire_missing_bep_150.sql
--
-- Tune refire_missing_bep default from 30 → 150 after EXPLAIN ANALYZE
-- showed pad_us emitter is ~1.4s/fid in batch (660ms execution + 250ms
-- planning + cascade overhead), not the 0.22s/fid measured pre-pad_us.
--
-- Math: 150 × 1.4s = 210s. Comfortably under the 8-min Supabase cap.
-- The 258-row backlog clears in ~2 nights at this rate.

begin;

create or replace function public.refire_missing_bep(p_max_fids int default 150)
returns table(missing_count bigint, fids_refired bigint)
language plpgsql
security definer
as $function$
declare
  v_missing bigint := 0;
  v_refired bigint := 0;
  v_fids bigint[];
  v_rec record;
begin
  select count(*) into v_missing
    from public.beaches_gold g
   where g.is_active
     and not exists (select 1 from public.beach_enrichment_provenance b
                      where b.gold_fid = g.fid and b.field_group = 'governance');

  select array_agg(g.fid) into v_fids
    from (select g.fid
            from public.beaches_gold g
           where g.is_active
             and not exists (select 1 from public.beach_enrichment_provenance b
                              where b.gold_fid = g.fid and b.field_group = 'governance')
           order by g.fid limit p_max_fids) g;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_rec from public.refire_bep_cascade(v_fids);
    v_refired := coalesce(v_rec.fids_processed, 0);
  end if;

  return query select v_missing, v_refired;
end;
$function$;

commit;
