-- 20260506_zone_rules_promote_bigint.sql
--
-- Fix type mismatch in _promote_zone_rules_for_fid: function takes
-- p_fid int, but beaches_gold.fid (and beach_enrichment_provenance.
-- gold_fid) are bigint. The trigger
-- tg_after_change_promote_zone_rules called the function with bigint
-- and silently failed via PL/pgSQL exception:
--   "No function matches the given name and argument types"
-- swallowed inside the trigger. Symptom: zone_rules never promoted
-- when the dogs-chain trigger fired _resolve_dogs_gold (which UPDATEs
-- BEP rows, which then fired the zone_rules trigger, which crashed).
--
-- Fix: redefine with bigint signature. CREATE OR REPLACE doesn't
-- handle arg-type changes — must DROP the old signature first.

begin;

drop function if exists public._promote_zone_rules_for_fid(int);

create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text
language plpgsql
security definer
as $$
declare
  v_zr jsonb;
  v_updated boolean;
begin
  select claimed_values->'zone_rules'
    into v_zr
    from public.beach_enrichment_provenance
   where source = 'text_repass_v1'
     and gold_fid = p_fid
     and claimed_values ? 'zone_rules'
   order by updated_at desc
   limit 1;

  if v_zr is null then return 'no zone_rules for fid'; end if;

  update public.beach_dog_policy
     set zone_rules = v_zr,
         zone_rules_updated_at = now()
   where arena_group_id = p_fid
     and (zone_rules is distinct from v_zr);
  v_updated := found;

  if not v_updated and not exists (
    select 1 from public.beach_dog_policy where arena_group_id = p_fid
  ) then
    insert into public.beach_dog_policy (arena_group_id, zone_rules, zone_rules_updated_at)
    values (p_fid, v_zr, now())
    on conflict (arena_group_id) do update
      set zone_rules = excluded.zone_rules,
          zone_rules_updated_at = excluded.zone_rules_updated_at;
    v_updated := true;
  end if;

  return case when v_updated then 'updated' else 'no-op (already current)' end;
end $$;

revoke all on function public._promote_zone_rules_for_fid(bigint) from public, anon, authenticated;
grant  execute on function public._promote_zone_rules_for_fid(bigint) to service_role;

-- Also drop the now-redundant ::int casts in the gold-insert trigger
-- since the function takes bigint natively.
create or replace function public.tg_after_insert_gold_promote_chain()
returns trigger
language plpgsql
as $$
begin
  perform public.compute_beach_field_consensus(NEW.fid);
  perform public._resolve_dogs_gold(NEW.fid);
  perform public.promote_canonical_dogs_to_beach_dog_policy(NEW.fid, 0.5);
  perform public._promote_zone_rules_for_fid(NEW.fid);
  return NEW;
end $$;

commit;
