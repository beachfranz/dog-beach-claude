-- 20260506_promote_chain_on_gold_insert.sql
--
-- Companion to tg_after_change_promote_dogs_chain. That trigger fires
-- when BEP rows change. But there's an inverse case: a beach gets
-- promoted from arena to gold AFTER its BEP evidence was already
-- written. In that case, the dogs-chain never re-fires, and the new
-- gold row sits at phase 02_evidence_only forever.
--
-- This trigger fires the same chain on INSERT to beaches_gold, picking
-- up any pre-existing evidence for that fid. Plus the zone_rules
-- promote for the same reason.

begin;

create or replace function public.tg_after_insert_gold_promote_chain()
returns trigger
language plpgsql
as $$
begin
  -- Run dogs-chain so any pre-existing BEP evidence flows to consumer.
  perform public.compute_beach_field_consensus(NEW.fid);
  perform public._resolve_dogs_gold(NEW.fid);
  perform public.promote_canonical_dogs_to_beach_dog_policy(NEW.fid, 0.5);

  -- Run zone_rules promote so any pre-existing text_repass_v1 row
  -- flows to beach_dog_policy.zone_rules. Function signature takes int;
  -- beaches_gold.fid is bigint so we cast.
  perform public._promote_zone_rules_for_fid(NEW.fid::int);

  return NEW;
end $$;

drop trigger if exists tg_after_insert_gold_promote_chain
  on public.beaches_gold;

create trigger tg_after_insert_gold_promote_chain
  after insert on public.beaches_gold
  for each row
  when (NEW.is_active is true)
  execute function public.tg_after_insert_gold_promote_chain();

commit;
