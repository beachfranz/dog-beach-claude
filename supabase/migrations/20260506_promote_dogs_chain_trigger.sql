-- 20260506_promote_dogs_chain_trigger.sql
--
-- Wires the landing→consumer chain for the 'dogs' field group. Whenever
-- a row is inserted or its claimed_values change in
-- beach_enrichment_provenance with field_group='dogs', re-runs:
--
--   1. compute_beach_field_consensus(fid)
--      → recomputes beach_field_consensus for that fid (all fields)
--   2. _resolve_dogs_gold(fid)
--      → marks canonical winner among BEP rows for the dogs field group
--   3. promote_canonical_dogs_to_beach_dog_policy(fid, 0.5)
--      → writes the canonical winner's claimed values into
--         beach_dog_policy (dogs_allowed, leash_policy, off_leash_flag,
--         dogs_prohibited_*, dogs_allowed_areas, etc.) — only when
--         confidence ≥ 0.5
--
-- Anti-recursion: _resolve_dogs_gold updates is_canonical on BEP rows.
-- Without a guard, that update re-fires the trigger. Filter the trigger
-- to evidence changes only — not metadata-only updates like is_canonical
-- flips. The WHEN clause checks claimed_values for changes.

begin;

create or replace function public.tg_promote_dogs_chain()
returns trigger
language plpgsql
as $$
begin
  if NEW.gold_fid is null then return NEW; end if;
  -- Anti-recursion: skip metadata-only updates (is_canonical flips, etc).
  -- _resolve_dogs_gold updates BEP rows to set is_canonical=true; without
  -- this guard the trigger re-fires forever.
  if TG_OP = 'UPDATE' and NEW.claimed_values is not distinct from OLD.claimed_values then
    return NEW;
  end if;

  -- Run the consensus + resolve + promote chain for this fid.
  -- compute_beach_field_consensus is field-agnostic (recomputes all);
  -- _resolve and promote here are dogs-specific.
  perform public.compute_beach_field_consensus(NEW.gold_fid);
  perform public._resolve_dogs_gold(NEW.gold_fid);
  perform public.promote_canonical_dogs_to_beach_dog_policy(NEW.gold_fid, 0.5);

  return NEW;
end $$;

drop trigger if exists tg_after_change_promote_dogs_chain
  on public.beach_enrichment_provenance;

create trigger tg_after_change_promote_dogs_chain
  after insert or update on public.beach_enrichment_provenance
  for each row
  when (NEW.field_group = 'dogs' and NEW.gold_fid is not null)
  execute function public.tg_promote_dogs_chain();

commit;
