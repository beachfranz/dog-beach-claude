-- 20260506_beach_dog_policy_zone_rules.sql
--
-- Step 6 of zone_rules build order — schema migration + auto-promoter.
-- See project_zone_rules_design_locked.md.
--
-- 442 rows of structured zone_rules jsonb sit in beach_enrichment_provenance
-- (source='text_repass_v1', from the closed_zones_v3 production text-repass
-- run on 2026-05-04). They've never reached the consumer surface because
-- (a) beach_dog_policy doesn't have a zone_rules column, and (b) no
-- promoter has been written for this source/field combination.
--
-- This migration:
--   1. Adds  beach_dog_policy.zone_rules jsonb + zone_rules_updated_at
--   2. Defines _promote_zone_rules_for_fid(p_fid) — copies the latest
--      text_repass_v1 zone_rules into beach_dog_policy.zone_rules
--   3. Bulk-runs the promoter for all 442 fids
--   4. Wires an AFTER INSERT/UPDATE trigger so future text_repass_v1
--      rows auto-promote (mirrors the json_structured_explode pattern)
--
-- Single-source-of-truth note: text_repass_v1 is currently the only source
-- producing zone_rules. When other sources start emitting zone_rules,
-- promote logic will need a consensus pass (per project_consensus_ensemble.md).
-- For now: text_repass_v1 wins.

begin;

-- ----------------------------------------------------------------------
-- 1. Schema
-- ----------------------------------------------------------------------
alter table public.beach_dog_policy
  add column if not exists zone_rules jsonb,
  add column if not exists zone_rules_updated_at timestamptz;

create index if not exists beach_dog_policy_zone_rules_gin
  on public.beach_dog_policy using gin (zone_rules);

comment on column public.beach_dog_policy.zone_rules is
  'Section-aware dog policy v2 (LOCKED design 2026-05-04). Schema: '
  '{seasons: [{name, dates: {start, end} | null, regions: [{name | null, '
  'evidence, time_windows?, sections: {<section_name>: {rule: '
  'off_leash|on_leash|not_allowed|unknown, time_windows?, evidence?, '
  'sub_zones?}}}]}], global_notes, extraction_method}. '
  'Section taxonomy: sand, trails, boardwalk, bluff, dunes, picnic_area, '
  'campground, playground, tide_pools, nesting_zones. '
  'See project_zone_rules_design_locked.md for full spec.';

-- ----------------------------------------------------------------------
-- 2. Per-fid promoter
-- ----------------------------------------------------------------------
create or replace function public._promote_zone_rules_for_fid(p_fid int)
returns text
language plpgsql
security definer
as $$
declare
  v_zr jsonb;
  v_updated boolean;
begin
  -- Take the most-recently-updated text_repass_v1 row for this fid that
  -- carries a zone_rules payload. (If multiple text_repass passes ever
  -- run, the latest wins.)
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

  -- If beach_dog_policy has no row for this fid yet, create a minimal one
  -- holding just zone_rules. Real curated columns get filled by other paths.
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

revoke all on function public._promote_zone_rules_for_fid(int) from public, anon, authenticated;
grant  execute on function public._promote_zone_rules_for_fid(int) to service_role;

-- ----------------------------------------------------------------------
-- 3. Bulk promoter — initial run + future re-runs
-- ----------------------------------------------------------------------
create or replace function public.promote_all_zone_rules_from_text_repass()
returns table (rows_updated int, distinct_fids int)
language plpgsql
as $func$
declare
  v_fid int;
  v_updated int := 0;
  v_total int := 0;
begin
  for v_fid in
    select distinct gold_fid
      from public.beach_enrichment_provenance
     where source = 'text_repass_v1'
       and claimed_values ? 'zone_rules'
       and gold_fid is not null
  loop
    v_total := v_total + 1;
    if public._promote_zone_rules_for_fid(v_fid) = 'updated' then
      v_updated := v_updated + 1;
    end if;
  end loop;
  return query select v_updated, v_total;
end
$func$;

grant execute on function public.promote_all_zone_rules_from_text_repass() to service_role;

-- ----------------------------------------------------------------------
-- 4. Trigger — auto-promote on future text_repass_v1 inserts/updates
-- ----------------------------------------------------------------------
create or replace function public.tg_after_change_promote_zone_rules()
returns trigger
language plpgsql
as $$
begin
  if NEW.gold_fid is null then return NEW; end if;
  perform public._promote_zone_rules_for_fid(NEW.gold_fid);
  return NEW;
end $$;

drop trigger if exists tg_after_change_promote_zone_rules on public.beach_enrichment_provenance;
create trigger tg_after_change_promote_zone_rules
  after insert or update on public.beach_enrichment_provenance
  for each row
  when (NEW.source = 'text_repass_v1' and NEW.claimed_values ? 'zone_rules')
  execute function public.tg_after_change_promote_zone_rules();

-- ----------------------------------------------------------------------
-- 5. Run the bulk promoter now
-- ----------------------------------------------------------------------
select * from public.promote_all_zone_rules_from_text_repass();

commit;
