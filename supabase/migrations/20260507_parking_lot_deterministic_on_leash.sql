-- 20260507_parking_lot_deterministic_on_leash.sql
--
-- Admin policy: parking lots are always on-leash. Apply this at the
-- zone_rules promote layer so:
--
--   1. Any region whose `sections.parking_lot.rule` differs from
--      'on_leash' gets normalized to on_leash.
--   2. Future closed_zones_v3 extractions that emit conflicting
--      parking_lot rules get auto-corrected on promote.
--   3. The beach-surface binaries (has_on_leash / has_off_leash)
--      already exclude parking_lot via _emit_evidence_from_zone_rules_derived's
--      on-beach / perimeter section split — no change there.
--
-- Idempotent: writing on_leash twice is a no-op.

begin;

-- ============================================================
-- 1. Per-region helper — force parking_lot.rule = on_leash
-- ============================================================
create or replace function public._region_force_parking_on_leash(p_region jsonb)
returns jsonb language sql immutable as $$
  select case
    when p_region is null then null
    when jsonb_typeof(p_region->'sections') = 'object'
         and (p_region->'sections') ? 'parking_lot'
      then jsonb_set(p_region, '{sections,parking_lot,rule}', '"on_leash"'::jsonb)
    else p_region
  end
$$;

-- ============================================================
-- 2. Top-level helper — handles both v1 (regions[]) and v2 (seasons[].regions[])
-- ============================================================
create or replace function public._inject_parking_on_leash(p_zr jsonb)
returns jsonb language sql immutable as $$
  select case
    when p_zr is null then null
    when p_zr ? 'seasons' then
      jsonb_set(p_zr, '{seasons}',
        coalesce((
          select jsonb_agg(
            jsonb_set(s, '{regions}', coalesce((
              select jsonb_agg(public._region_force_parking_on_leash(r))
              from jsonb_array_elements(s->'regions') r
            ), '[]'::jsonb))
          )
          from jsonb_array_elements(p_zr->'seasons') s
        ), '[]'::jsonb))
    when p_zr ? 'regions' then
      jsonb_set(p_zr, '{regions}',
        coalesce((
          select jsonb_agg(public._region_force_parking_on_leash(r))
          from jsonb_array_elements(p_zr->'regions') r
        ), '[]'::jsonb))
    else p_zr
  end
$$;

-- ============================================================
-- 3. Extend the zone_rules promoter to apply the rule on write
-- ============================================================
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

  -- Apply admin policy: parking_lot rule -> on_leash
  v_zr := public._inject_parking_on_leash(v_zr);

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

-- ============================================================
-- 4. One-shot backfill — apply rule to every existing zone_rules
-- ============================================================
update public.beach_dog_policy
   set zone_rules = public._inject_parking_on_leash(zone_rules),
       zone_rules_updated_at = now()
 where zone_rules is not null
   and zone_rules is distinct from public._inject_parking_on_leash(zone_rules);

commit;
