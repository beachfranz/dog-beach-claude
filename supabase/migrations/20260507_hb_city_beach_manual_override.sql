-- 20260507_hb_city_beach_manual_override.sql
--
-- Manual override for fid 8901 (Huntington City Beach):
--   1. Patches global_notes prose to correctly identify operator
--      as City of Huntington Beach (not CDPR — the LLM extraction
--      was misled by a high-confidence state-parks source that
--      doesn't actually govern this city beach).
--   2. Sets source='manual_curator' on the bdp row to prevent the
--      consensus promoter from overwriting.
--
-- Also patches `_promote_zone_rules_for_fid` to skip rows where
-- bdp.source='manual_curator' so the zone_rules JSONB doesn't get
-- overwritten by future text_repass runs. (The dogs-policy promoter
-- already had this skip; zone_rules promoter didn't.)

begin;

-- 1. Patch the prose for HB City Beach
update public.beach_dog_policy
   set zone_rules = jsonb_set(zone_rules, '{global_notes}',
     to_jsonb(
       'Huntington City Beach is operated by the City of Huntington Beach. ' ||
       'Dogs are prohibited on sand and water within the city beach footprint, ' ||
       'except at the designated Dog Beach carve-out at 100 Goldenwest Street ' ||
       '(a separate beach in our catalog) where off-leash play is allowed. ' ||
       'Leashed dogs are permitted on multi-use trails and in parking lots. ' ||
       'Service animals are excepted from all restrictions. Adjacent ' ||
       'state-operated beaches (Bolsa Chica State Beach to the north, ' ||
       'Huntington State Beach to the south) have their own rules and are ' ||
       'separate catalog entries.'
     )),
       source = 'manual_curator',
       curated_at = now()
 where arena_group_id = (select group_id from public.beaches_gold where fid = 8901);

-- 2. Patch _promote_zone_rules_for_fid to respect manual_curator
create or replace function public._promote_zone_rules_for_fid(p_fid bigint)
returns text language plpgsql security definer as $$
declare
  v_zr     jsonb;
  v_updated boolean;
  v_source text;
begin
  -- Don't overwrite manually-curated zone_rules
  select source into v_source from public.beach_dog_policy
   where arena_group_id = p_fid;
  if v_source = 'manual_curator' then
    return 'skipped — manual_curator';
  end if;

  select claimed_values->'zone_rules' into v_zr
    from public.beach_enrichment_provenance
   where source = 'text_repass_v1' and gold_fid = p_fid
     and claimed_values ? 'zone_rules'
   order by updated_at desc limit 1;

  if v_zr is null then
    v_zr := public._zr_inject_perimeter('{}'::jsonb, p_fid);
    if not (v_zr ? 'regions') and not (v_zr ? 'seasons') then
      return 'no zone_rules for fid';
    end if;
  else
    v_zr := public._inject_parking_on_leash(v_zr);
    v_zr := public._zr_inject_perimeter(v_zr, p_fid);
  end if;

  update public.beach_dog_policy
     set zone_rules = v_zr, zone_rules_updated_at = now()
   where arena_group_id = p_fid and (zone_rules is distinct from v_zr);
  v_updated := found;

  if not v_updated and not exists (select 1 from public.beach_dog_policy where arena_group_id = p_fid) then
    insert into public.beach_dog_policy (arena_group_id, zone_rules, zone_rules_updated_at)
    values (p_fid, v_zr, now())
    on conflict (arena_group_id) do update
      set zone_rules = excluded.zone_rules, zone_rules_updated_at = excluded.zone_rules_updated_at;
    v_updated := true;
  end if;

  return case when v_updated then 'updated' else 'no-op' end;
end $$;

revoke all on function public._promote_zone_rules_for_fid(bigint) from public, anon, authenticated;
grant  execute on function public._promote_zone_rules_for_fid(bigint) to service_role;

commit;
