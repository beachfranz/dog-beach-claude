-- 20260506_phase_view_dogs_terminal.sql
--
-- New gating rule (Franz, 2026-05-06):
--   * dogs_allowed = 'no'   -> processes phases 01..05 then is TERMINAL.
--   * dogs_allowed != 'no'  -> processes phases 01..06 (06 = scored).
--
-- Old phase view short-circuited dogs='no' beaches into 00_not_scoreable
-- and they were skipped by every upstream sweep (extraction, consensus,
-- zone_rules promote). Under the new rule those phases must run for
-- every active beach; only the score fan-out (phase 05 -> 06) gates on
-- is_scoreable.
--
-- Phase set (revised):
--   00_inactive                 -- is_active=false
--   01_no_evidence              -- no BEP rows for dogs field group
--   02_evidence_only            -- BEP exists, no policy row
--   03_canonical_pending        -- policy row, dogs_allowed still null
--   04_dogs_resolved_no_zones   -- dogs_allowed filled, no zone_rules
--   05_terminal_no_dogs         -- TERMINAL: dogs='no' + zone_rules promoted
--   05_zone_rules_pending_score -- dogs!='no' + zone_rules + no recent score
--   06_complete                 -- recent score in last 36h (dogs!='no' only)
--
-- 00_not_scoreable is retired -- no beach should land there under the
-- new rule. The set of beaches blocked from phase 06 is now derivable
-- as `phase = '05_terminal_no_dogs'`.

begin;

drop view if exists public.beach_pipeline_status cascade;
create view public.beach_pipeline_status as
with base as (
  select
    bg.fid,
    bg.name,
    bg.county_name,
    bg.is_active,
    bg.is_scoreable,
    bg.location_id,
    -- evidence
    exists (
      select 1 from public.beach_enrichment_provenance e
       where e.gold_fid = bg.fid and e.field_group = 'dogs'
    ) as has_bep_dogs,
    exists (
      select 1 from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_policy_row,
    (
      select bool_or(p.dogs_allowed is not null)
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_dogs_allowed,
    (
      select bool_or(p.dogs_allowed = 'no')
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as dogs_explicitly_no,
    (
      select bool_or(p.zone_rules is not null)
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_zone_rules,
    exists (
      select 1 from public.beach_day_recommendations r
       where r.arena_group_id = bg.fid
         and r.local_date >= current_date - interval '1 day'
    ) as has_recent_score,
    (
      select max(e.updated_at)
        from public.beach_enrichment_provenance e
       where e.gold_fid = bg.fid and e.field_group = 'dogs'
    ) as last_bep_at,
    (
      select max(coalesce(p.zone_rules_updated_at, p.curated_at))
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as last_policy_at
  from public.beaches_gold bg
)
select b.*,
  case
    when not is_active                                         then '00_inactive'
    when not has_bep_dogs                                      then '01_no_evidence'
    when not has_policy_row                                    then '02_evidence_only'
    when not coalesce(has_dogs_allowed, false)                 then '03_canonical_pending'
    when not coalesce(has_zone_rules, false)                   then '04_dogs_resolved_no_zones'
    when coalesce(dogs_explicitly_no, false)                   then '05_terminal_no_dogs'
    when not has_recent_score                                  then '05_zone_rules_pending_score'
    else                                                            '06_complete'
  end as phase,
  greatest(coalesce(last_bep_at, '1970-01-01'::timestamptz),
           coalesce(last_policy_at, '1970-01-01'::timestamptz)) as last_touched_at
from base b;

grant select on public.beach_pipeline_status to anon, authenticated;

drop view if exists public.beach_pipeline_phase_counts;
create view public.beach_pipeline_phase_counts as
select phase, count(*) as n
  from public.beach_pipeline_status
 group by phase
 order by phase;

grant select on public.beach_pipeline_phase_counts to anon, authenticated;

-- Recreate activity feed (depended on the dropped view via cascade)
drop view if exists public.beach_pipeline_activity_recent;
create view public.beach_pipeline_activity_recent as
(
  select 'evidence_added' as event,
         e.updated_at as event_at,
         e.gold_fid as fid,
         bg.name as beach_name,
         e.source as detail,
         null::text as field_name
    from public.beach_enrichment_provenance e
    join public.beaches_gold bg on bg.fid = e.gold_fid
   where e.field_group = 'dogs'
   order by e.updated_at desc
   limit 25
)
union all
(
  select 'zone_rules_promoted' as event,
         p.zone_rules_updated_at as event_at,
         p.arena_group_id as fid,
         bg.name as beach_name,
         null::text as detail,
         'zone_rules' as field_name
    from public.beach_dog_policy p
    join public.beaches_gold bg on bg.fid = p.arena_group_id
   where p.zone_rules_updated_at is not null
   order by p.zone_rules_updated_at desc
   limit 25
)
union all
(
  select 'recommendation_landed' as event,
         r.updated_at as event_at,
         r.arena_group_id as fid,
         bg.name as beach_name,
         r.day_status as detail,
         'recommendation' as field_name
    from public.beach_day_recommendations r
    join public.beaches_gold bg on bg.fid = r.arena_group_id
   where r.local_date = current_date
     and r.updated_at > now() - interval '6 hours'
   order by r.updated_at desc
   limit 15
)
order by event_at desc nulls last
limit 50;

grant select on public.beach_pipeline_activity_recent to anon, authenticated;

commit;
