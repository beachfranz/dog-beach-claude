-- 20260506_beach_pipeline_status.sql
--
-- Per-beach view of which pipeline phase the beach has reached, plus a
-- recent-activity view feeding the admin monitor page. Each gold fid
-- lands in exactly one phase based on what's been populated downstream.
--
-- Phases (in order):
--   00_inactive             — is_active=false (not in scope)
--   00_not_scoreable        — active but is_scoreable=false (out of scope)
--   01_no_evidence          — scoreable, but no BEP rows for dogs field group
--   02_evidence_only        — has BEP rows, but no row in beach_dog_policy
--   03_canonical_pending    — has policy row, but dogs_allowed still null
--   04_dogs_allowed_only    — dogs_allowed filled, but no zone_rules
--   05_zone_rules_no_score  — full policy + zone_rules, but no recent scoring
--   06_complete             — everything above + a recommendation row in last 36h
--
-- The "complete" path is the soup-to-nuts target: landing tables ->
-- consumer surface, end to end.

begin;

drop view if exists public.beach_pipeline_status;
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
    -- canonical row in beach_dog_policy
    exists (
      select 1 from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_policy_row,
    -- core canonical column filled
    (
      select bool_or(p.dogs_allowed is not null)
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_dogs_allowed,
    -- zone_rules promoted
    (
      select bool_or(p.zone_rules is not null)
        from public.beach_dog_policy p
       where p.arena_group_id = bg.fid
    ) as has_zone_rules,
    -- recent scoring
    exists (
      select 1 from public.beach_day_recommendations r
       where r.arena_group_id = bg.fid
         and r.local_date >= current_date - interval '1 day'
    ) as has_recent_score,
    -- timestamps for "last touched" feed
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
    when not is_active                                    then '00_inactive'
    when not is_scoreable                                 then '00_not_scoreable'
    when not has_bep_dogs                                 then '01_no_evidence'
    when not has_policy_row                               then '02_evidence_only'
    when not coalesce(has_dogs_allowed, false)            then '03_canonical_pending'
    when not coalesce(has_zone_rules, false)              then '04_dogs_allowed_only'
    when not has_recent_score                             then '05_zone_rules_no_score'
    else                                                       '06_complete'
  end as phase,
  greatest(coalesce(last_bep_at, '1970-01-01'::timestamptz),
           coalesce(last_policy_at, '1970-01-01'::timestamptz)) as last_touched_at
from base b;

grant select on public.beach_pipeline_status to anon, authenticated;

-- Phase counts — what the monitor's top bar chart reads
drop view if exists public.beach_pipeline_phase_counts;
create view public.beach_pipeline_phase_counts as
select phase, count(*) as n
  from public.beach_pipeline_status
 group by phase
 order by phase;

grant select on public.beach_pipeline_phase_counts to anon, authenticated;

-- Recent activity feed — last 50 events, mixed across signals
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
