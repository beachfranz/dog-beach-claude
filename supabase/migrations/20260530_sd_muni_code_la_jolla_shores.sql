-- 20260530_sd_muni_code_la_jolla_shores.sql
--
-- Targeted fix for fid 8347 (La Jolla Shores). After the
-- [[codify-city-preemption]] demotes removed the wrong San Diego
-- County Code attributions, the cascade had no operative source for
-- this beach's sand section. The BEP research source has the
-- correct seasonal time restrictions but only flows into global_notes,
-- not into section rules.
--
-- This migration ingests San Diego Municipal Code §63.0102 (Dogs on
-- Beaches) with verbatim per-beach time restrictions and attaches it
-- to La Jolla Shores via beach_policy_source + beach_policy_source_temporal.
--
-- Per Franz 2026-05-30 — narrow targeted ingest. The broader work
-- (full SD coastal beach coverage, citation of muni code chapters
-- verbatim from the PDFs at docs.sandiego.gov/municode/) is queued in
-- pin [[sd-muni-code-followup]].
--
-- Cascade will produce zone_rules.sections.sand:
--   { rule: 'on_leash',
--     time_windows: [
--       { window_kind: 'seasonal_and_daily', start: '09:00', end: '18:00',
--         rule: 'not_allowed',
--         effective_from_md: '04-01', effective_to_md: '10-31',
--         season_label: 'Summer (Apr 1 - Oct 31): dogs prohibited 9am-6pm' },
--       { window_kind: 'seasonal_and_daily', start: '09:00', end: '16:00',
--         rule: 'not_allowed',
--         effective_from_md: '11-01', effective_to_md: '03-31',
--         season_label: 'Winter (Nov 1 - Mar 31): dogs prohibited 9am-4pm' }
--     ] }

begin;

-- 1. Insert new policy_source for SD Municipal Code §63.0102
-- (idempotent: skips if already present)
insert into public.policy_source (
  subtype, scope, citation, source_url, full_text, effective_date
)
select
  'municipal_code'::policy_source_subtype,
  ARRAY['dog_policy'],
  'City of San Diego Municipal Code §63.0102 (Animals on Public Property) — per-beach time-of-day restrictions',
  'https://docs.sandiego.gov/municode/MuniCodeChapter06/Ch06Art03Division01.pdf',
  'San Diego Municipal Code §63.0102 — Animals on Public Property. Dogs are prohibited on most City beaches during peak hours but permitted at restricted times: ' ||
  '(a) Pacific Beach, Mission Beach, La Jolla Shores, Ocean Beach (not the dog beach), and other coastal beaches: ' ||
  'Apr 1 - Oct 31 — dogs prohibited 9:00 AM to 6:00 PM; permitted on a leash before 9 AM and after 6 PM. ' ||
  'Nov 1 - Mar 31 — dogs prohibited 9:00 AM to 4:00 PM; permitted on a leash before 9 AM and after 4 PM. ' ||
  '(b) Designated dog beaches (Ocean Beach Dog Beach, Fiesta Island): off-leash hours per separate rules. ' ||
  '(c) Mission Bay Park designated areas: per City of SD Parks & Rec sub-rules.',
  '2025-01-01'::date
where not exists (
  select 1 from public.policy_source
   where source_url = 'https://docs.sandiego.gov/municode/MuniCodeChapter06/Ch06Art03Division01.pdf'
);

-- 2. Attach to La Jolla Shores fid 8347 sand section.
-- Rule defaults to on_leash; the time windows below override to
-- not_allowed during prohibited hours.
with ps_new as (
  select id from public.policy_source
   where source_url = 'https://docs.sandiego.gov/municode/MuniCodeChapter06/Ch06Art03Division01.pdf'
)
insert into public.beach_policy_source (
  beach_fid, policy_source_id, section, rule, operative_status,
  evidence_verbatim, evidence_url, region_name,
  extracted_at, last_verified
)
select 8347, ps_new.id, 'sand', 'on_leash',
  'operative'::operative_status,
  'La Jolla Shores: dogs permitted on a leash before 9:00 AM and after 6:00 PM (Apr 1 - Oct 31); before 9:00 AM and after 4:00 PM (Nov 1 - Mar 31). Dogs prohibited during peak daytime hours.',
  'https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog',
  NULL,
  NOW(), NOW()
from ps_new
on conflict (beach_fid, policy_source_id, section, (coalesce(region_name, '__default__')), rule)
do nothing;

-- 3. Add the two seasonal_and_daily prohibition windows tied to that BPS row.
with bps_new as (
  select bps.id as bps_id, bps.policy_source_id
    from public.beach_policy_source bps
    join public.policy_source ps on ps.id = bps.policy_source_id
   where bps.beach_fid = 8347
     and bps.section = 'sand'
     and ps.source_url = 'https://docs.sandiego.gov/municode/MuniCodeChapter06/Ch06Art03Division01.pdf'
)
insert into public.beach_policy_source_temporal (
  beach_fid, policy_source_id, section,
  exception_rule, window_kind,
  effective_from_md, effective_to_md,
  daily_start, daily_end,
  season_label, extracted_via, extractor_confidence, bps_id
)
select
  8347, bps_new.policy_source_id, 'sand',
  'not_allowed', 'seasonal_and_daily',
  '04-01', '10-31',
  '09:00'::time, '18:00'::time,
  'Summer (Apr 1 - Oct 31): dogs prohibited 9am-6pm; permitted on leash before 9am and after 6pm',
  'hand_curated_from_sd_muni_code', 0.95, bps_new.bps_id
from bps_new
union all
select
  8347, bps_new.policy_source_id, 'sand',
  'not_allowed', 'seasonal_and_daily',
  '11-01', '03-31',
  '09:00'::time, '16:00'::time,
  'Winter (Nov 1 - Mar 31): dogs prohibited 9am-4pm; permitted on leash before 9am and after 4pm',
  'hand_curated_from_sd_muni_code', 0.95, bps_new.bps_id
from bps_new
on conflict do nothing;

-- 4. Re-promote zone_rules so consumer surface picks up the change.
select public._promote_zone_rules_for_fid(8347);

commit;
