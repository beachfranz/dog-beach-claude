-- 20260508_dedup_richness_score.sql
--
-- Late-stage dedup richness scorer (Pin #29). Computes a 0-100ish int score
-- per beaches_gold fid measuring how much enrichment + curator signal +
-- consumer-facing data is attached to that row. Used to pick winners
-- when multiple gold rows belong to the same arena group_id.
--
-- Weights validated 2026-05-08 on Marin's 9 known dupe pairs (with one
-- adjustment after Franz review: zone_rules bumped from 10 to 20, since
-- it's the #2 strategic asset).

begin;

create or replace function public._dedup_richness_score(p_fid bigint)
returns int
language sql
stable
security definer
set search_path to 'public'
as $$
  select
    -- + 20 manual_curator BEP rows (human-blessed truth)
    (case when exists(select 1 from public.beach_enrichment_provenance
                      where gold_fid = p_fid and source = 'manual_curator') then 20 else 0 end)
    -- + 20 zone_rules JSONB populated (#2 strategic asset)
    + (case when (select zone_rules from public.beach_dog_policy
                  where arena_group_id = p_fid) is not null then 20 else 0 end)
    -- + 15 curator was_moved=true
    + (case when exists(select 1 from public.beach_location_curation_status
                        where arena_group_id = p_fid and was_moved) then 15 else 0 end)
    -- + 10 has clear operator (park_url / park_operators / operator_default BEP source)
    + (case when exists(select 1 from public.beach_enrichment_provenance
                        where gold_fid = p_fid
                          and source in ('park_operators','operator_default','park_url',
                                         'park_url_buffer_attribution')) then 10 else 0 end)
    -- + 8 has dogs_prohibited_start (real schedule)
    + (case when (select dogs_prohibited_start from public.beach_dog_policy
                  where arena_group_id = p_fid) is not null then 8 else 0 end)
    -- + 5 has beach_dog_policy with non-null dogs_allowed
    + (case when exists(select 1 from public.beach_dog_policy
                        where arena_group_id = p_fid and dogs_allowed is not null) then 5 else 0 end)
    -- + 5 has photos
    + (case when exists(select 1 from public.beach_photos
                        where arena_group_id = p_fid) then 5 else 0 end)
    -- + 3 is_scoreable
    + (case when (select is_scoreable from public.beaches_gold
                  where fid = p_fid) then 3 else 0 end)
    -- + 2 has noaa_station_id
    + (case when (select noaa_station_id from public.beaches_gold
                  where fid = p_fid) is not null then 2 else 0 end)
    -- + 1 per BEP row, capped at 5
    + least((select count(*) from public.beach_enrichment_provenance
             where gold_fid = p_fid)::int, 5)
    -- - 5 location_id ends with -<fid> (sloppy slug from later promoter)
    - (case when (select location_id from public.beaches_gold where fid = p_fid)
              ~ ('-' || p_fid::text || '$') then 5 else 0 end);
$$;

grant execute on function public._dedup_richness_score(bigint) to anon, authenticated, service_role;

commit;
