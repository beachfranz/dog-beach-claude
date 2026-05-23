-- Governance early-vs-late assignment audit
-- ------------------------------------------
-- Goal: measure how often the post-URL-harvest resolver disagrees with
-- what the spatial-only early pass would have produced, to decide
-- whether deferring the canonical promotion is worthwhile.

-- Q1. Coverage: how many beaches have spatial-only evidence vs park_url
--     evidence vs both?
with
  spatial as (
    select distinct fid from beach_enrichment_provenance
    where field_group = 'governance'
      and source in (
        'cpad','tiger_places','csp_parks','nps_places',
        'tribal_lands','military_bases','park_operators','name'
      )
  ),
  park_url as (
    select distinct fid from beach_enrichment_provenance
    where field_group = 'governance'
      and source in ('park_url','park_url_buffer_attribution')
  )
select
  (select count(*) from spatial)                                    as has_spatial,
  (select count(*) from park_url)                                   as has_park_url,
  (select count(*) from spatial s join park_url p on s.fid=p.fid)   as has_both,
  (select count(*) from spatial s
     where not exists (select 1 from park_url p where p.fid=s.fid)) as spatial_only,
  (select count(*) from park_url p
     where not exists (select 1 from spatial s where s.fid=p.fid))  as park_url_only;

-- Q2. Canonical-source distribution across the WHOLE corpus.
--     Tells us which sources are actually winning at promotion time.
select
  source,
  count(*) as beaches_with_canonical
from beach_enrichment_provenance
where field_group = 'governance' and is_canonical = true
group by source
order by count(*) desc;

-- Q3. Among beaches that received park_url evidence, what's the
--     canonical-source distribution? If most canonicals are still
--     spatial sources, the override rarely fires.
with had_park_url as (
  select distinct fid from beach_enrichment_provenance
  where field_group = 'governance'
    and source in ('park_url','park_url_buffer_attribution')
)
select
  bep.source as canonical_source,
  count(*)   as beaches
from beach_enrichment_provenance bep
join had_park_url h on h.fid = bep.fid
where bep.field_group = 'governance' and bep.is_canonical = true
group by bep.source
order by count(*) desc;

-- Q4. Name agreement: among beaches with BOTH a park_url-asserted
--     governing_body_name AND any spatial-asserted name, does the
--     park_url name match at least one spatial name (case-insensitive)?
with
  park_url_names as (
    select distinct on (fid)
      fid,
      lower(trim(claimed_values->>'governing_body_name')) as pu_name
    from beach_enrichment_provenance
    where field_group = 'governance'
      and source in ('park_url','park_url_buffer_attribution')
      and claimed_values->>'governing_body_name' is not null
    order by fid, confidence desc nulls last
  ),
  spatial_names as (
    select
      fid,
      lower(trim(coalesce(
        claimed_values->>'name',
        claimed_values->>'governing_body_name'
      ))) as sp_name
    from beach_enrichment_provenance
    where field_group = 'governance'
      and source in (
        'cpad','tiger_places','csp_parks','nps_places',
        'tribal_lands','military_bases','park_operators'
      )
      and coalesce(
        claimed_values->>'name',
        claimed_values->>'governing_body_name'
      ) is not null
  )
select
  count(distinct pu.fid)                                              as beaches_with_both_named,
  count(distinct pu.fid) filter (where exists (
    select 1 from spatial_names sp where sp.fid=pu.fid and sp.sp_name=pu.pu_name
  ))                                                                  as park_url_matches_some_spatial,
  count(distinct pu.fid) filter (where not exists (
    select 1 from spatial_names sp where sp.fid=pu.fid and sp.sp_name=pu.pu_name
  ))                                                                  as park_url_disagrees_with_all_spatial
from park_url_names pu;

-- Q5. Spot-check the disagreements: 15 examples where park_url name
--     differs from every spatial name. Useful for sanity-checking
--     whether the disagreements are real (override-worthy) or noise
--     (capitalization / "City of X" vs "X" string variations).
with
  park_url_names as (
    select distinct on (fid)
      fid,
      claimed_values->>'governing_body_name' as pu_name
    from beach_enrichment_provenance
    where field_group = 'governance'
      and source in ('park_url','park_url_buffer_attribution')
      and claimed_values->>'governing_body_name' is not null
    order by fid, confidence desc nulls last
  ),
  spatial_names as (
    select
      fid,
      string_agg(
        source || ':' || coalesce(
          claimed_values->>'name',
          claimed_values->>'governing_body_name'
        ),
        ' | '
      ) as spatial_names
    from beach_enrichment_provenance
    where field_group = 'governance'
      and source in (
        'cpad','tiger_places','csp_parks','nps_places',
        'tribal_lands','military_bases','park_operators'
      )
      and coalesce(
        claimed_values->>'name',
        claimed_values->>'governing_body_name'
      ) is not null
    group by fid
  )
select
  pu.fid,
  ls.display_name,
  pu.pu_name as park_url_name,
  sp.spatial_names
from park_url_names pu
join spatial_names sp on sp.fid = pu.fid
join locations_stage ls on ls.fid = pu.fid
where lower(trim(pu.pu_name)) <> all (
  select lower(trim(coalesce(
    claimed_values->>'name',
    claimed_values->>'governing_body_name'
  )))
  from beach_enrichment_provenance
  where fid = pu.fid
    and field_group = 'governance'
    and source in (
      'cpad','tiger_places','csp_parks','nps_places',
      'tribal_lands','military_bases','park_operators'
    )
    and coalesce(
      claimed_values->>'name',
      claimed_values->>'governing_body_name'
    ) is not null
)
order by pu.fid
limit 15;
