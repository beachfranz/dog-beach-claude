-- 20260512_state_coastal_region_clusters.sql
--
-- Defines coastal-region clusters and adds a cluster fallback to
-- refresh_catchment_state_pct. Today the function returns NULL for any
-- state with <5 scored beaches, which prevents those states from
-- receiving a scoring_tier. When MS/AL/FL/etc come online with sparse
-- coverage, we want them ranked relative to their region's distribution
-- rather than the unfair national distribution.
--
-- Regional groupings:
--   NE Atlantic — ME, NH, MA, RI, CT, NY, NJ, PA, DE, MD, VA, DC, WV
--   SE Atlantic — NC, SC, GA, FL
--   Gulf Coast  — AL, MS, LA, TX
--   Pacific     — CA, OR, WA, AK, HI
--   Great Lakes — MN, WI, IL, IN, MI, OH
--   (states without coastline / great-lakes shores omitted)
--
-- Cluster choice reflects similar coastal-tourism-density profiles, not
-- pure geographic adjacency. FL goes to SE (dominant coastline orientation).
-- NY goes to NE Atlantic (Long Island is more characteristic than Great
-- Lakes shore).

begin;

create table if not exists public.state_coastal_region (
  state_code text primary key,
  region     text not null,
  notes      text
);

insert into public.state_coastal_region (state_code, region) values
  -- NE Atlantic
  ('ME','NE_Atlantic'),('NH','NE_Atlantic'),('MA','NE_Atlantic'),
  ('RI','NE_Atlantic'),('CT','NE_Atlantic'),('NY','NE_Atlantic'),
  ('NJ','NE_Atlantic'),('PA','NE_Atlantic'),('DE','NE_Atlantic'),
  ('MD','NE_Atlantic'),('VA','NE_Atlantic'),('DC','NE_Atlantic'),
  ('WV','NE_Atlantic'),
  -- SE Atlantic
  ('NC','SE_Atlantic'),('SC','SE_Atlantic'),('GA','SE_Atlantic'),
  ('FL','SE_Atlantic'),
  -- Gulf
  ('AL','Gulf'),('MS','Gulf'),('LA','Gulf'),('TX','Gulf'),
  -- Pacific
  ('CA','Pacific'),('OR','Pacific'),('WA','Pacific'),
  ('AK','Pacific'),('HI','Pacific'),
  -- Great Lakes
  ('MN','Great_Lakes'),('WI','Great_Lakes'),('IL','Great_Lakes'),
  ('IN','Great_Lakes'),('MI','Great_Lakes'),('OH','Great_Lakes')
on conflict (state_code) do nothing;

-- Rewrite refresh_catchment_state_pct to fall back to cluster rank for
-- low-N states. Logic:
--   - If state has ≥5 scored beaches → use within-state percentile
--   - Else if state's cluster has ≥5 scored beaches → use within-cluster
--   - Else → leave NULL (no signal yet)
drop function if exists public.refresh_catchment_state_pct(text);
create or replace function public.refresh_catchment_state_pct(p_state text default null)
returns table(state_code text, source text, n_updated bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
begin
  return query
  with eligible as (
    select g.fid, g.state as st, g.catchment_score,
           r.region as cluster_code
      from public.beaches_gold g
      left join public.state_coastal_region r on r.state_code = g.state
     where g.is_active and g.catchment_score is not null
       and g.state is not null
       and (p_state is null or g.state = p_state)
  ),
  state_counts as (
    select st, count(*) as n from eligible group by st
  ),
  cluster_counts as (
    select cluster_code, count(*) as n
      from eligible where cluster_code is not null
     group by cluster_code
  ),
  scored_state as (
    select e.fid, e.st, 'state'::text as source,
           percent_rank() over (partition by e.st
                                order by e.catchment_score) as pct
      from eligible e
      join state_counts sc on sc.st = e.st
     where sc.n >= 5
  ),
  scored_cluster as (
    select e.fid, e.st, 'cluster'::text as source,
           percent_rank() over (partition by e.cluster_code
                                order by e.catchment_score) as pct
      from eligible e
      join state_counts sc on sc.st = e.st
      join cluster_counts cc on cc.cluster_code = e.cluster_code
     where sc.n < 5 and cc.n >= 5
  ),
  combined as (
    select * from scored_state
    union all
    select * from scored_cluster
  ),
  upd as (
    update public.beaches_gold g
       set catchment_state_pct = combined.pct,
           catchment_state_pct_updated_at = now()
      from combined
     where g.fid = combined.fid
       and (g.catchment_state_pct is distinct from combined.pct
            or g.catchment_state_pct_updated_at is null)
    returning g.state as st, combined.source as source
  )
  select upd.st, upd.source, count(*)::bigint
    from upd group by upd.st, upd.source
   order by upd.st;
end;
$function$;

grant execute on function public.refresh_catchment_state_pct(text) to service_role;

commit;
