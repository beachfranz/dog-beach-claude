-- Per-beach comparison view: what each external source says vs our model.
--
-- One row per beach (origin_key). Columns roll up each source's verdict
-- so you can see at-a-glance "BringFido yes, CB off_leash, DT no, us yes".
-- Useful for the agreement matrix and for surfacing disagreement rows.
--
-- External rules collapse to (yes / no / unknown) for comparison:
--   off_leash | leash | yes  → 'yes'
--   no                       → 'no'
--   unknown                  → null

create or replace view public.truth_comparison_v
with (security_invoker = true)
as
with externals as (
  select te.matched_origin_key as origin_key,
         te.source,
         te.dogs_rule as raw_rule,
         case
           when te.dogs_rule in ('yes','off_leash','leash') then 'yes'
           when te.dogs_rule = 'no' then 'no'
           else null
         end as collapsed
    from public.truth_external te
   where te.matched_origin_key is not null
),
per_beach as (
  select origin_key,
         max(raw_rule) filter (where source = 'bringfido')         as bringfido_rule,
         max(raw_rule) filter (where source = 'californiabeaches') as cb_rule,
         max(raw_rule) filter (where source = 'dogtrekker')        as dt_rule,
         max(collapsed) filter (where source = 'bringfido')         as bf_yn,
         max(collapsed) filter (where source = 'californiabeaches') as cb_yn,
         max(collapsed) filter (where source = 'dogtrekker')        as dt_yn
    from externals
   group by origin_key
),
ours as (
  select origin_key, name, dogs_verdict
    from public.all_coastal_features_lite()
   where layer = 'beach'
)
select o.origin_key,
       o.name,
       o.dogs_verdict   as our_verdict,
       p.bringfido_rule,
       p.cb_rule,
       p.dt_rule,
       -- Count of external sources that cover this beach
       (case when p.bf_yn is not null then 1 else 0 end
       + case when p.cb_yn is not null then 1 else 0 end
       + case when p.dt_yn is not null then 1 else 0 end) as n_external_sources,
       -- Count saying yes vs no
       (case when p.bf_yn = 'yes' then 1 else 0 end
       + case when p.cb_yn = 'yes' then 1 else 0 end
       + case when p.dt_yn = 'yes' then 1 else 0 end) as n_external_yes,
       (case when p.bf_yn = 'no' then 1 else 0 end
       + case when p.cb_yn = 'no' then 1 else 0 end
       + case when p.dt_yn = 'no' then 1 else 0 end) as n_external_no,
       -- Outcome label
       case
         when p.bf_yn is null and p.cb_yn is null and p.dt_yn is null
            then 'no_external_coverage'
         when (case when p.bf_yn='yes' then 1 else 0 end
              + case when p.cb_yn='yes' then 1 else 0 end
              + case when p.dt_yn='yes' then 1 else 0 end) >= 2
              and o.dogs_verdict = 'no'
            then 'LIKELY_OUR_ERROR_no'
         when (case when p.bf_yn='no' then 1 else 0 end
              + case when p.cb_yn='no' then 1 else 0 end
              + case when p.dt_yn='no' then 1 else 0 end) >= 2
              and o.dogs_verdict = 'yes'
            then 'LIKELY_OUR_ERROR_yes'
         when (case when p.bf_yn='yes' then 1 else 0 end
              + case when p.cb_yn='yes' then 1 else 0 end
              + case when p.dt_yn='yes' then 1 else 0 end)
            = (case when p.bf_yn is not null then 1 else 0 end
              + case when p.cb_yn is not null then 1 else 0 end
              + case when p.dt_yn is not null then 1 else 0 end)
            and o.dogs_verdict = 'yes'
            then 'AGREE_yes'
         when (case when p.bf_yn='no' then 1 else 0 end
              + case when p.cb_yn='no' then 1 else 0 end
              + case when p.dt_yn='no' then 1 else 0 end)
            = (case when p.bf_yn is not null then 1 else 0 end
              + case when p.cb_yn is not null then 1 else 0 end
              + case when p.dt_yn is not null then 1 else 0 end)
            and o.dogs_verdict = 'no'
            then 'AGREE_no'
         when (case when p.bf_yn='yes' then 1 else 0 end
              + case when p.cb_yn='yes' then 1 else 0 end
              + case when p.dt_yn='yes' then 1 else 0 end) > 0
            and (case when p.bf_yn='no' then 1 else 0 end
                + case when p.cb_yn='no' then 1 else 0 end
                + case when p.dt_yn='no' then 1 else 0 end) > 0
            then 'EXTERNALS_DISAGREE'
         when o.dogs_verdict is null
            then 'us_unknown'
         else 'mixed'
       end as outcome
  from ours o
  left join per_beach p on p.origin_key = o.origin_key
 where p.origin_key is not null;

grant select on public.truth_comparison_v to anon, authenticated;
