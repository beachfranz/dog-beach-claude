-- Compares our dogs_verdict against external truth-set sources
-- (BringFido / CaliforniaBeaches / DogTrekker / websearch).
--
-- Mirrors public.truth_comparison_v but expressed declaratively in dbt.
-- Differences from the public view:
--   - References stg_truth_external + stg_beach_verdicts directly
--     (no dependency on all_coastal_features_lite RPC).
--   - Names come from truth_external.name (the external source's
--     own label for the beach), not all_coastal_features_lite()'s
--     joined name. Slight differences expected on rows where the
--     external label diverges from our canonical name.
--
-- Outcome categories:
--   AGREE_yes               externals all yes, we said yes
--   AGREE_no                externals all no, we said no
--   LIKELY_OUR_ERROR_no     ≥2 externals yes, we said no
--   LIKELY_OUR_ERROR_yes    ≥2 externals no, we said yes
--   EXTERNALS_DISAGREE      externals disagree among themselves
--   us_unknown              externals have data, we have null
--   mixed                   one external + ours, but neither pattern above
--   no_external_coverage    truth_external has zero rows for this beach

{{ config(materialized='view') }}

with externals as (
    select te.matched_origin_key as origin_key,
           te.source              as ext_source,
           te.dogs_rule           as raw_rule,
           te.name                as ext_name,
           case
             when te.dogs_rule in ('yes','off_leash','leash') then 'yes'
             when te.dogs_rule = 'no' then 'no'
             else null
           end as collapsed
      from {{ ref('stg_truth_external') }} te
     where te.matched_origin_key is not null
),

per_beach as (
    select origin_key,
           max(raw_rule)  filter (where ext_source = 'bringfido')         as bringfido_rule,
           max(raw_rule)  filter (where ext_source = 'californiabeaches') as cb_rule,
           max(raw_rule)  filter (where ext_source = 'dogtrekker')        as dt_rule,
           max(raw_rule)  filter (where ext_source = 'websearch')         as ws_rule,
           max(collapsed) filter (where ext_source = 'bringfido')         as bf_yn,
           max(collapsed) filter (where ext_source = 'californiabeaches') as cb_yn,
           max(collapsed) filter (where ext_source = 'dogtrekker')        as dt_yn,
           max(collapsed) filter (where ext_source = 'websearch')         as ws_yn,
           max(ext_name)                                                   as name
      from externals
     group by origin_key
)

select
    p.origin_key,
    p.name,
    bv.dogs_verdict as our_verdict,
    p.bringfido_rule,
    p.cb_rule,
    p.dt_rule,
    p.ws_rule,
    (case when p.bf_yn is not null then 1 else 0 end +
     case when p.cb_yn is not null then 1 else 0 end +
     case when p.dt_yn is not null then 1 else 0 end +
     case when p.ws_yn is not null then 1 else 0 end) as n_external_sources,
    (case when p.bf_yn = 'yes' then 1 else 0 end +
     case when p.cb_yn = 'yes' then 1 else 0 end +
     case when p.dt_yn = 'yes' then 1 else 0 end +
     case when p.ws_yn = 'yes' then 1 else 0 end) as n_external_yes,
    (case when p.bf_yn = 'no'  then 1 else 0 end +
     case when p.cb_yn = 'no'  then 1 else 0 end +
     case when p.dt_yn = 'no'  then 1 else 0 end +
     case when p.ws_yn = 'no'  then 1 else 0 end) as n_external_no,
    case
      when p.bf_yn is null and p.cb_yn is null and p.dt_yn is null and p.ws_yn is null
        then 'no_external_coverage'
      when (case when p.bf_yn = 'yes' then 1 else 0 end +
            case when p.cb_yn = 'yes' then 1 else 0 end +
            case when p.dt_yn = 'yes' then 1 else 0 end +
            case when p.ws_yn = 'yes' then 1 else 0 end) >= 2
       and bv.dogs_verdict = 'no'
        then 'LIKELY_OUR_ERROR_no'
      when (case when p.bf_yn = 'no' then 1 else 0 end +
            case when p.cb_yn = 'no' then 1 else 0 end +
            case when p.dt_yn = 'no' then 1 else 0 end +
            case when p.ws_yn = 'no' then 1 else 0 end) >= 2
       and bv.dogs_verdict = 'yes'
        then 'LIKELY_OUR_ERROR_yes'
      when (case when p.bf_yn = 'yes' then 1 else 0 end +
            case when p.cb_yn = 'yes' then 1 else 0 end +
            case when p.dt_yn = 'yes' then 1 else 0 end +
            case when p.ws_yn = 'yes' then 1 else 0 end)
         = (case when p.bf_yn is not null then 1 else 0 end +
            case when p.cb_yn is not null then 1 else 0 end +
            case when p.dt_yn is not null then 1 else 0 end +
            case when p.ws_yn is not null then 1 else 0 end)
       and bv.dogs_verdict = 'yes'
        then 'AGREE_yes'
      when (case when p.bf_yn = 'no' then 1 else 0 end +
            case when p.cb_yn = 'no' then 1 else 0 end +
            case when p.dt_yn = 'no' then 1 else 0 end +
            case when p.ws_yn = 'no' then 1 else 0 end)
         = (case when p.bf_yn is not null then 1 else 0 end +
            case when p.cb_yn is not null then 1 else 0 end +
            case when p.dt_yn is not null then 1 else 0 end +
            case when p.ws_yn is not null then 1 else 0 end)
       and bv.dogs_verdict = 'no'
        then 'AGREE_no'
      when (case when p.bf_yn = 'yes' then 1 else 0 end +
            case when p.cb_yn = 'yes' then 1 else 0 end +
            case when p.dt_yn = 'yes' then 1 else 0 end +
            case when p.ws_yn = 'yes' then 1 else 0 end) > 0
       and (case when p.bf_yn = 'no' then 1 else 0 end +
            case when p.cb_yn = 'no' then 1 else 0 end +
            case when p.dt_yn = 'no' then 1 else 0 end +
            case when p.ws_yn = 'no' then 1 else 0 end) > 0
        then 'EXTERNALS_DISAGREE'
      when bv.dogs_verdict is null
        then 'us_unknown'
      else 'mixed'
    end as outcome
  from per_beach p
  left join {{ ref('stg_beach_verdicts') }} bv
    on bv.origin_key = p.origin_key
