-- The consumer-facing 805: every beach_locations row joined to its
-- current dog verdict + confidence + sources. Most app code should
-- query this rather than reaching into beach_verdicts directly.
--
-- A LEFT JOIN — beaches without a computed verdict still appear with
-- nulls. ~5-10% of rows are expected null today (true orphans + cases
-- where the cascade returned no signal).

{{ config(materialized='view') }}

select
    bl.origin_key,
    bl.name,
    bl.operator_id,
    bl.geom,
    bl.feature_type,
    bl.address_clean,
    bl.address_city,
    bl.address_state,
    bl.address_postal,

    bv.dogs_verdict,
    bv.dogs_verdict_confidence,
    bv.dogs_verdict_meta -> 'sources'      as verdict_sources,
    bv.dogs_verdict_meta -> 'cpad_unit_id' as verdict_cpad_unit_id,
    bv.dogs_verdict_meta -> 'review'       as verdict_needs_review,
    bv.computed_at                         as verdict_computed_at
  from {{ ref('stg_beach_locations') }} bl
  left join {{ ref('stg_beach_verdicts') }} bv
    on bv.origin_key = bl.origin_key
