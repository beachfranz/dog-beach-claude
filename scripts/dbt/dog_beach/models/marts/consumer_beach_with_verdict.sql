-- The bridge: consumer-app `beaches` table joined to the catalog
-- pipeline's beach_verdicts. One row per consumer beach.
--
-- For each row, we compute the nearest beach_locations origin_key
-- via spatial proximity (≤500m), then look up the cascade verdict.
--
-- Three columns of interest for diff:
--   consumer_dogs_allowed   = beaches.dogs_allowed (curated by hand or earlier flow)
--   catalog_verdict         = beach_verdicts.dogs_verdict (cascade-computed)
--   parity                  = match | disagree | catalog_missing | both_null
--
-- Use cases:
--   1. Surface parity issues for review (Huntington State Beach: consumer
--      'mixed' vs catalog 'no' — needs reconciliation).
--   2. Identify catalog gaps (e.g., 5 Oregon beaches with no catalog row;
--      will close when PAD-US ingest lights up non-CA states).
--   3. Confirm cascade isn't drifting from manually-curated truth on the 5
--      flagship CA beaches.

{{ config(materialized='view') }}

with consumer as (
    select b.location_id,
           b.display_name,
           b.latitude,
           b.longitude,
           b.is_active,
           b.dogs_allowed,
           b.leash_policy,
           b.off_leash_flag,
           b.dogs_prohibited_start,
           b.dogs_prohibited_end,
           b.access_rule,
           ST_SetSRID(ST_MakePoint(b.longitude, b.latitude), 4326) as geom
      from {{ ref('stg_beaches') }} b
),

matched as (
    select c.*,
           (select bl.origin_key
              from {{ ref('stg_beach_locations') }} bl
             where st_dwithin(bl.geom::geography, c.geom::geography, 500)
             order by st_distance(bl.geom::geography, c.geom::geography) asc
             limit 1) as nearest_origin_key,
           (select st_distance(bl.geom::geography, c.geom::geography)::int
              from {{ ref('stg_beach_locations') }} bl
             where st_dwithin(bl.geom::geography, c.geom::geography, 500)
             order by st_distance(bl.geom::geography, c.geom::geography) asc
             limit 1) as match_distance_m
      from consumer c
)

select
    m.location_id,
    m.display_name,
    m.is_active,
    m.latitude,
    m.longitude,

    -- Consumer-side fields (curated)
    m.dogs_allowed       as consumer_dogs_allowed,
    m.leash_policy       as consumer_leash_policy,
    m.off_leash_flag     as consumer_off_leash_flag,
    m.dogs_prohibited_start as consumer_dogs_prohibited_start,
    m.dogs_prohibited_end   as consumer_dogs_prohibited_end,
    m.access_rule        as consumer_access_rule,

    -- Catalog-side fields (cascade-computed via beach_verdicts)
    m.nearest_origin_key,
    m.match_distance_m,
    bv.dogs_verdict      as catalog_verdict,
    bv.dogs_verdict_confidence as catalog_confidence,
    bv.dogs_verdict_meta -> 'sources' as catalog_sources,
    bv.computed_at       as catalog_computed_at,

    -- Parity classification — treats 'mixed'/'restricted' on consumer
    -- side as a soft-yes (dogs allowed with conditions).
    case
      when m.nearest_origin_key is null
        then 'catalog_missing'
      when m.dogs_allowed is null and bv.dogs_verdict is null
        then 'both_null'
      when (m.dogs_allowed in ('yes','restricted','mixed')) and bv.dogs_verdict = 'yes'
        then 'agree_yes'
      when m.dogs_allowed = 'no' and bv.dogs_verdict = 'no'
        then 'agree_no'
      when m.dogs_allowed is null
        then 'consumer_missing'
      when bv.dogs_verdict is null
        then 'catalog_null'
      else 'disagree'
    end as parity
  from matched m
  left join {{ ref('stg_beach_verdicts') }} bv
    on bv.origin_key = m.nearest_origin_key
