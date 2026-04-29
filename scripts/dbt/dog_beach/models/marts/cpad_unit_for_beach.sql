-- Per-beach canonical CPAD unit lookup, declarative.
--
-- Mirrors the public.cpad_unit_for_beach precompute but expressed as a
-- dbt-managed table built from staging models. Same lookup ranking as
-- compute_dogs_verdict_core uses inline:
--   1. demote environmental overlays (marine parks, ecological reserves, etc.)
--   2. prefer high match_beach_name score (full-name match first, stripped fallback)
--   3. prefer "Beach" in unit name
--   4. smallest containing polygon as final tiebreak
--
-- Source-of-truth note: when the cascade runs, compute_dogs_verdict_core
-- still does its own inline CPAD lookup with the same logic. This mart
-- is for visibility / audit, NOT consumed by the cascade. If we wanted
-- to make the cascade dbt-aware, we'd swap the inline lookup for a
-- read of this mart.

{{ config(materialized='table') }}

with universe as (
  select bl.origin_key,
         bl.name,
         bl.geom
    from {{ ref('stg_beach_locations') }} bl
)
select u.origin_key,
       u.name as beach_name,
       (select c.name
          from {{ ref('stg_counties') }} c
         where st_intersects(c.geom, u.geom)
         limit 1) as beach_county,
       st_y(u.geom)::float8 as lat,
       st_x(u.geom)::float8 as lng,
       cu.unit_id,
       cu.unit_area_m2,
       now() as computed_at
  from universe u
  left join lateral (
    select cu2.unit_id,
           st_area(cu2.geom::geography) as unit_area_m2
      from {{ ref('stg_cpad_units') }} cu2
     where st_contains(cu2.geom, u.geom)
     order by
       (cu2.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
       public.match_beach_name(coalesce(u.name, ''), coalesce(cu2.unit_name, '')) desc,
       (cu2.unit_name ~* '\mbeach\M')::int desc,
       st_area(cu2.geom) asc
     limit 1
  ) cu on true
