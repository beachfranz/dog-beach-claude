-- 20260523_populate_from_operators_gold_keys_fix.sql
--
-- Fixes the consensus-key drift in populate_from_operators_gold's three
-- non-town blocks (gap #46e). The function emits BEP rows with claim
-- keys 'dogs_allowed' / 'leash_policy' but consensus_field_config for
-- field_group='dogs' looks for 'allowed' / 'leash_required'.
-- compute_beach_field_consensus filters claims via
-- `claimed_values ? claim_key`, so every operator_pad_us /
-- operator_county / operator_city BEP row has been silently DROPPED
-- from consensus voting since the function was last touched.
--
-- Audit confirmed 2,828 affected BEP rows across all states:
--   operator_county   1,733 rows  (0 use 'allowed', all use wrong keys)
--   operator_pad_us     693 rows  (same)
--   operator_city       402 rows  (same)
--
-- operator_town (4th block) was already fixed in gap #46d. This patch
-- aligns the other three to the same shape.
--
-- Three steps:
--   1. CREATE OR REPLACE populate_from_operators_gold with consensus-
--      aligned keys.
--   2. UPDATE existing wrong-key BEP rows in place (rename keys, keep
--      values). Idempotent — only touches rows with the wrong keys.
--   3. Refresh consensus + resolver + consumer-table promotion across
--      all beaches with affected BEP rows. Bounded scope, fast.

BEGIN;

-- ── 1. CREATE OR REPLACE populate_from_operators_gold ──────────────
CREATE OR REPLACE FUNCTION public.populate_from_operators_gold(p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(emitted_dogs integer, emitted_practical integer)
LANGUAGE plpgsql
AS $function$
declare
  v_dogs int := 0;
  v_pract int := 0;
begin
  -- ── PAD-US containment via resolver (unit_name → Loc_Mang → Loc_Own) ──
  with hits as (
    select distinct on (g.fid)
           g.fid as gold_fid, op.id as operator_id, op.canonical_name,
           odp.default_rule, odp.leash_required,
           odp.summary, odp.source_url
      from public.beaches_gold g
      join public.pad_us_units pu
        on st_contains(pu.geom, g.geom)
      cross join lateral (
        select coalesce(
          (select operator_id from public.resolve_agency(
              pu.unit_name, 'pad_us_unit_name', g.state, 0.65) limit 1),
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Mang', 'pad_us_loc_mang', g.state, 0.65) limit 1),
          (select operator_id from public.resolve_agency(
              pu.raw_attrs->>'Loc_Own',  'pad_us_loc_own',  g.state, 0.65) limit 1)
        ) as resolved_op_id
      ) r
      join public.operators op
        on op.id = r.resolved_op_id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active
       and (p_fid is null or g.fid = p_fid)
     order by g.fid, st_area(pu.geom) asc
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_pad_us', source_url,
           -- Consensus-aligned keys (gap #46e): 'allowed' + 'leash_required'
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case default_rule
                          when 'yes' then 'yes' when 'no' then 'no'
                          when 'mixed' then 'mixed' when 'seasonal' then 'yes'
                          when 'restricted' then 'yes'
                          else null end,
             'leash_required', case
                          when leash_required is true then 'on_leash'
                          when leash_required is false then 'off_leash'
                          else null end,
             'dogs_policy_notes', summary
           )),
           0.70, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning 1
  )
  select count(*) into v_dogs from ins_dogs;

  -- ── County via county_fips ─────────────────────────────────────────
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.operators op
        on op.level = 'county' and op.county_geoid = g.county_fips and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs2 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_county', source_url,
           -- Consensus-aligned keys (gap #46e)
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case default_rule
                          when 'yes' then 'yes' when 'no' then 'no'
                          when 'mixed' then 'mixed' when 'seasonal' then 'yes'
                          when 'restricted' then 'yes'
                          else null end,
             'leash_required', case
                          when leash_required is true then 'on_leash'
                          when leash_required is false then 'off_leash'
                          else null end,
             'dogs_policy_notes', summary
           )),
           0.55, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning 1
  )
  select count(*) into v_dogs from ins_dogs2;

  -- ── City via jurisdictions PIP ─────────────────────────────────────
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.jurisdictions j on st_contains(j.geom, g.geom) and j.place_type like 'C%'
      join public.operators op
        on op.level = 'city' and op.jurisdiction_id = j.id and op.is_active
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs3 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_city', source_url,
           -- Consensus-aligned keys (gap #46e)
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case default_rule
                          when 'yes' then 'yes' when 'no' then 'no'
                          when 'mixed' then 'mixed' when 'seasonal' then 'yes'
                          when 'restricted' then 'yes'
                          else null end,
             'leash_required', case
                          when leash_required is true then 'on_leash'
                          when leash_required is false then 'off_leash'
                          else null end,
             'dogs_policy_notes', summary
           )),
           0.50, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning 1
  )
  select count(*) into v_dogs from ins_dogs3;

  -- ── Town via tiger_county_subdivisions PIP (gap #46b + #46d) ───────
  -- Already uses consensus-aligned keys.
  with hits as (
    select distinct g.fid as gold_fid, op.canonical_name,
           odp.default_rule, odp.leash_required, odp.summary, odp.source_url
      from public.beaches_gold g
      join public.tiger_county_subdivisions tcs
        on tcs.state = g.state
       and tcs.classfp = 'T1'
       and st_contains(tcs.geom, g.geom)
      join public.operators op
        on op.level = 'town'
       and op.state_code = g.state
       and op.is_active
       and op.slug = public.slugify_agency('Town of ' || tcs.name)
      join public.operator_dogs_policy odp
        on odp.operator_id = op.id and odp.policy_found = true
     where g.is_active and (p_fid is null or g.fid = p_fid)
  ),
  ins_dogs4 as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_town', source_url,
           jsonb_strip_nulls(jsonb_build_object(
             'allowed', case default_rule
                          when 'yes' then 'yes' when 'no' then 'no'
                          when 'mixed' then 'mixed' when 'seasonal' then 'yes'
                          when 'restricted' then 'yes'
                          else null end,
             'leash_required', case
                          when leash_required is true then 'on_leash'
                          when leash_required is false then 'off_leash'
                          else null end,
             'dogs_policy_notes', summary
           )),
           0.90, false, 'derived_url_crawl', 'beach_access', now()
      from hits where source_url is not null
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence    is distinct from excluded.confidence
    returning 1
  )
  select count(*) into v_dogs from ins_dogs4;

  return query select v_dogs, v_pract;
end $function$;

-- ── 2. UPDATE existing wrong-key BEP rows in place ──────────────────
-- Rename 'dogs_allowed' → 'allowed' and 'leash_policy' → 'leash_required'
-- inside claimed_values jsonb. Only touches rows that have the wrong
-- keys, so re-running is a no-op.
UPDATE public.beach_enrichment_provenance bep
   SET claimed_values = (
         -- Strip wrong keys
         (bep.claimed_values - 'dogs_allowed' - 'leash_policy')
         -- Add 'allowed' key if 'dogs_allowed' was present
         || CASE WHEN bep.claimed_values ? 'dogs_allowed'
                 THEN jsonb_build_object('allowed', bep.claimed_values->>'dogs_allowed')
                 ELSE '{}'::jsonb END
         -- Add 'leash_required' key if 'leash_policy' was present
         || CASE WHEN bep.claimed_values ? 'leash_policy'
                 THEN jsonb_build_object('leash_required', bep.claimed_values->>'leash_policy')
                 ELSE '{}'::jsonb END
       ),
       updated_at = now()
 WHERE bep.field_group = 'dogs'
   AND bep.source IN ('operator_pad_us', 'operator_county', 'operator_city')
   AND (bep.claimed_values ? 'dogs_allowed' OR bep.claimed_values ? 'leash_policy');

-- ── 3. Refresh consensus + resolver + consumer-table promotion ──────
-- Bounded to beaches that have at least one affected BEP row.
-- Idempotent — re-running is safe.
WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs'
     AND source IN ('operator_pad_us', 'operator_county', 'operator_city', 'operator_town')
     AND gold_fid IS NOT NULL
)
SELECT public._resolve_dogs_gold(a.gold_fid) FROM affected a;

WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs'
     AND source IN ('operator_pad_us', 'operator_county', 'operator_city', 'operator_town')
     AND gold_fid IS NOT NULL
)
SELECT (public.compute_beach_field_consensus(a.gold_fid)).rows_inserted FROM affected a;

WITH affected AS (
  SELECT DISTINCT gold_fid
    FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs'
     AND source IN ('operator_pad_us', 'operator_county', 'operator_city', 'operator_town')
     AND gold_fid IS NOT NULL
)
SELECT public.promote_canonical_to_consumer_tables(a.gold_fid) FROM affected a;

COMMIT;

-- Sanity (post-apply):
--   SELECT source, COUNT(*) FILTER (WHERE claimed_values ? 'allowed') AS correct_key
--     FROM public.beach_enrichment_provenance
--    WHERE field_group='dogs' AND source IN ('operator_pad_us','operator_county','operator_city')
--    GROUP BY source;
--   Expected: 100% of rows have 'allowed' key after this migration.
