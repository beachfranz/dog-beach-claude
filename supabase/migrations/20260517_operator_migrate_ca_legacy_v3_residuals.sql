-- v3 residual fix for 5 specific operators that v2 missed due to
-- type-mapping bugs:
--
-- (a) 4 county PARKS DEPARTMENTS + 1 flood-control DISTRICT — in the
--     legacy `operators` table with level='county' (which v2 mapped
--     to operator_type 'county'), but in the new `agency` table with
--     type='county_department'. v2's type filter wouldn't match.
--     Fix: insert these with operator_type='county_department'.
--
-- (b) United States Coast Guard — in agency table with
--     type='federal_military' (Coast Guard is technically a military
--     service under DHS), but v2's USCG fallback looked in
--     federal_department. Fix: insert with operator_type='federal_military'.
--
-- After this, the only remaining unmigrated CA beach-touching legacy
-- operators are the 10 private/nonprofit entities (HOAs, conservancies,
-- land trusts) — those need delegated_authority_via MOU/easement data
-- which isn't captured yet. Intentionally deferred.

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT lo.canonical_name, lo.short_name, mapped.t::operator_type, mapped.agency_id,
       lo.website, lo.dog_policy_url,
       jsonb_build_object(
         'legacy_operator_id', lo.id, 'slug', lo.slug, 'aliases', lo.aliases,
         'level', lo.level, 'subtype', lo.subtype,
         'cpad_agncy_name', lo.cpad_agncy_name, 'origin_source', lo.origin_source,
         'pad_us_mng_name', lo.pad_us_mng_name
       )
FROM public.operators lo
CROSS JOIN LATERAL (
  SELECT
    CASE
      WHEN lo.canonical_name = 'United States Coast Guard' THEN 'federal_military'
      WHEN lo.level='county' AND (
        lo.canonical_name ILIKE '%Department%'
        OR lo.canonical_name ILIKE '%District%'
        OR lo.canonical_name ILIKE '%Parks%'
      ) THEN 'county_department'
      ELSE NULL
    END AS t,
    CASE
      WHEN lo.canonical_name = 'United States Coast Guard' THEN
        (SELECT id FROM public.agency WHERE name='United States Coast Guard' AND type='federal_military' LIMIT 1)
      WHEN lo.level='county' AND (
        lo.canonical_name ILIKE '%Department%'
        OR lo.canonical_name ILIKE '%District%'
        OR lo.canonical_name ILIKE '%Parks%'
      ) THEN
        (SELECT id FROM public.agency WHERE name=lo.canonical_name AND type='county_department' LIMIT 1)
      ELSE NULL
    END AS agency_id
) mapped
WHERE lo.state_code='CA' AND lo.is_active=true
  AND mapped.t IS NOT NULL
  AND mapped.agency_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM public.beaches_gold g WHERE g.state='CA' AND g.is_active=true AND st_intersects(lo.geom, g.geom))
  AND NOT EXISTS (SELECT 1 FROM public.operator o WHERE o.name=lo.canonical_name AND o.type::text=mapped.t);
