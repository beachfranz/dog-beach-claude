-- Migrate CA beach-touching operators from legacy `public.operators`
-- (plural, 1,325 CA rows seeded from CPAD + TIGER + PAD-US) into the
-- new `public.operator` schema (singular, the FK target of
-- policy_source.issuing_operator_id and beach_operator.operator_id).
--
-- Scope: only operators where the legacy geometry spatially intersects
-- at least one active CA beach in beaches_gold. Expected: ~160 rows.
--
-- Type mapping (legacy `level/subtype` → new `operator_type` enum):
--   'city'             → 'city'
--   'county'           → 'county'
--   'state'            → 'state'
--   'special-district' → 'special_district'
--   'tribal'           → 'tribal'
--   'private'          → 'private_company'
--   'federal' + 'dod'  → 'federal_military'
--   'federal' (other)  → 'federal_department'
--   'joint'            → 'special_district' (no exact enum equiv)
--
-- agency_id matching: best-effort by stripping "City of "/"County of "
-- prefix from legacy canonical_name and ILIKE-matching against
-- public.agency on name + mapped type. NULL when no match (acceptable;
-- can be backfilled later).
--
-- metadata: preserves legacy slug, aliases, cpad_agncy_name, level,
-- subtype, origin_source so the upstream attribution isn't lost.
--
-- Unique constraint: ux_operator_name_type(name, type) — the WHERE
-- NOT EXISTS guard prevents duplicates. The 2 nonprofit operators
-- already seeded by 20260516_operator_nonprofit_seed.sql (PSHDB,
-- Crystal Cove Conservancy) won't be touched since they're not in
-- the legacy `operators` table.

INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT DISTINCT ON (lo.canonical_name, mapped_type.t)
       lo.canonical_name,
       lo.short_name,
       mapped_type.t::operator_type,
       (
         -- Best-effort agency lookup: strip "City of " / "County of " prefix and ILIKE-match
         SELECT a.id FROM public.agency a
          WHERE a.type::text = (
            case mapped_type.t
              when 'city' then 'city'
              when 'county' then 'county'
              when 'state' then 'state_department'
              when 'special_district' then 'special_district'
              when 'tribal' then 'tribal'
              when 'federal_department' then 'federal_department'
              when 'federal_military' then 'federal_military'
              when 'private_company' then null
              else null
            end
          )
            AND (
              lower(a.name) = lower(regexp_replace(lo.canonical_name, '^(City|County|Town) of ', '', 'i'))
              OR lower(a.name) = lower(lo.canonical_name)
            )
          LIMIT 1
       ),
       lo.website,
       lo.dog_policy_url,
       jsonb_build_object(
         'legacy_operator_id', lo.id,
         'slug', lo.slug,
         'aliases', lo.aliases,
         'level', lo.level,
         'subtype', lo.subtype,
         'cpad_agncy_name', lo.cpad_agncy_name,
         'origin_source', lo.origin_source,
         'pad_us_mng_name', lo.pad_us_mng_name
       )
FROM public.operators lo
CROSS JOIN LATERAL (
  SELECT case
    when lo.level = 'city' then 'city'
    when lo.level = 'county' then 'county'
    when lo.level = 'state' then 'state'
    when lo.level = 'special-district' then 'special_district'
    when lo.level = 'tribal' then 'tribal'
    when lo.level = 'private' then 'private_company'
    when lo.level = 'federal' and lo.subtype = 'dod' then 'federal_military'
    when lo.level = 'federal' then 'federal_department'
    when lo.level = 'joint' then 'special_district'
    else 'private_company'
  end as t
) mapped_type
WHERE lo.state_code = 'CA'
  AND lo.is_active = true
  AND EXISTS (
    SELECT 1 FROM public.beaches_gold g
     WHERE g.state='CA' AND g.is_active=true
       AND st_intersects(lo.geom, g.geom)
  )
  AND NOT EXISTS (
    SELECT 1 FROM public.operator o
     WHERE o.name = lo.canonical_name AND o.type::text = mapped_type.t
  )
ORDER BY lo.canonical_name, mapped_type.t, lo.id;
