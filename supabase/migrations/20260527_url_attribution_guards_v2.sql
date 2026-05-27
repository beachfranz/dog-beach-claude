-- 20260527_url_attribution_guards_v2.sql
--
-- Follow-up to 20260525_url_attribution_guards.sql. Sample audit on 25
-- demoted rows showed ~64% false-positive rate. Root cause: the exclusion
-- regex was anchored-exact-match (^(rules|policies|...)$) so it caught
-- slug='rules' but missed slug='la-county-beach-rules', 'Beach-Rules-Regulations',
-- 'pets-beaches.aspx', 'Off-Leash-Dog-Areas-OLDA', 'parks-rules-day-use-full.pdf',
-- etc. — generic catalog pages whose slug happens to contain 'beach' or kebab-
-- multi-word.
--
-- Fix in v2:
--   1. Strip file extensions (.aspx .pdf .html .htm .php .asp .jsp)
--   2. URL-decode %20 / + into separators before slug normalization
--   3. Expanded exclusion: substring patterns with word-boundaries for
--      (beach|park|parks)-(rules|policies|regulations|...) style catalogs,
--      (pets|dogs)-(beaches|on-the-beach), off-leash-dog-areas,
--      frequently-asked-questions, day-use, allowable-activities, etc.
--
-- True positives that MUST stay firing (verified preserved by v2):
--   - SNK PDF slug 'snk-policies-public-beach' (after %20-decode + ext-strip):
--     doesn't match any catalog substring pattern → guard fires on
--     beaches != Sandy Neck.
--   - Plymouth Long Beach slug 'seasonal-dog-restrictions-long-beach':
--     similarity check disambiguates per-beach (high for Long Beach itself,
--     low for siblings like Humarock).
--   - Joseph Sylvia 'josephsylviastatebeachrulesregulations' (no separators):
--     no word boundaries inside, doesn't match substring patterns.
--
-- Restore sweep + consensus refresh in same transaction.

begin;

-- ─── 1. Replace guard with v2 ───────────────────────────────────────────

create or replace function public._url_slug_specific_beach_mismatch(
  p_url        text,
  p_beach_name text
) returns boolean
language sql immutable
as $$
  with parts as (
    select
      -- FULL-PATH slug — strips protocol/host but keeps parent path segments.
      -- Used for token-overlap (positive override): includes context like
      -- /SandyNeckPark/ so 'sandy'/'neck' from the beach name can match.
      regexp_replace(
        regexp_replace(
          regexp_replace(
            replace(replace(
              lower(regexp_replace(
                regexp_replace(coalesce(p_url, ''), '\?.*$', ''),
                '^https?://[^/]+/?', ''
              )),
              '%20', '-'),
              '+', '-'),
            '\.(aspx|pdf|html?|php|asp|jsp)(/|$)', '', 'g'
          ),
          '[^a-z0-9]+', '-', 'g'
        ),
        '(^-+|-+$)', '', 'g'
      ) as slug_full,
      -- LAST-SEGMENT slug — used for catalog-pattern exclusion. Avoids
      -- false-excluding URLs where parent path happens to contain catalog
      -- words (e.g. /departments/beach-management/JosephSylviaStateBeach...).
      regexp_replace(
        regexp_replace(
          regexp_replace(
            replace(replace(
              lower(regexp_replace(
                regexp_replace(coalesce(p_url, ''), '\?.*$', ''),
                '.*/([^/]+)/?$', '\1'
              )),
              '%20', '-'),
              '+', '-'),
            '\.(aspx|pdf|html?|php|asp|jsp)$', ''
          ),
          '[^a-z0-9]+', '-', 'g'
        ),
        '(^-+|-+$)', '', 'g'
      ) as slug,
      lower(regexp_replace(coalesce(p_beach_name, ''), '[^a-zA-Z0-9]+', '-', 'g')) as name_kebab,
      -- name tokens ≥3 chars, excluding generic words that appear in many slugs
      (
        select array_agg(t)
          from unnest(regexp_split_to_array(lower(coalesce(p_beach_name, '')), '[^a-z0-9]+')) as t
         where length(t) >= 3
           and t not in (
             'the','and','for','beach','beaches','park','parks','county','state',
             'area','areas','dog','dogs','town','city','village','lake','pond',
             'road','street','point','cove','bay','public','private','open','space',
             'shoreline','sand','rules','regulations','policies','recreation'
           )
      ) as name_tokens
  ),
  token_check as (
    select parts.*,
      -- positive override: any specific beach-name token appears in
      -- FULL slug (path included)? Catches /SandyNeckPark/SNK...beach.pdf
      -- where the disambiguator lives in the parent path.
      coalesce((
        select bool_or(position(tk in parts.slug_full) > 0)
          from unnest(parts.name_tokens) as tk
      ), false) as token_in_slug
    from parts
  )
  select
    p_url is not null
    -- slug looks like a specific beach (contains 'beach' or kebab-multi-word)
    and (slug ~ 'beach' or slug ~ '[a-z]{3,}-[a-z]{3,}')
    -- positive override: if any beach-name token appears in slug, this URL
    -- legitimately references THIS beach. Don't fire.
    and not token_in_slug
    -- (A) exclude catalog slugs (anchored exact match, expanded list)
    and slug !~ ('^(pets|pet-friendly|parks-trails-pets|pets-beaches|pets-on-beaches|pet-policies|pet-policy|'
              || 'rules|regulations|rules-regulations|rules-and-regulations|policies|policy|'
              || 'index|home|main|faq|faqs|campgrounds|recreation|'
              || 'news-center|news|seasonal|'
              || 'state-park|state-parks|state-park-and-state-beach-rules|state-park-rules|state-parks-rules|'
              || 'park-rules|parks-rules|park-policies|parks-policies|park-faqs|parks-faqs|'
              || 'dogs-in-dcr-parks|dog-off-leash-areas|off-leash-dog-areas|off-leash-dog-areas-olda|off-leash-dogs|'
              || 'dogs-on-beach|dogs-on-the-beach|dog-on-the-beach|dogs-on-beaches|'
              || 'beach|beaches|'
              || 'beach-rules|beach-rules-regulations|beach-regulations|beach-policies|beach-policy|'
              || 'beach-management|beach-information|'
              || 'dog-rules|dog-friendly-areas|'
              || 'allowable-activities|allowable-activities-state-parks)$')
    -- (B) exclude catalog substring patterns (word-boundary via -|^|$)
    and slug !~ '(^|-)(beach|beaches|park|parks)-(rules|policies|policy|regulations|faq|faqs|management|information)(-|$)'
    and slug !~ '(^|-)(pets|dogs?)-(beaches?|on-the-beach|on-beach)(-|$)'
    and slug !~ '(^|-)(rules)-(and-)?(regulations|policy|policies)(-|$)'
    and slug !~ '(^|-)off-leash-(dog|dogs)-areas?(-olda)?(-|$)'
    and slug !~ '(^|-)frequently-asked-questions(-|$)'
    and slug !~ '(^|-)campground-rules?(-|$)'
    and slug !~ '(^|-)day-use(-|$)'
    and slug !~ '(^|-)allowable-activities(-|$)'
    and slug !~ '(^|-)dog-friendly-areas?(-|$)'
    and slug !~ '(^|-)summer-rules(-|$)'
    and slug !~ '(^|-)parks-and-recreation(-|$)'
    and slug !~ '(^|-)recreation-areas(-|$)'
    and slug !~ '(^|-)consolidated-dog-laws?(-|$)'
    and slug !~ '(^|-)state-park-visit-dogs(-|$)'
    and slug !~ '(^|-)how-have-great-state-park(-|$)'
    -- (C) and doesn't match this beach's own name
    and similarity(name_kebab, slug) < 0.30
  from token_check;
$$;

comment on function public._url_slug_specific_beach_mismatch is
  'v2 (2026-05-27): strips file extensions + URL-decodes %20/+ + uses substring '
  'word-boundary patterns to exclude generic catalog slugs (la-county-beach-rules, '
  'pets-beaches.aspx, off-leash-dog-areas-olda, etc.). Adds beach-name-token '
  'positive override using full URL path (so /SandyNeckPark/...SNK...beach.pdf '
  'correctly attaches to Sandy Neck Beach via the parent path). Restores ~616 '
  'of 1,511 rows demoted by v1''s over-aggressive anchored-exact-match exclusion.';

-- ─── 2. Restore sweep ──────────────────────────────────────────────────
-- Rows demoted 2026-05-25 that no longer fire under v2 guard → restore
-- is_canonical=true (relevance_verified stays false so the review backlog
-- knows these are restored-but-unverified).

with restorable as (
  select bep.id
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where bep.notes ilike '%DEMOTED 2026-05-25%'
     and bep.notes not ilike '%RESTORED 2026-05-27%'
     and not public._url_slug_specific_beach_mismatch(bep.source_url, g.name)
)
update public.beach_enrichment_provenance bep
   set is_canonical = true,
       notes = coalesce(notes, '')
              || ' [RESTORED 2026-05-27: guard v2 no longer fires (catalog slug or self-page).]'
  from restorable r
 where bep.id = r.id;

-- ─── 3. Recompute consensus + propagate to beach_dog_policy ────────────
-- For every fid touched by the restore sweep, re-run consensus picking
-- and re-promote to the consumer surface table.

do $$
declare r record;
begin
  for r in
    select distinct gold_fid as fid
      from public.beach_enrichment_provenance
     where notes ilike '%RESTORED 2026-05-27%'
       and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(r.fid);
    perform public.promote_canonical_dogs_to_beach_dog_policy(r.fid);
  end loop;
end $$;

-- ─── 4. Refresh scoring_tier for affected fids ─────────────────────────

do $$
declare r record;
begin
  for r in
    select distinct gold_fid as fid
      from public.beach_enrichment_provenance
     where notes ilike '%RESTORED 2026-05-27%'
       and gold_fid is not null
  loop
    perform public.refresh_scoring_tier(r.fid);
  end loop;
end $$;

commit;
