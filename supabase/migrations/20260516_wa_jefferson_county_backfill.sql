-- Wave 4: Jefferson County (WA) dog-policy backfill.
--
-- Source: Jefferson County Code §6.07.060 (Animal at large) — Title
-- 6 / Chapter 6.07 ANIMAL CONTROL.
--
-- Live URL: https://www.codepublishing.com/WA/JeffersonCounty/html/JeffersonCounty06/JeffersonCounty0607.html
--
-- Code defines "at large" (§6.07.020(8), verbatim):
--   "At large" means physically off the premises of the owner,
--   handler, or keeper, and not secured by a leash eight feet or less
--   in length and in the physical control of the owner or owner's
--   designee, provided, "at large" does not include dogs exhibited in
--   dog shows, field trials, obedience trials, or the training of
--   dogs therefor; or the use of a dog under the supervision of a
--   person to hunt, to chase or tree predatory animals or game birds;
--   or the use of a dog to control or protect livestock or property
--   or in other agricultural activities; or a dog when otherwise
--   safely and securely confined or completely controlled within or
--   upon any vehicle; or under control of a competent person in a
--   designated off-leash area; or dogs on duty for a law enforcement
--   agency.
--
-- Operative rule (§6.07.060, verbatim):
--   (1) It is unlawful for the owner or keeper of any dog whether
--   licensed or not, or any livestock as defined in this chapter, to
--   allow such animal to be at large or to roam, stray or be away
--   from the premises of the owner or keeper, or to enter or be on
--   the private property of another without permission of the owner
--   or lawful custodian of such property, or to be at large on any
--   public property, including but not limited to any public park,
--   beach, pond, fountain or stream therein, playground or school
--   ground, building, roadway, street, alley, trail or sidewalk. This
--   section shall not apply to any real property that is designated
--   with formal signage and demarcated boundaries as an "off-leash"
--   area. Nothing in this section shall prohibit the county
--   commission from establishing for domestic animals different rules
--   and regulations applicable within one, some or all county-owned
--   parks and other county-owned real property and such separate
--   rules or regulations, if different than this chapter, shall apply
--   at those locations rather than the regulations listed in this
--   chapter.
--
-- Sand rule = on_leash. The §6.07.060 explicit list includes "beach"
-- — so the leash-or-confined rule applies countywide on every public
-- beach by name. Off-leash status requires formal county designation.
--
-- 35 Jefferson County beaches in beaches_gold (Olympic peninsula
-- + Quimper peninsula + Hood Canal west shore).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Jefferson County Code §6.07.060 (Animal at large) — Title 6 / Chapter 6.07 Animal Control',
       (SELECT id FROM public.agency WHERE name='Jefferson County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/JeffersonCounty/html/JeffersonCounty06/JeffersonCounty0607.html',
       'Jefferson County Code §6.07.060 Animal at large. (1) It is unlawful for the owner or keeper of any dog whether licensed or not, or any livestock as defined in this chapter, to allow such animal to be at large or to roam, stray or be away from the premises of the owner or keeper, or to enter or be on the private property of another without permission of the owner or lawful custodian of such property, or to be at large on any public property, including but not limited to any public park, beach, pond, fountain or stream therein, playground or school ground, building, roadway, street, alley, trail or sidewalk. This section shall not apply to any real property that is designated with formal signage and demarcated boundaries as an "off-leash" area. Nothing in this section shall prohibit the county commission from establishing for domestic animals different rules and regulations applicable within one, some or all county-owned parks and other county-owned real property and such separate rules or regulations, if different than this chapter, shall apply at those locations rather than the regulations listed in this chapter. §6.07.020(8) "At large" means physically off the premises of the owner, handler, or keeper, and not secured by a leash eight feet or less in length and in the physical control of the owner or owner''s designee. [Beach explicitly named in §6.07.060(1). Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Jefferson County Code §6.07.060%'
);

-- ─── 2. Link Jefferson County beaches ───────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Jefferson'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Jefferson County Code §6.07.060%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Jefferson County Code §6.07.060(1): unlawful for dogs to be at large on any public property including "beach" by name. "At large" per §6.07.020(8) = not secured by a leash 8 feet or less. Off-leash requires formal county designation.',
       'https://www.codepublishing.com/WA/JeffersonCounty/html/JeffersonCounty06/JeffersonCounty0607.html',
       'Sand rule = on_leash per §6.07.060 (8-foot max). Beaches explicitly named in the statute.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
