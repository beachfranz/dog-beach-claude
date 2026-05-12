-- 20260511_hdb_dog_policy_manual_override.sql
--
-- TACTICAL fix for fid 6212 (Huntington Beach Dog Beach). The BEP
-- resolver collapsed the city of Huntington Beach's ordinance ("dogs
-- prohibited on most of HB; leashed only allowed north of 22nd Street
-- to Seapoint Ave") into zone_rules.sand = 'not_allowed', which is
-- catastrophically wrong: this stretch IS the iconic OC off-leash
-- dog beach, operated by HBDB Inc as a city carve-out. The city's
-- jurisdictional ordinance is technically correct for the rest of HB
-- but flatly inapplicable to the carve-out zone.
--
-- Same operator-vs-jurisdiction pattern that bit us with Bolsa Chica:
-- the spatial join promotes a containing-area policy that doesn't
-- reflect the actual managing operator. Durable fix = a dog-policy
-- override mechanism analogous to polygon_overrides on beaches_gold;
-- that's TODO.
--
-- For now: two-layer manual fix
--   1. Inject a high-confidence canonical BEP row for source='manual'
--      reflecting actual operator policy (HBDB Inc)
--   2. Demote all competing canonical BEP rows for dogs/fid 6212
--   3. Hand-patch beach_dog_policy directly (the resolver-built
--      zone_rules doesn't assemble carve-out regions cleanly)
--
-- Source: https://hbdogbeach.org/about (HBDB Inc, the nonprofit that
-- maintains the off-leash zone) + on-the-ground signage.

begin;

-- Step 1: demote any existing canonical dogs rows for fid 6212
update public.beach_enrichment_provenance
   set is_canonical = false
 where gold_fid = 6212
   and field_group = 'dogs'
   and is_canonical = true;

-- Step 2: insert (or refresh) the manual canonical row
insert into public.beach_enrichment_provenance
       (gold_fid, field_group, source, source_url, confidence,
        is_canonical, updated_at, claimed_values, notes)
values (
  6212, 'dogs', 'manual',
  'https://hbdogbeach.org/about',
  0.99,
  true,
  now(),
  jsonb_build_object(
    'allowed',         'yes',
    'dogs_allowed',    'yes',
    'leash_policy',    'off_leash',
    'off_leash_exists', true,
    'has_off_leash',    true,
    'has_on_leash',     true,
    'dogs_policy_notes',
      'HB Dog Beach (Goldenwest to Seapoint stretch) is operated as an off-leash carve-out '
      'by HBDB Inc. Off-leash on the sand and in the water; on-leash required in the '
      'parking lots and around restrooms.'
  ),
  'Manual override: city ordinance applied jurisdictionally flattens the operator carve-out. '
  'Recurate via durable dog_policy_override mechanism when built.'
);

-- Step 3: hand-patch beach_dog_policy directly. The resolver doesn't
-- produce the carve-out shape correctly from a single BEP row; we
-- write the zone_rules explicitly.
update public.beach_dog_policy
   set dogs_allowed        = 'yes',
       leash_policy        = 'off_leash',
       off_leash_flag      = true,
       has_off_leash       = true,
       has_on_leash        = true,
       access_rule         = null,
       dogs_prohibited_start = null,
       dogs_prohibited_end   = null,
       source              = 'manual',
       consensus_confidence = 0.99,
       disagreement_flag    = false,
       curated_at           = now(),
       zone_rules_updated_at = now(),
       notes                = 'Manual override 2026-05-11: operator carve-out (HBDB Inc) supersedes the jurisdictional city ordinance.',
       zone_rules = jsonb_build_object(
         'seasons', jsonb_build_array(
           jsonb_build_object(
             'name', 'All year',
             'regions', jsonb_build_array(
               jsonb_build_object(
                 'name', 'HB Dog Beach (Goldenwest to Seapoint)',
                 'sections', jsonb_build_object(
                   'sand',         jsonb_build_object('rule','off_leash'),
                   'water_swim',   jsonb_build_object('rule','off_leash'),
                   'parking_lot',  jsonb_build_object('rule','on_leash'),
                   'restrooms',    jsonb_build_object('rule','on_leash'),
                   'picnic_area',  jsonb_build_object('rule','on_leash')
                 )
               )
             )
           )
         )
       )
 where arena_group_id = 6212;

commit;
