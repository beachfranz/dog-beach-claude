-- 20260612g_carmel_zone_rules_override.sql
--
-- Follow-up to 20260612f. The previous migration set the binary leash
-- flags on fid 8287 (Carmel City Beach) to off-leash, but the
-- tg_after_change_dogs_refire_zone_rules trigger immediately re-fired
-- _promote_zone_rules_for_fid and rebuilt zone_rules from the existing
-- BEP evidence — which is wrong-river-beach data, so zone_rules ended
-- up showing on-leash sand again.
--
-- This migration writes a curator-authored zone_rules JSONB directly.
-- Because the trigger watches dogs_allowed / has_off_leash / has_on_leash
-- (not zone_rules), updating zone_rules in isolation does NOT re-fire it.
--
-- Note: the underlying BEP row for fid 8287 still contains the bad
-- river-beach quote ("Dogs must be on-leash at Carmel River Beach.")
-- attributed to a Carmel-by-the-Sea tourism page. Cleaning that up is
-- a separate workstream — any future re-derivation of consensus +
-- zone_rules from BEP will overwrite this override unless the BEP
-- evidence is also fixed.

BEGIN;

UPDATE public.beach_dog_policy
   SET zone_rules = jsonb_build_object(
     'global_notes', NULL,
     'regions', jsonb_build_array(
       jsonb_build_object(
         'name', NULL,
         'sections', jsonb_build_object(
           'sand', jsonb_build_object(
             'rule', 'off_leash',
             'evidence', jsonb_build_object(
               'citation', 'Carmel-by-the-Sea Municipal Code — Carmel City Beach',
               'source',   'manual_curator',
               'quote',    'Curator-asserted: Carmel City Beach permits off-leash dogs under direct voice control of owner (per Carmel-by-the-Sea Municipal Code, Title 4).',
               'curator_date', '2026-06-12'
             )
           ),
           'water', jsonb_build_object(
             'rule', 'off_leash',
             'evidence', jsonb_build_object(
               'citation', 'Carmel-by-the-Sea Municipal Code — Carmel City Beach',
               'source',   'manual_curator',
               'quote',    'Curator-asserted: Carmel City Beach permits off-leash dogs under direct voice control of owner (per Carmel-by-the-Sea Municipal Code, Title 4).',
               'curator_date', '2026-06-12'
             )
           )
         )
       )
     )
   ),
       zone_rules_updated_at = now()
 WHERE arena_group_id = 8287;

COMMIT;

NOTIFY pgrst, 'reload schema';
