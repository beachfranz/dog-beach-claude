-- Mark known-private beaches inactive (admin_inactive = true).
--
-- Surfaced during truth-set false-negative triage when Chambers
-- Landing Beach (a private West Lake Tahoe beach club) showed up
-- as LIKELY_OUR_ERROR_no. Decision: private beaches don't belong
-- in the public corpus at all — showing them in the app and
-- recommending dog-friendliness sends users to a beach they can't
-- actually access. Tier 1 credibility issue.
--
-- This migration is the one-shot cleanup. The going-forward fix
-- (auto-flag at ingest time) lands in a separate migration that
-- updates load_ccc_batch / load_us_beach_points_batch and the
-- OSM ingest scripts.
--
-- Inactivation criteria, conservative (single signal per row):
--
-- 1. CCC where open_to_public IN ('No', 'Restricted')
--    Examples: Martin's Beach (the Vinod-Khosla case), Sanctuary
--    Beach Resort Accessway, Punta Gorda Lighthouse (BLM-restricted).
--
-- 2. OSM where tags->>'access' IN ('private','military','customers','no')
--    Examples: Camp Pendleton (military), Lake Arrowhead Village,
--    Surfside Beach (gated), Hamiltair Cove (HOA), Speedboat Beach.
--    'permissive' is NOT inactivated — public access by owner
--    permission is fine.
--
-- 3. UBP with explicit "Beach Club / HOA / Members" pattern in name
--    Specific list, not regex, to avoid false positives like
--    "Redondo Beach Breakwall" (address contained "Yacht Club Way"
--    but the breakwall itself is public).
--
-- 4. Chambers Landing Beach (UBP fid 13044227) — explicit case.
--    Private members-only club; not caught by any of the above
--    automated signals.
--
-- After this lands, recompute_all_dogs_verdicts_by_origin will
-- naturally drop these rows from beach_verdicts (the universe
-- filter excludes admin_inactive).

begin;

-- 1. CCC private/restricted access points
update public.ccc_access_points
   set admin_inactive = true
 where (admin_inactive is null or admin_inactive = false)
   and open_to_public in ('No', 'Restricted');

-- 2. OSM features tagged with non-public access
update public.osm_features
   set admin_inactive = true
 where (admin_inactive is null or admin_inactive = false)
   and feature_type in ('beach','dog_friendly_beach')
   and tags->>'access' in ('private','military','customers','no');

-- 3. UBP rows with explicit private-club / HOA / members in name
update public.us_beach_points
   set admin_inactive = true
 where (admin_inactive is null or admin_inactive = false)
   and fid in (
     10628306,  -- North Lake Beach Club (Irvine)
     15626772,  -- Executive Surfing Club (Coronado)
     530952,    -- Tahoe Pines HOA Pier and Beach
     13044227   -- Chambers Landing Beach (West Tahoe — private club)
   );

-- Drop the now-redundant Chambers Landing exception we added earlier
-- (the row is inactive; the exception will never fire). Keep for
-- audit trail; admin_inactive supersedes.

commit;
