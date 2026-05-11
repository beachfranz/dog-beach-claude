-- 20260510_dismiss_reviewed_scoreability_flags.sql
--
-- scoreability_review_queue surfaces beaches where polygon_containment
-- says military_base or tribal_land AND is_scoreable=true. 12 beaches
-- on the queue today, all reviewed 2026-05-10 by Franz with verdict
-- "these all look fine, can't recall why I marked them." The flags
-- are noise — military polygons overlap public-access points
-- (Vandenberg outer access roads, Coronado boundary, etc.) but the
-- beaches themselves are correctly scoreable.
--
-- Two changes:
--   1. Stamp review_status='dismissed' on the 12 fids so they don't
--      keep surfacing on future loads.
--   2. Patch the view to exclude dismissed rows — invariant: a beach
--      can be dismissed once and stay dismissed unless re-flagged.

begin;

-- 1. Stamp the 12 fids currently in the queue as reviewed.
-- review_status check constraint allows: needs_review | verified | flagged
update public.beaches_gold
   set review_status = 'verified',
       review_notes  = coalesce(review_notes, '') ||
                       case when review_notes is null or review_notes = '' then '' else E'\n' end ||
                       '[2026-05-10] Verified: military/tribal polygon overlap is a false-positive flag; beach is public-access.'
 where fid in (
   select fid from public.scoreability_review_queue
 );

-- 2. Update the view to exclude dismissed rows.
create or replace view public.scoreability_review_queue as
 SELECT g.fid,
    g.name,
    g.county_name,
    g.is_scoreable AS current_is_scoreable,
    e.claimed_values ->> 'polygon_kind'::text AS flagged_kind,
    e.claimed_values ->> 'polygon_name'::text AS flagged_polygon_name,
    e.source AS flagged_source,
    e.confidence,
    e.updated_at AS flagged_at
   FROM beaches_gold g
     JOIN beach_enrichment_provenance e ON e.gold_fid = g.fid
  WHERE e.field_group = 'polygon_containment'::text
    AND e.is_canonical = true
    AND ((e.claimed_values ->> 'polygon_kind'::text) = ANY (ARRAY['military_base'::text, 'tribal_land'::text]))
    AND g.is_scoreable = true
    AND (g.review_status is null OR g.review_status <> 'verified')
  ORDER BY g.county_name, g.name;

commit;
