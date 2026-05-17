-- Leo Carrillo correction:
-- 1) Update ps id 70 source_url to the per-park canonical page
--    (parks.ca.gov/?page_id=616) instead of the generic /Dogs catalog.
-- 2) Refresh full_text to capture both the geographic split AND the
--    explicit leash requirement (Franz pointer).
-- 3) Correct the bucket-E beach_policy_source row for fid 3671
--    (Leo Carillo State Beach): rule was set to not_allowed (wrong);
--    should be on_leash because dogs on leash ARE permitted on the
--    north section (north of Lifeguard Tower 3). Add status_note
--    flagging the south-of-Tower-3 / backcountry-trail exclusions.

UPDATE public.policy_source
   SET source_url = 'https://parks.ca.gov/?page_id=616',
       full_text = 'CA DPR — Leo Carrillo State Park dog policy: Dogs on a leash are allowed in the Park''s day-use areas, campground, and North Beach (north of Lifeguard Tower 3). Dogs are NOT allowed on backcountry trails or South Beach (south of Lifeguard Tower 3). Standard CA DPR 6-ft leash requirement applies. [Source: parks.ca.gov/?page_id=616 — canonical per-park page. Replaces the generic parks.ca.gov/Dogs catalog URL. Refreshed 2026-05-17.]'
 WHERE id = 70;

UPDATE public.beach_policy_source
   SET rule = 'on_leash',
       evidence_verbatim = 'CA DPR Leo Carrillo State Park: Dogs on leash allowed in day-use areas, campground, and North Beach (north of Lifeguard Tower 3). Standard CA DPR 6-ft leash.',
       evidence_url = 'https://parks.ca.gov/?page_id=616',
       status_note = 'GEOGRAPHIC SPLIT: Dogs prohibited on South Beach (south of Lifeguard Tower 3) and on backcountry trails. fid 3671 covers the full beach; per-beach refinement could split into North-Beach / South-Beach sub-records.'
 WHERE beach_fid = 3671 AND policy_source_id = 70 AND section = 'sand';
