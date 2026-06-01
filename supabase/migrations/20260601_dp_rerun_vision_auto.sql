-- 20260601_dp_rerun_vision_auto.sql
--
-- Per Franz 2026-06-01: "to be clear curated:AI needs to be re-run
-- just not curated:human".
--
-- The 544 vision-auto picks on DPs are AI-generated and superseded
-- by the loader-bias method. UN-curate them so they fall back into
-- the candidate pool; the subsequent biased Tavily re-load will
-- replace the now-uncurated photos with fresh dog-content imagery;
-- the loader-bias curate step will then pick top-3 per park.
--
-- Also: relabel experiment:no-vision (DE pilot, 13 parks) to
-- loader-bias for sentinel consistency — same method, different name.
--
-- Curated:Human on DPs: zero rows. Nothing to preserve.

BEGIN;

UPDATE public.dog_park_photos
   SET curated_at    = NULL,
       curated_by    = NULL,
       match_quality = NULL
 WHERE curated_by = 'vision-auto';

UPDATE public.dog_park_photos
   SET curated_by = 'loader-bias'
 WHERE curated_by = 'experiment:no-vision';

COMMIT;
