-- 20260607i_get_beach_advisories_date_param.sql
--
-- Wire-through follow-up: cautions card on beach.html should reflect
-- the day-pill that find.html clicked through with, same as the badge
-- + chart + headline + scout blurb now do (per 20260607h).
--
-- Adds p_date to get_beach_advisories. Defaults to today at the
-- beach's own timezone (not server-side UTC current_date — same
-- DST/border-crossing trap that bit get_beach_info earlier).
--
-- The horizon_hours arg stays for backwards compat (kept as 2nd
-- positional + still ignored), p_date slots in as a 3rd named arg so
-- existing callers passing only {p_fid, p_horizon_hours} don't break.

CREATE OR REPLACE FUNCTION public.get_beach_advisories(
  p_fid           bigint,
  p_horizon_hours integer DEFAULT 24,   -- ignored (kept for sig compat)
  p_date          date    DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $function$
  WITH bounds AS (
    SELECT
      COALESCE(bg.timezone, 'America/Los_Angeles') AS tz,
      -- target_date defaults to TODAY at the beach's own tz when caller
      -- omits p_date (so server-side UTC midnight crossings don't
      -- silently flip the displayed day). When p_date is provided
      -- (find.html day-pill click), use it verbatim.
      (COALESCE(
        p_date,
        (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date
       )::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS day_start_utc,
      ((COALESCE(
        p_date,
        (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date
       ) + 1)::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS day_end_utc,
      -- "Still in effect" gate: for today's view, drop advisories whose
      -- valid_to has already passed. For future-day views, no such
      -- pruning — show the day's complete advisory set even if some
      -- windows have technically passed by the time the user clicks.
      (p_date IS NULL OR p_date = (now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date) AS is_today
    FROM public.beaches_gold bg
    WHERE bg.fid = p_fid
    LIMIT 1
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'source',             a.source,
    'severity',           a.severity,
    'event_type',         a.event_type,
    'dog_impact_class',   a.dog_impact_class,
    'dog_impact_text',    a.dog_impact_text,
    'label',              a.label,
    'value',              a.value,
    'icon',               a.icon,
    'valid_from',         GREATEST(a.valid_from, b.day_start_utc),
    'valid_to',           LEAST(a.valid_to, b.day_end_utc),
    'translation_source', a.translation_source,
    'raw_data',           a.raw_data
  ) ORDER BY public._advisory_severity_rank(a.severity) DESC, a.valid_from ASC),
                  '[]'::jsonb)
    FROM public.beach_advisory a
    CROSS JOIN bounds b
   WHERE a.beach_fid   = p_fid
     AND a.event_type  <> 'strong_drift'                -- defense-in-depth
     AND a.valid_to    >  b.day_start_utc               -- overlaps day
     AND a.valid_from  <  b.day_end_utc
     AND (NOT b.is_today OR a.valid_to > now())         -- on today, prune already-passed
$function$;

-- Need to drop the old (bigint, integer) overload OR keep both — PG can
-- disambiguate by arity (2-arg vs 3-arg), so leaving the old as a
-- shadow is fine. But to keep semantics clean, replace it.
DROP FUNCTION IF EXISTS public.get_beach_advisories(bigint, integer);

GRANT EXECUTE ON FUNCTION public.get_beach_advisories(bigint, integer, date)
  TO anon, authenticated;
