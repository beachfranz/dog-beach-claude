-- ─────────────────────────────────────────────────────────────────────
-- beach_section_hour_status — per-beach per-section per-hour effective
-- rule materialization. Powers scoring + "right now" queries.
-- Franz 2026-05-21.
--
-- Why a materialization rather than computing on-the-fly:
--   * Scoring runs across thousands of beaches; computing the minute-
--     merge per query would be expensive.
--   * UI surfaces ("beaches off-leash right now") need fast lookups.
--   * Catalog-wide find.html filters need indexable conditions.
--
-- Refreshed by scripts/refresh_beach_section_hour_status.py:
--   * Per beach when zone_rules changes (trigger fires NOTIFY; script
--     listens or periodic --all catches stragglers).
--   * Daily for seasonal-date rolls + dawn/dusk shifts (cron, planned).
--
-- effective_rule values:
--   off_leash | on_leash | off_leash_voice_control | on_leash_or_voice |
--   not_allowed | closed (outside operating hours) | unknown
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.beach_section_hour_status (
  beach_fid       bigint   NOT NULL,
  section         text     NOT NULL,   -- 'sand' | 'water' | 'trails' | ...
  hour_of_day     smallint NOT NULL,   -- 0..23 (beach-local hour)
  effective_rule  text     NOT NULL,
  valid_date      date     NOT NULL,   -- the date these rows represent
  computed_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (beach_fid, section, hour_of_day)
);

CREATE INDEX IF NOT EXISTS beach_section_hour_status_fid_idx
  ON public.beach_section_hour_status (beach_fid);

CREATE INDEX IF NOT EXISTS beach_section_hour_status_rule_idx
  ON public.beach_section_hour_status (effective_rule, hour_of_day);

COMMENT ON TABLE public.beach_section_hour_status IS
  'Per (beach, section, hour-of-day) effective dog-policy rule for today. '
  'Refreshed by scripts/refresh_beach_section_hour_status.py. Consumed '
  'by scoring + UI "right now" queries. Daily cron + on-change trigger '
  'keep it current. (Franz 2026-05-21)';

COMMENT ON COLUMN public.beach_section_hour_status.hour_of_day IS
  'Beach-local hour 0-23. Operating-hours-clipped (rule = ''closed'' '
  'outside open/close window).';
COMMENT ON COLUMN public.beach_section_hour_status.effective_rule IS
  'Resolved rule via per-minute most-restrictive-wins applied to '
  'beach_dog_policy.zone_rules.regions[].sections[].rule + time_windows[]. '
  'Seasonal filter applied for valid_date. Operating-hours clip via '
  'beaches_gold.open_time/close_time (or sunrise-sunset fallback).';

-- Trigger: when zone_rules changes, NOTIFY so the population script
-- (or any listener) can refresh THIS beach. Refresh itself is OUT-
-- OF-PROCESS (Python) so this trigger only emits the signal.
CREATE OR REPLACE FUNCTION public._notify_zone_rules_changed()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.zone_rules IS DISTINCT FROM OLD.zone_rules THEN
    PERFORM pg_notify('zone_rules_changed', NEW.arena_group_id::text);
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tg_zone_rules_changed_notify ON public.beach_dog_policy;
CREATE TRIGGER tg_zone_rules_changed_notify
  AFTER UPDATE OF zone_rules ON public.beach_dog_policy
  FOR EACH ROW
  EXECUTE FUNCTION public._notify_zone_rules_changed();

COMMENT ON FUNCTION public._notify_zone_rules_changed() IS
  'Emits NOTIFY zone_rules_changed (payload = arena_group_id) when a '
  'beach''s zone_rules change. Consumed by Python population script.';
