-- ============================================================
-- Enable Row Level Security on all tables
-- Public read-only access for beach/forecast data.
-- All sensitive tables fully locked to anon.
-- Edge functions use service role and bypass RLS entirely.
-- ============================================================

-- ── Enable RLS on every table ─────────────────────────────────
ALTER TABLE public.beaches                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scoring_config           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.beach_day_hourly_scores  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.beach_day_recommendations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.subscribers              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.subscriber_locations     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notification_log         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.refresh_errors           ENABLE ROW LEVEL SECURITY;

-- ── Public read-only policies ─────────────────────────────────
-- Anyone can read beach metadata and forecast data.
-- No writes, no deletes.

CREATE POLICY "public_read_beaches"
  ON public.beaches
  FOR SELECT
  TO anon
  USING (true);

CREATE POLICY "public_read_hourly_scores"
  ON public.beach_day_hourly_scores
  FOR SELECT
  TO anon
  USING (true);

CREATE POLICY "public_read_recommendations"
  ON public.beach_day_recommendations
  FOR SELECT
  TO anon
  USING (true);

-- ── No anon access to sensitive / operational tables ──────────
-- scoring_config  — configuration, no reason for public access
-- subscribers     — contains phone numbers (PII)
-- subscriber_locations — linked to subscribers
-- notification_log — operational log
-- refresh_errors   — operational log
--
-- No policies created = RLS blocks all anon access by default.
