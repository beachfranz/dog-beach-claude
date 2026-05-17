-- Phase H1 — temporal schema for beach_policy_source.
--
-- Per docs/consensus_phase_h_temporal_spec.md. Sibling table that captures
-- structured seasonal + daily-window carve-outs from policy_source prose.
-- Today: ~0 of 744 CA beaches have structured temporal data despite
-- extensive seasonal/day-part rules in their source text (Del Mar §4.08.020
-- seasonal, Rosie's hours modifier, plover Mar-Sep, etc.). Consumer
-- surface (beach.html + mobile-beach.html) already renders the shape;
-- this lets producers feed it.
--
-- Sibling-table approach (not bps columns) keeps the M:M base table clean
-- and lets a single bps row carry 0-N temporal exception windows.
--
-- bps reference is composite (beach_fid, policy_source_id, section) per
-- bps's PK; ON DELETE CASCADE so window rows are dropped when the bps row
-- is removed (e.g., when a ps is superseded).
--
-- Anchors (Labor Day / Memorial Day / dawn / dusk) are stored as text
-- identifiers + resolved at query time by resolve_anchor_date() (H3+).
-- This keeps the data year-agnostic — a row that says
-- "Labor Day +1 through June 15" doesn't need annual maintenance.

CREATE TABLE IF NOT EXISTS public.beach_policy_source_temporal (
  id bigserial PRIMARY KEY,

  -- bps reference (composite per bps PK)
  beach_fid        bigint NOT NULL,
  policy_source_id bigint NOT NULL,
  section          text   NOT NULL,

  -- The rule that applies DURING this window. Overrides bps.rule when
  -- the window is active; bps.rule is the base/outside-window rule.
  exception_rule text NOT NULL CHECK (exception_rule IN
    ('not_allowed','on_leash','off_leash','off_leash_voice_control')),

  -- Window classification.
  --   seasonal           — date range only (e.g., Jun 15 – Labor Day)
  --   daily              — hour range only, applies year-round (e.g., dawn-8am)
  --   seasonal_and_daily — both (e.g., dawn-8am DURING Jun 15-Labor Day)
  window_kind text NOT NULL CHECK (window_kind IN
    ('seasonal','daily','seasonal_and_daily')),

  -- Seasonal range (MM-DD format; year-agnostic for recurring annual).
  -- effective_from_md > effective_to_md means wrap-around year-boundary
  -- (e.g., '11-15' to '03-15' = winter closure spanning Dec/Jan/Feb).
  effective_from_md text NULL CHECK (effective_from_md ~ '^\d{2}-\d{2}$'),
  effective_to_md   text NULL CHECK (effective_to_md   ~ '^\d{2}-\d{2}$'),

  -- Named anchors for variable dates (resolved by H3 anchor-resolver).
  -- 'labor_day_after' = day after Labor Day (Sep, first Monday +1).
  -- 'plover_open' / 'plover_close' = snowy plover closure bookends.
  -- 'dawn' / 'dusk' = beach-coord-dependent; resolved per-beach.
  anchor_start text NULL CHECK (anchor_start IN
    ('labor_day','labor_day_after','memorial_day','memorial_day_after',
     'mlk_day','presidents_day','plover_open','plover_close',
     'dst_start','dst_end','dawn','dusk')),
  anchor_end   text NULL CHECK (anchor_end IN
    ('labor_day','labor_day_after','memorial_day','memorial_day_after',
     'mlk_day','presidents_day','plover_open','plover_close',
     'dst_start','dst_end','dawn','dusk')),

  -- Daily window (24h). daily_end < daily_start means wrap-around midnight
  -- (e.g., '22:00' to '06:00' = 10 PM through 6 AM).
  daily_start time NULL,
  daily_end   time NULL,

  -- Human-friendly label for the season (renders as zone_rules.seasons[].name).
  season_label text NULL,

  -- Extractor provenance / human notes.
  extracted_via text NULL,           -- e.g., 'sonnet-4-7-2026-05-17'
  extractor_confidence numeric NULL, -- 0..1; flag <0.7 for human review
  notes text NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  -- Idempotency: same bps shouldn't have duplicate windows.
  CONSTRAINT temporal_window_kind_consistency CHECK (
    (window_kind = 'seasonal' AND daily_start IS NULL AND daily_end IS NULL
        AND (effective_from_md IS NOT NULL OR anchor_start IS NOT NULL))
    OR
    (window_kind = 'daily' AND effective_from_md IS NULL AND effective_to_md IS NULL
        AND anchor_start IS NULL AND anchor_end IS NULL
        AND (daily_start IS NOT NULL OR daily_end IS NOT NULL))
    OR
    (window_kind = 'seasonal_and_daily'
        AND (effective_from_md IS NOT NULL OR anchor_start IS NOT NULL)
        AND (daily_start IS NOT NULL OR daily_end IS NOT NULL))
  ),

  FOREIGN KEY (beach_fid, policy_source_id, section)
    REFERENCES public.beach_policy_source (beach_fid, policy_source_id, section)
    ON DELETE CASCADE
);

-- Unique window per bps (content-hash to handle NULLable temporal fields).
CREATE UNIQUE INDEX IF NOT EXISTS ux_temporal_unique_window
  ON public.beach_policy_source_temporal
  (beach_fid, policy_source_id, section, window_kind,
   md5(
     coalesce(effective_from_md, '') || '|' ||
     coalesce(effective_to_md,   '') || '|' ||
     coalesce(anchor_start,      '') || '|' ||
     coalesce(anchor_end,        '') || '|' ||
     coalesce(daily_start::text, '') || '|' ||
     coalesce(daily_end::text,   '')
   ));

CREATE INDEX IF NOT EXISTS ix_temporal_bps_lookup
  ON public.beach_policy_source_temporal (beach_fid, policy_source_id, section);

CREATE INDEX IF NOT EXISTS ix_temporal_beach
  ON public.beach_policy_source_temporal (beach_fid);

-- Touch updated_at on any update.
CREATE OR REPLACE FUNCTION public._tg_temporal_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END$$;

CREATE TRIGGER bep_touch_updated_at
  BEFORE UPDATE ON public.beach_policy_source_temporal
  FOR EACH ROW EXECUTE FUNCTION public._tg_temporal_touch_updated_at();

-- When temporal rows change, re-promote the affected beach so the new
-- structured window flows through to zone_rules + flat columns.
-- Mirrors the existing tg_stmt_*_beach_policy_source triggers (same
-- helper, same dedup behavior).

CREATE OR REPLACE FUNCTION public.tg_stmt_ins_temporal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(SELECT DISTINCT beach_fid FROM new_rows WHERE beach_fid IS NOT NULL)
  );
  RETURN NULL;
END$$;

CREATE OR REPLACE FUNCTION public.tg_stmt_upd_temporal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(
      SELECT DISTINCT beach_fid FROM (
        SELECT beach_fid FROM new_rows
        UNION
        SELECT beach_fid FROM old_rows
      ) t WHERE beach_fid IS NOT NULL
    )
  );
  RETURN NULL;
END$$;

CREATE OR REPLACE FUNCTION public.tg_stmt_del_temporal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(SELECT DISTINCT beach_fid FROM old_rows WHERE beach_fid IS NOT NULL)
  );
  RETURN NULL;
END$$;

CREATE TRIGGER tg_stmt_ins_temporal
  AFTER INSERT ON public.beach_policy_source_temporal
  REFERENCING NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION public.tg_stmt_ins_temporal();

CREATE TRIGGER tg_stmt_upd_temporal
  AFTER UPDATE ON public.beach_policy_source_temporal
  REFERENCING NEW TABLE AS new_rows OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION public.tg_stmt_upd_temporal();

CREATE TRIGGER tg_stmt_del_temporal
  AFTER DELETE ON public.beach_policy_source_temporal
  REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE FUNCTION public.tg_stmt_del_temporal();

-- ─── Anchor resolver (date math; year-aware) ──────────────────────
-- Resolves named anchors to a concrete date in a given year. Used by H3
-- injector to materialize seasons[].dates strings + by H4 to compute
-- structured prohibition windows.
--
-- Tier 1 anchors: labor_day, labor_day_after, memorial_day,
-- memorial_day_after, mlk_day, presidents_day, plover_open, plover_close.
-- dawn / dusk deferred to a Python helper (beach-coord-dependent).

CREATE OR REPLACE FUNCTION public.resolve_anchor_date(
  p_anchor text,
  p_year   int
) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_first_of_month date;
  v_dow int;  -- ISO day-of-week (1=Mon..7=Sun)
  v_first_monday date;
BEGIN
  IF p_anchor IS NULL OR p_year IS NULL THEN
    RETURN NULL;
  END IF;

  CASE p_anchor
    WHEN 'labor_day' THEN
      -- First Monday of September.
      v_first_of_month := make_date(p_year, 9, 1);
      v_dow := extract(isodow from v_first_of_month)::int;
      RETURN v_first_of_month + ((8 - v_dow) % 7);

    WHEN 'labor_day_after' THEN
      -- Day after Labor Day.
      RETURN public.resolve_anchor_date('labor_day', p_year) + 1;

    WHEN 'memorial_day' THEN
      -- Last Monday of May.
      v_first_of_month := make_date(p_year, 5, 31);  -- last day of May
      v_dow := extract(isodow from v_first_of_month)::int;
      RETURN v_first_of_month - ((v_dow - 1 + 7) % 7);

    WHEN 'memorial_day_after' THEN
      RETURN public.resolve_anchor_date('memorial_day', p_year) + 1;

    WHEN 'mlk_day' THEN
      -- Third Monday of January.
      v_first_of_month := make_date(p_year, 1, 1);
      v_dow := extract(isodow from v_first_of_month)::int;
      v_first_monday := v_first_of_month + ((8 - v_dow) % 7);
      RETURN v_first_monday + 14;

    WHEN 'presidents_day' THEN
      -- Third Monday of February.
      v_first_of_month := make_date(p_year, 2, 1);
      v_dow := extract(isodow from v_first_of_month)::int;
      v_first_monday := v_first_of_month + ((8 - v_dow) % 7);
      RETURN v_first_monday + 14;

    WHEN 'plover_open' THEN
      -- Sep 16: day after snowy plover closure ends (closure runs Mar 15 - Sep 15).
      RETURN make_date(p_year, 9, 16);

    WHEN 'plover_close' THEN
      -- Mar 15: start of snowy plover closure.
      RETURN make_date(p_year, 3, 15);

    WHEN 'dst_start' THEN
      -- Second Sunday of March.
      v_first_of_month := make_date(p_year, 3, 1);
      v_dow := extract(isodow from v_first_of_month)::int;
      RETURN v_first_of_month + ((7 - v_dow) % 7) + 7;

    WHEN 'dst_end' THEN
      -- First Sunday of November.
      v_first_of_month := make_date(p_year, 11, 1);
      v_dow := extract(isodow from v_first_of_month)::int;
      RETURN v_first_of_month + ((7 - v_dow) % 7);

    ELSE
      -- dawn/dusk and unknown anchors require beach-coord context;
      -- caller must handle (Python helper / suncalc).
      RETURN NULL;
  END CASE;
END$$;

COMMENT ON TABLE public.beach_policy_source_temporal IS
  'Structured seasonal + daily-window carve-outs for beach_policy_source rows. '
  'Sibling table; one bps row can have 0-N temporal windows. Consumed by H3+ '
  'injector to populate zone_rules.seasons[]/time_windows[] and by H4 to '
  'populate beach_dog_policy.dogs_prohibited_start/end. Producer-side is the '
  'extract_temporal_from_policy_source.py LLM script (H2).';

COMMENT ON COLUMN public.beach_policy_source_temporal.exception_rule IS
  'The rule that applies DURING this window. Overrides bps.rule when the '
  'window is active; bps.rule is the base/outside-window rule.';

COMMENT ON COLUMN public.beach_policy_source_temporal.window_kind IS
  'Window classification: seasonal (date range only), daily (hour range only, '
  'year-round), seasonal_and_daily (both — hour range only applies during the '
  'season window).';

COMMENT ON FUNCTION public.resolve_anchor_date(text, int) IS
  'Tier-1 anchor resolver: Labor Day, Memorial Day, MLK, Presidents, '
  'plover open/close (Mar 15 / Sep 16), DST start/end. Returns NULL for '
  'dawn/dusk (beach-coord-dependent; resolve via Python helper). Immutable.';
