// scoring.ts
// Core scoring logic for Dog Beach Scout.
//
// Responsibilities:
//   1. Derive busyness category from raw busyness score
//   2. Run hard NO-GO checks → hour_status = 'no_go'
//   3. Run caution checks    → hour_status = 'caution'
//   4. Compute 0-100 composite hour_score for candidate hours
//   5. Collect passed/failed checks and reason codes 
//   6. Select best 2-5 contiguous window from candidate hours
//   7. Build explainability payload per hour
//
// All thresholds and weights come from a ScoringConfig row — never hardcoded.

// Types inlined — no external path imports in edge functions
type HourStatus       = "go" | "caution" | "no_go";
type BusynessCategory = "quiet" | "moderate" | "dog_party" | "too_crowded";

interface ScoringConfig {
  scoring_version: string;
  nogo_precip_chance: number;
  nogo_wind_speed: number;
  nogo_wmo_codes: number[];
  caution_precip_chance: number;
  caution_wind_speed: number;
  caution_tide_height: number;
  caution_uv_index: number;
  positive_low_tide: number;
  positive_very_low_tide: number;
  positive_low_precip: number;
  positive_calm_wind: number;
  positive_temp_min: number;
  positive_temp_max: number;
  positive_low_uv: number;
  busy_quiet_max: number;
  busy_moderate_max: number;
  busy_dog_party_max: number;
  weight_tide: number;
  weight_rain: number;
  weight_wind: number;
  weight_crowd: number;
  weight_temp: number;
  weight_uv: number;
  norm_tide_max: number;
  norm_wind_max: number;
  norm_temp_target: number;
  norm_temp_range: number;
  norm_uv_max: number;
  window_min_hours: number;
  window_max_hours: number;
  window_caution_penalty: number;
}
import { SEVERE_WMO_CODES } from "./openmeteo.ts";

// ─── Input / output types ─────────────────────────────────────────────────────

export interface RawHourData {
  forecastTs: string;    // UTC ISO timestamptz — the PK
  localDate: string;     // "YYYY-MM-DD"
  localHour: number;     // 0-23
  hourLabel: string;     // "6am", "2pm" etc.
  isDaylight: boolean;
  weatherCode: number | null;
  tempAir: number | null;
  windSpeed: number | null;
  precipChance: number | null;
  uvIndex: number | null;
  tideHeight: number | null;
  busynessScore: number | null;  // 0-100 from BestTime
  isBeachOpen: boolean;          // derived from open_time / close_time
}

export interface ScoredHour {
  // All RawHourData fields pass through
  forecastTs: string;
  localDate: string;
  localHour: number;
  hourLabel: string;
  isDaylight: boolean;
  weatherCode: number | null;
  tempAir: number | null;
  windSpeed: number | null;
  precipChance: number | null;
  uvIndex: number | null;
  tideHeight: number | null;
  busynessScore: number | null;
  busynessCategory: BusynessCategory | null;
  // Derived
  hourStatus: HourStatus;
  hourScore: number | null;
  isCandidateWindow: boolean;
  isInBestWindow: boolean;
  passedChecks: string[];
  failedChecks: string[];
  positiveReasonCodes: string[];
  riskReasonCodes: string[];
  explainability: Record<string, number>;
  hourText: string;
}

export interface BestWindow {
  hours: ScoredHour[];
  startTs: string;
  endTs: string;
  label: string;          // "12pm–4pm"
  windowScore: number;
  status: HourStatus;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Score all hours for a single beach-day.
 * Returns an array of ScoredHour in the same order as input.
 */
export function scoreHours(
  hours: RawHourData[],
  config: ScoringConfig,
): ScoredHour[] {
  return hours.map((h) => scoreOneHour(h, config));
}

/**
 * Given a full set of scored hours for a beach (all 7 days),
 * identify the best contiguous window per calendar date.
 * Returns a Map<localDate, BestWindow | null>.
 */
export function selectBestWindows(
  allScoredHours: ScoredHour[],
  config: ScoringConfig,
): Map<string, BestWindow | null> {
  // Group by local_date
  const byDate = new Map<string, ScoredHour[]>();
  for (const h of allScoredHours) {
    const arr = byDate.get(h.localDate) ?? [];
    arr.push(h);
    byDate.set(h.localDate, arr);
  }

  const result = new Map<string, BestWindow | null>();
  for (const [date, hours] of byDate) {
    // Sort by local hour to ensure contiguity checks are correct
    const sorted = [...hours].sort((a, b) => a.localHour - b.localHour);
    result.set(date, findBestWindow(sorted, config));
  }
  return result;
}

/**
 * Mark is_in_best_window on the scored hours in-place,
 * given the windows map from selectBestWindows().
 */
export function applyBestWindowFlags(
  scoredHours: ScoredHour[],
  windows: Map<string, BestWindow | null>,
): void {
  const windowTsSet = new Set<string>();
  for (const window of windows.values()) {
    if (window) {
      for (const h of window.hours) {
        windowTsSet.add(h.forecastTs);
      }
    }
  }
  for (const h of scoredHours) {
    h.isInBestWindow = windowTsSet.has(h.forecastTs);
  }
}

// ─── Hour scoring ─────────────────────────────────────────────────────────────

function scoreOneHour(raw: RawHourData, cfg: ScoringConfig): ScoredHour {
  const passedChecks: string[]       = [];
  const failedChecks: string[]       = [];
  const positiveReasonCodes: string[] = [];
  const riskReasonCodes: string[]    = [];

  // ── 1. Availability checks (before any scoring) ───────────────────────────
  if (!raw.isDaylight) {
    failedChecks.push("no_daylight");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("daylight");

  if (!raw.isBeachOpen) {
    failedChecks.push("beach_closed");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("beach_open");

  // ── 2. Hard NO-GO checks ──────────────────────────────────────────────────
  if (raw.weatherCode !== null && SEVERE_WMO_CODES.has(raw.weatherCode)) {
    failedChecks.push("severe_weather");
    riskReasonCodes.push("severe_weather");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("weather_ok");

  if (raw.precipChance !== null && raw.precipChance >= cfg.nogo_precip_chance) {
    failedChecks.push("high_rain_risk");
    riskReasonCodes.push("high_rain_risk");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("rain_ok");

  if (raw.windSpeed !== null && raw.windSpeed >= cfg.nogo_wind_speed) {
    failedChecks.push("dangerous_wind");
    riskReasonCodes.push("dangerous_wind");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("wind_ok");

  const busynessCategory = deriveBusynessCategory(raw.busynessScore, cfg);
  if (busynessCategory === "too_crowded") {
    failedChecks.push("too_crowded");
    riskReasonCodes.push("too_crowded");
    return buildResult(raw, "no_go", null, false,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {});
  }
  passedChecks.push("crowd_ok");

  // ── 3. Caution checks ─────────────────────────────────────────────────────
  let isCaution = false;

  if (raw.precipChance !== null && raw.precipChance >= cfg.caution_precip_chance) {
    isCaution = true;
    riskReasonCodes.push("rain_risk");
    failedChecks.push("caution_rain");
  } else {
    passedChecks.push("low_rain");
  }

  if (raw.windSpeed !== null && raw.windSpeed >= cfg.caution_wind_speed) {
    isCaution = true;
    riskReasonCodes.push("strong_wind");
    failedChecks.push("caution_wind");
  } else {
    passedChecks.push("calm_wind_check");
  }

  if (raw.tideHeight !== null && raw.tideHeight >= cfg.caution_tide_height) {
    isCaution = true;
    riskReasonCodes.push("high_tide");
    failedChecks.push("caution_tide");
  } else {
    passedChecks.push("tide_ok");
  }

  if (raw.uvIndex !== null && raw.uvIndex >= cfg.caution_uv_index) {
    isCaution = true;
    riskReasonCodes.push("high_uv");
    failedChecks.push("caution_uv");
  } else {
    passedChecks.push("uv_ok");
  }

  if (busynessCategory === "dog_party") {
    isCaution = true;
    riskReasonCodes.push("dog_party_crowds");
    failedChecks.push("caution_crowds");
  } else {
    passedChecks.push("crowds_ok");
  }

  const hourStatus: HourStatus = isCaution ? "caution" : "go";

  // ── 4. Positive reason codes ──────────────────────────────────────────────
  if (raw.tideHeight !== null) {
    if (raw.tideHeight <= cfg.positive_very_low_tide) {
      positiveReasonCodes.push("very_low_tide");
    } else if (raw.tideHeight <= cfg.positive_low_tide) {
      positiveReasonCodes.push("low_tide");
    }
  }

  if (raw.precipChance !== null && raw.precipChance <= cfg.positive_low_precip) {
    positiveReasonCodes.push("clear_skies");
  }

  if (raw.windSpeed !== null && raw.windSpeed <= cfg.positive_calm_wind) {
    positiveReasonCodes.push("calm_wind");
  }

  if (busynessCategory === "quiet") {
    positiveReasonCodes.push("quiet_beach");
  }

  if (
    raw.tempAir !== null &&
    raw.tempAir >= cfg.positive_temp_min &&
    raw.tempAir <= cfg.positive_temp_max
  ) {
    positiveReasonCodes.push("perfect_temp");
  }

  if (raw.uvIndex !== null && raw.uvIndex <= cfg.positive_low_uv) {
    positiveReasonCodes.push("low_uv");
  }

  // ── 5. Composite score ────────────────────────────────────────────────────
  const tideScore  = raw.tideHeight !== null
    ? clamp(1 - raw.tideHeight / cfg.norm_tide_max) : 0.5;
  const rainScore  = raw.precipChance !== null
    ? clamp(1 - raw.precipChance / 100) : 0.5;
  const windScore  = raw.windSpeed !== null
    ? clamp(1 - raw.windSpeed / cfg.norm_wind_max) : 0.5;
  const crowdScore = raw.busynessScore !== null
    ? clamp(1 - raw.busynessScore / 100) : 0.5;
  const tempScore  = raw.tempAir !== null
    ? clamp(1 - Math.abs(raw.tempAir - cfg.norm_temp_target) / cfg.norm_temp_range) : 0.5;
  const uvScore    = raw.uvIndex !== null
    ? clamp(1 - raw.uvIndex / cfg.norm_uv_max) : 0.5;

  const hourScore = Math.round((
    tideScore  * cfg.weight_tide  +
    rainScore  * cfg.weight_rain  +
    windScore  * cfg.weight_wind  +
    crowdScore * cfg.weight_crowd +
    tempScore  * cfg.weight_temp  +
    uvScore    * cfg.weight_uv
  ) * 100);

  const explainability: Record<string, number> = {
    tide_score:  round2(tideScore),
    rain_score:  round2(rainScore),
    wind_score:  round2(windScore),
    crowd_score: round2(crowdScore),
    temp_score:  round2(tempScore),
    uv_score:    round2(uvScore),
    hour_score:  hourScore,
  };

  return buildResult(
    raw, hourStatus, hourScore, true,
    passedChecks, failedChecks,
    positiveReasonCodes, riskReasonCodes,
    explainability,
    busynessCategory,
  );
}

// ─── Window selection ─────────────────────────────────────────────────────────

function findBestWindow(
  hours: ScoredHour[],
  cfg: ScoringConfig,
): BestWindow | null {
  // Only candidate hours (passed all NO-GO checks) are eligible
  const candidates = hours.filter((h) => h.isCandidateWindow);
  if (candidates.length === 0) return null;

  let bestWindow: BestWindow | null = null;
  let bestScore = -Infinity;

  for (
    let size = cfg.window_max_hours;
    size >= cfg.window_min_hours;
    size--
  ) {
    // Slide a window of `size` hours across the candidate list.
    // Hours must be contiguous (no gaps in localHour).
    for (let start = 0; start <= hours.length - size; start++) {
      const slice = hours.slice(start, start + size);

      // Verify all hours in slice are candidates and contiguous
      if (!slice.every((h) => h.isCandidateWindow)) continue;
      if (!isContiguous(slice)) continue;

      // Score the window
      const goHours      = slice.filter((h) => h.hourStatus === "go");
      const cautionHours = slice.filter((h) => h.hourStatus === "caution");
      const avgScore     = average(slice.map((h) => h.hourScore ?? 0));
      const penalty      = cautionHours.length * cfg.window_caution_penalty;
      const windowScore  = avgScore - penalty;

      if (windowScore > bestScore) {
        bestScore = windowScore;
        const startTs = slice[0].forecastTs;
        const endTs   = slice[slice.length - 1].forecastTs;
        bestWindow = {
          hours:       slice,
          startTs,
          endTs,
          label:       buildWindowLabel(slice[0].localHour, slice[slice.length - 1].localHour),
          windowScore: round2(windowScore),
          status:      goHours.length === slice.length ? "go" : "caution",
        };
      }
    }
  }

  return bestWindow;
}

function isContiguous(hours: ScoredHour[]): boolean {
  for (let i = 1; i < hours.length; i++) {
    if (hours[i].localHour !== hours[i - 1].localHour + 1) return false;
  }
  return true;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

export function deriveBusynessCategory(
  score: number | null,
  cfg: ScoringConfig,
): BusynessCategory | null {
  if (score === null) return null;
  if (score <= cfg.busy_quiet_max)      return "quiet";
  if (score <= cfg.busy_moderate_max)   return "moderate";
  if (score <= cfg.busy_dog_party_max)  return "dog_party";
  return "too_crowded";
}

function buildWindowLabel(startHour: number, endHour: number): string {
  return `${formatHour(startHour)}–${formatHour(endHour + 1)}`;
}

function formatHour(hour: number): string {
  if (hour === 0 || hour === 24) return "12am";
  if (hour === 12) return "12pm";
  return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
}

export function buildHourLabel(hour: number): string {
  return formatHour(hour);
}

function average(nums: number[]): number {
  if (nums.length === 0) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function clamp(val: number): number {
  return Math.max(0, Math.min(1, val));
}

function round2(val: number): number {
  return Math.round(val * 100) / 100;
}

function buildResult(
  raw: RawHourData,
  status: HourStatus,
  score: number | null,
  isCandidate: boolean,
  passedChecks: string[],
  failedChecks: string[],
  positiveReasonCodes: string[],
  riskReasonCodes: string[],
  explainability: Record<string, number>,
  busynessCategory?: BusynessCategory | null,
): ScoredHour {
  const cat = busynessCategory ?? null;

  // Build a concise hour_text label for the UI chip
  const parts: string[] = [];
  if (raw.tideHeight !== null) parts.push(`${raw.tideHeight.toFixed(1)}ft tide`);
  if (raw.windSpeed  !== null) parts.push(`${Math.round(raw.windSpeed)}mph wind`);
  if (cat)                     parts.push(cat.replace("_", " "));
  const hourText = parts.join(" · ") || status;

  return {
    forecastTs:          raw.forecastTs,
    localDate:           raw.localDate,
    localHour:           raw.localHour,
    hourLabel:           raw.hourLabel,
    isDaylight:          raw.isDaylight,
    weatherCode:         raw.weatherCode,
    tempAir:             raw.tempAir,
    windSpeed:           raw.windSpeed,
    precipChance:        raw.precipChance,
    uvIndex:             raw.uvIndex,
    tideHeight:          raw.tideHeight,
    busynessScore:       raw.busynessScore,
    busynessCategory:    cat,
    hourStatus:          status,
    hourScore:           score,
    isCandidateWindow:   isCandidate,
    isInBestWindow:      false,   // set later by applyBestWindowFlags()
    passedChecks,
    failedChecks,
    positiveReasonCodes,
    riskReasonCodes,
    explainability,
    hourText,
  };
}
