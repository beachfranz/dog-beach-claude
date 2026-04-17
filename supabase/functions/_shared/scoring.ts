// scoring.ts
// Core scoring logic for Dog Beach Scout.
//
// Responsibilities:
//   1. Derive busyness category from raw busyness score
//   2. Estimate sand and asphalt surface temperatures
//   3. Derive per-metric statuses (4 tiers: go / advisory / caution / no_go)
//   4. Compute overall hour_status (worst of all metric statuses)
//   5. Compute 0-100 composite hour_score (scored on feels-like temp)
//   6. Collect passed/failed checks and reason codes
//   7. Select best 2-5 contiguous window from candidate hours
//   8. Build explainability payload per hour
//
// All thresholds and weights come from a ScoringConfig row — never hardcoded.

type HourStatus       = "go" | "advisory" | "caution" | "no_go";
type BusynessCategory = "quiet" | "moderate" | "dog_party" | "too_crowded";

interface ScoringConfig {
  scoring_version:        string;

  // No-go thresholds
  nogo_wind_speed:        number;
  nogo_wmo_codes:         number[];
  nogo_uv_index:          number;    // ≥ this → no_go  (default 11)
  nogo_temp_hot_max:      number;    // > this → no_go  (default 95°F feels-like)
  caution_temp_cold_min:  number;    // < this → caution; below nogo threshold → no_go (default 20)

  // Caution thresholds
  caution_wmo_codes:      number[];  // drizzle, fog, etc.
  caution_precip_chance:  number;    // ≥ this → caution (default 50%)
  caution_wind_speed:     number;    // ≥ this → caution (default 15mph)
  caution_tide_height:    number;    // ≥ this → caution (default 5.0ft)
  caution_uv_index:       number;    // ≥ this → caution (default 8)
  caution_temp_hot_max:   number;    // > this → caution (default 85°F)
  advisory_crowd_max:     number;    // > this → caution crowd (default 84)

  // Advisory thresholds
  advisory_precip_chance: number;    // ≥ this → advisory (default 10%)
  advisory_wind_speed:    number;    // ≥ this → advisory (default 10mph)
  advisory_tide_height:   number;    // ≥ this → advisory (default 3.0ft)
  advisory_uv_index:      number;    // ≥ this → advisory (default 3)
  advisory_temp_cold_min: number;    // < this → advisory cold (default 32°F)
  go_temp_cold_min:       number;    // ≥ this → go for cold metric (default 50°F)
  advisory_temp_hot_max:  number;    // > this → advisory hot (default 75°F)
  advisory_crowd_min:     number;    // ≥ this → advisory crowd (default 61)

  // Surface temp thresholds (°F)
  advisory_sand_temp:     number;    // ≥ this → advisory  (default 105)
  caution_sand_temp:      number;    // ≥ this → caution   (default 115)
  nogo_sand_temp:         number;    // ≥ this → no_go     (default 125)
  advisory_asphalt_temp:  number;    // ≥ this → advisory  (default 105)
  caution_asphalt_temp:   number;    // > this → caution   (default 114)

  // Positive signal thresholds
  positive_low_tide:      number;
  positive_very_low_tide: number;
  positive_low_precip:    number;
  positive_calm_wind:     number;
  positive_temp_min:      number;
  positive_temp_max:      number;
  positive_low_uv:        number;

  // Busyness category boundaries
  busy_quiet_max:         number;
  busy_moderate_max:      number;
  busy_dog_party_max:     number;

  // Component weights (must sum to 1.0)
  weight_tide:            number;
  weight_rain:            number;
  weight_wind:            number;
  weight_crowd:           number;
  weight_temp:            number;
  weight_uv:              number;

  // Normalisation ranges
  norm_tide_max:          number;
  norm_wind_max:          number;
  norm_temp_target:       number;
  norm_temp_range:        number;
  norm_uv_max:            number;

  // Best-window selection
  window_score_threshold: number;
}

// WMO codes that are immediate no-go (thunderstorm, heavy rain, snow, freezing rain)
const SEVERE_WMO_CODES = new Set([
  63, 64, 65, 66, 67,
  71, 72, 73, 74, 75, 76, 77,
  95, 96, 97, 98, 99,
]);

// ─── Input / output types ─────────────────────────────────────────────────────

export interface RawHourData {
  forecastTs:    string;           // UTC ISO timestamptz — the PK
  localDate:     string;           // "YYYY-MM-DD"
  localHour:     number;           // 0-23
  hourLabel:     string;           // "6am", "2pm" etc.
  isDaylight:    boolean;
  weatherCode:   number | null;
  tempAir:       number | null;    // °F — used for surface temp estimation
  feelsLike:     number | null;    // °F apparent_temperature — used for scoring/status
  windSpeed:     number | null;
  precipChance:  number | null;
  uvIndex:       number | null;
  tideHeight:    number | null;
  busynessScore: number | null;    // 0-100 from BestTime
  isBeachOpen:   boolean;
}

export interface ScoredHour {
  // Raw fields pass through
  forecastTs:        string;
  localDate:         string;
  localHour:         number;
  hourLabel:         string;
  isDaylight:        boolean;
  weatherCode:       number | null;
  tempAir:           number | null;
  feelsLike:         number | null;
  windSpeed:         number | null;
  precipChance:      number | null;
  uvIndex:           number | null;
  tideHeight:        number | null;
  busynessScore:     number | null;
  busynessCategory:  BusynessCategory | null;
  // Derived surface temps
  sandTemp:          number | null;
  asphaltTemp:       number | null;
  // Scoring outputs
  hourStatus:        HourStatus;
  hourScore:         number | null;
  isCandidateWindow: boolean;
  isInBestWindow:    boolean;
  passedChecks:      string[];
  failedChecks:      string[];
  positiveReasonCodes: string[];
  riskReasonCodes:   string[];
  explainability:    Record<string, number>;
  metricStatuses:    Record<string, HourStatus | null>;
  hourText:          string;
}

export interface BestWindow {
  hours:       ScoredHour[];
  startTs:     string;
  endTs:       string;
  label:       string;        // "12pm–4pm"
  windowScore: number;
  status:      HourStatus;
}

// ─── Public API ───────────────────────────────────────────────────────────────

export function scoreHours(
  hours: RawHourData[],
  config: ScoringConfig,
): ScoredHour[] {
  return hours.map((h) => scoreOneHour(h, config));
}

export function selectBestWindows(
  allScoredHours: ScoredHour[],
  config: ScoringConfig,
): Map<string, BestWindow | null> {
  const byDate = new Map<string, ScoredHour[]>();
  for (const h of allScoredHours) {
    const arr = byDate.get(h.localDate) ?? [];
    arr.push(h);
    byDate.set(h.localDate, arr);
  }
  const result = new Map<string, BestWindow | null>();
  for (const [date, hours] of byDate) {
    const sorted = [...hours].sort((a, b) => a.localHour - b.localHour);
    result.set(date, findBestWindow(sorted, config));
  }
  return result;
}

export function applyBestWindowFlags(
  scoredHours: ScoredHour[],
  windows: Map<string, BestWindow | null>,
): void {
  const windowTsSet = new Set<string>();
  for (const window of windows.values()) {
    if (window) {
      for (const h of window.hours) windowTsSet.add(h.forecastTs);
    }
  }
  for (const h of scoredHours) {
    h.isInBestWindow = windowTsSet.has(h.forecastTs);
  }
}

// ─── Surface temperature estimation ──────────────────────────────────────────

// Solar heat addition by UV index (°F above air temp on exposed surface).
// Based on empirical energy balance research — asphalt absorbs ~92% of solar radiation.
function solarAdd(uvIndex: number): number {
  if (uvIndex >= 11) return 65;
  if (uvIndex >= 9)  return 55;
  if (uvIndex >= 7)  return 45;
  if (uvIndex >= 5)  return 30;
  if (uvIndex >= 3)  return 20;
  if (uvIndex >= 1)  return 10;
  return 0;
}

// Returns estimated sand and asphalt surface temps (°F), or null at night.
function estimateSurfaceTemps(
  airTemp: number | null,
  uvIndex: number | null,
  windSpeed: number | null,
  isDaylight: boolean,
): { sand: number | null; asphalt: number | null } {
  if (!isDaylight || airTemp === null) return { sand: null, asphalt: null };
  const solar   = solarAdd(uvIndex ?? 0);
  const cooling = (windSpeed ?? 0) * 0.5;
  return {
    sand:    round1(airTemp + solar * 0.7 - cooling),  // sand albedo ~30% higher than asphalt
    asphalt: round1(airTemp + solar - cooling),
  };
}

// ─── Status helpers ───────────────────────────────────────────────────────────

const STATUS_RANK: Record<HourStatus, number> = {
  go: 0, advisory: 1, caution: 2, no_go: 3,
};

function worstOf(...statuses: (HourStatus | null)[]): HourStatus {
  let worst: HourStatus = "go";
  for (const s of statuses) {
    if (s !== null && STATUS_RANK[s] > STATUS_RANK[worst]) worst = s;
  }
  return worst;
}

// ─── Per-metric status derivations ───────────────────────────────────────────

function tideStatus(h: number | null, cfg: ScoringConfig): HourStatus | null {
  if (h === null) return null;
  if (h >= cfg.caution_tide_height)  return "caution";   // ≥ 5.0 ft
  if (h >= cfg.advisory_tide_height) return "advisory";  // 3.0–4.9 ft
  return "go";
}

function windStatus(s: number | null, cfg: ScoringConfig): HourStatus | null {
  if (s === null) return null;
  if (s >= cfg.nogo_wind_speed)     return "no_go";    // ≥ 25 mph
  if (s >= cfg.caution_wind_speed)  return "caution";  // 15–24.9 mph
  if (s >= cfg.advisory_wind_speed) return "advisory"; // 10–14.9 mph
  return "go";
}

function rainStatus(p: number | null, cfg: ScoringConfig): HourStatus | null {
  if (p === null) return null;
  if (p >= cfg.caution_precip_chance)  return "caution";  // ≥ 50%
  if (p >= cfg.advisory_precip_chance) return "advisory"; // 10–49%
  return "go";
}

function crowdStatus(score: number | null, cfg: ScoringConfig): HourStatus | null {
  if (score === null) return null;
  if (score > cfg.advisory_crowd_max)   return "caution";  // ≥ 85
  if (score >= cfg.advisory_crowd_min)  return "advisory"; // 61–84
  return "go";
}

// Cold side: returns null when temp is in go-or-warmer territory
function tempColdStatus(feelsLike: number, cfg: ScoringConfig): HourStatus {
  if (feelsLike >= cfg.go_temp_cold_min)       return "go";
  if (feelsLike >= cfg.advisory_temp_cold_min) return "advisory"; // 32–49.9°F
  if (feelsLike >= cfg.caution_temp_cold_min)  return "caution";  // 20–31.9°F
  return "no_go";                                                  // < 20°F
}

// Hot side: returns "go" when temp is in comfortable-or-cooler territory
function tempHotStatus(feelsLike: number, cfg: ScoringConfig): HourStatus {
  if (feelsLike <= cfg.advisory_temp_hot_max) return "go";
  if (feelsLike > cfg.nogo_temp_hot_max)      return "no_go";    // > 95°F
  if (feelsLike > cfg.caution_temp_hot_max)   return "caution";  // 85.1–95°F
  return "advisory";                                              // 75.1–85°F
}

function uvStatus(uv: number | null, cfg: ScoringConfig): HourStatus | null {
  if (uv === null) return null;
  if (uv >= cfg.nogo_uv_index)     return "no_go";    // ≥ 11
  if (uv >= cfg.caution_uv_index)  return "caution";  // 8–10
  if (uv >= cfg.advisory_uv_index) return "advisory"; // 3–7
  return "go";
}

function sandStatus(temp: number | null, cfg: ScoringConfig): HourStatus | null {
  if (temp === null) return null;
  if (temp >= cfg.nogo_sand_temp)      return "no_go";    // ≥ 125°F
  if (temp >= cfg.caution_sand_temp)   return "caution";  // 115–124°F
  if (temp >= cfg.advisory_sand_temp)  return "advisory"; // 105–114°F
  return "go";
}

function asphaltStatus(temp: number | null, cfg: ScoringConfig): HourStatus | null {
  if (temp === null) return null;
  if (temp > cfg.caution_asphalt_temp)    return "caution";  // > 114°F
  if (temp >= cfg.advisory_asphalt_temp)  return "advisory"; // 105–114°F
  return "go";
}

function weatherCodeStatus(code: number | null, cfg: ScoringConfig): HourStatus | null {
  if (code === null) return null;
  if (SEVERE_WMO_CODES.has(code))                          return "no_go";
  if ((cfg.caution_wmo_codes ?? []).includes(code))        return "caution";
  return "go";
}

// ─── Hour scoring ─────────────────────────────────────────────────────────────

function scoreOneHour(raw: RawHourData, cfg: ScoringConfig): ScoredHour {
  const passedChecks:       string[] = [];
  const failedChecks:       string[] = [];
  const positiveReasonCodes:string[] = [];
  const riskReasonCodes:    string[] = [];

  // ── Surface temp estimation ───────────────────────────────────────────────
  const { sand: sandTemp, asphalt: asphaltTemp } = estimateSurfaceTemps(
    raw.tempAir, raw.uvIndex, raw.windSpeed, raw.isDaylight,
  );

  // Use feels-like for temp scoring/status; fall back to air temp if absent
  const feelsLike = raw.feelsLike ?? raw.tempAir;

  // ── Pre-compute all metric statuses (needed for early-exit results) ───────
  const ms = {
    tide:         tideStatus(raw.tideHeight, cfg),
    wind:         windStatus(raw.windSpeed, cfg),
    rain:         rainStatus(raw.precipChance, cfg),
    crowd:        crowdStatus(raw.busynessScore, cfg),
    temp_cold:    feelsLike !== null ? tempColdStatus(feelsLike, cfg) : null,
    temp_hot:     feelsLike !== null ? tempHotStatus(feelsLike, cfg)  : null,
    uv:           uvStatus(raw.uvIndex, cfg),
    sand:         sandStatus(sandTemp, cfg),
    asphalt:      asphaltStatus(asphaltTemp, cfg),
    weather_code: weatherCodeStatus(raw.weatherCode, cfg),
  };

  // Combined temp status (worst of cold + hot for backward compat)
  const tempStatusCombined = worstOf(ms.temp_cold, ms.temp_hot);

  const metricStatuses: Record<string, HourStatus | null> = {
    tide_status:         ms.tide,
    wind_status:         ms.wind,
    rain_status:         ms.rain,
    crowd_status:        ms.crowd,
    temp_status:         tempStatusCombined,   // backward compat
    temp_cold_status:    ms.temp_cold,
    temp_hot_status:     ms.temp_hot,
    uv_status:           ms.uv,
    sand_status:         ms.sand,
    asphalt_status:      ms.asphalt,
  };

  // ── Composite score (always computed — needed for no_go hours too) ────────
  const tideScore  = raw.tideHeight  !== null ? clamp(1 - raw.tideHeight  / cfg.norm_tide_max)  : 0.5;
  const rainScore  = raw.precipChance !== null ? clamp(1 - raw.precipChance / 100)               : 0.5;
  const windScore  = raw.windSpeed    !== null ? clamp(1 - raw.windSpeed    / cfg.norm_wind_max) : 0.5;
  const crowdScore = raw.busynessScore !== null ? clamp(1 - raw.busynessScore / 100)             : 0.5;
  const tempScore  = feelsLike        !== null
    ? clamp(1 - Math.abs(feelsLike - cfg.norm_temp_target) / cfg.norm_temp_range) : 0.5;
  const uvScore    = raw.uvIndex      !== null ? clamp(1 - raw.uvIndex      / cfg.norm_uv_max)   : 0.5;

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

  // ── 1. Availability gates (no score/candidate if not open/daylight) ───────
  if (!raw.isDaylight) {
    failedChecks.push("no_daylight");
    return buildResult(raw, "no_go", null, false, sandTemp, asphaltTemp,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {}, null, metricStatuses);
  }
  passedChecks.push("daylight");

  if (!raw.isBeachOpen) {
    failedChecks.push("beach_closed");
    return buildResult(raw, "no_go", null, false, sandTemp, asphaltTemp,
      passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes, {}, null, metricStatuses);
  }
  passedChecks.push("beach_open");

  // ── 2. Derive overall status from all metric statuses ─────────────────────
  const overallStatus = worstOf(
    ms.weather_code,
    ms.wind,
    ms.temp_cold,
    ms.temp_hot,
    ms.uv,
    ms.sand,
    ms.rain,
    ms.crowd,
    ms.tide,
    ms.asphalt,
  );

  // ── 3. Build reason codes ─────────────────────────────────────────────────
  const isCandidate = overallStatus !== "no_go";

  if (ms.weather_code === "no_go")   { failedChecks.push("severe_weather");  riskReasonCodes.push("severe_weather"); }
  else if (ms.weather_code === "caution") { failedChecks.push("caution_weather"); riskReasonCodes.push("bad_weather"); }
  else passedChecks.push("weather_ok");

  if (ms.wind === "no_go")           { failedChecks.push("dangerous_wind");  riskReasonCodes.push("dangerous_wind"); }
  else if (ms.wind === "caution")    { failedChecks.push("caution_wind");    riskReasonCodes.push("strong_wind"); }
  else if (ms.wind === "advisory")   { failedChecks.push("advisory_wind");   riskReasonCodes.push("breezy"); }
  else passedChecks.push("wind_ok");

  if (ms.rain === "caution")         { failedChecks.push("caution_rain");    riskReasonCodes.push("rain_risk"); }
  else if (ms.rain === "advisory")   { failedChecks.push("advisory_rain");   riskReasonCodes.push("some_rain_chance"); }
  else passedChecks.push("low_rain");

  if (ms.crowd === "caution")        { failedChecks.push("caution_crowds");  riskReasonCodes.push("crowded"); }
  else if (ms.crowd === "advisory")  { failedChecks.push("advisory_crowds"); riskReasonCodes.push("moderate_crowds"); }
  else passedChecks.push("crowds_ok");

  if (ms.tide === "caution")         { failedChecks.push("caution_tide");    riskReasonCodes.push("high_tide"); }
  else if (ms.tide === "advisory")   { failedChecks.push("advisory_tide");   riskReasonCodes.push("rising_tide"); }
  else passedChecks.push("tide_ok");

  if (ms.uv === "no_go")             { failedChecks.push("extreme_uv");      riskReasonCodes.push("extreme_uv"); }
  else if (ms.uv === "caution")      { failedChecks.push("caution_uv");      riskReasonCodes.push("high_uv"); }
  else if (ms.uv === "advisory")     { failedChecks.push("advisory_uv");     riskReasonCodes.push("moderate_uv"); }
  else passedChecks.push("uv_ok");

  if (tempStatusCombined === "no_go")      { failedChecks.push("extreme_temp");  riskReasonCodes.push("extreme_temp"); }
  else if (tempStatusCombined === "caution") { failedChecks.push("caution_temp"); riskReasonCodes.push("temp_out_of_range"); }
  else if (tempStatusCombined === "advisory") { failedChecks.push("advisory_temp"); riskReasonCodes.push("cool_or_warm"); }
  else passedChecks.push("temp_ok");

  if (ms.sand === "no_go")           { failedChecks.push("nogo_sand_temp");  riskReasonCodes.push("hot_sand"); }
  else if (ms.sand === "caution")    { failedChecks.push("caution_sand");    riskReasonCodes.push("warm_sand"); }
  else if (ms.sand === "advisory")   { failedChecks.push("advisory_sand");   riskReasonCodes.push("sand_warming"); }

  if (ms.asphalt === "caution")      { failedChecks.push("caution_asphalt"); riskReasonCodes.push("hot_pavement"); }
  else if (ms.asphalt === "advisory"){ failedChecks.push("advisory_asphalt");riskReasonCodes.push("warm_pavement"); }

  // ── 4. Positive reason codes ──────────────────────────────────────────────
  if (raw.tideHeight !== null) {
    if (raw.tideHeight <= cfg.positive_very_low_tide)  positiveReasonCodes.push("very_low_tide");
    else if (raw.tideHeight <= cfg.positive_low_tide)  positiveReasonCodes.push("low_tide");
  }
  if (raw.precipChance !== null && raw.precipChance <= cfg.positive_low_precip) {
    positiveReasonCodes.push("clear_skies");
  }
  if (raw.windSpeed !== null && raw.windSpeed <= cfg.positive_calm_wind) {
    positiveReasonCodes.push("calm_wind");
  }
  if (ms.crowd === "go") positiveReasonCodes.push("quiet_beach");
  if (feelsLike !== null && feelsLike >= cfg.positive_temp_min && feelsLike <= cfg.positive_temp_max) {
    positiveReasonCodes.push("perfect_temp");
  }
  if (raw.uvIndex !== null && raw.uvIndex <= cfg.positive_low_uv) {
    positiveReasonCodes.push("low_uv");
  }

  const busynessCategory = deriveBusynessCategory(raw.busynessScore, cfg);

  return buildResult(
    raw, overallStatus, hourScore, isCandidate, sandTemp, asphaltTemp,
    passedChecks, failedChecks, positiveReasonCodes, riskReasonCodes,
    explainability, busynessCategory, metricStatuses,
  );
}

// ─── Window selection ─────────────────────────────────────────────────────────

function findBestWindow(
  hours: ScoredHour[],
  cfg: ScoringConfig,
): BestWindow | null {
  const candidates = hours.filter((h) => h.isCandidateWindow);
  if (candidates.length === 0) return null;

  const peak      = candidates.reduce((best, h) =>
    (h.hourScore ?? 0) > (best.hourScore ?? 0) ? h : best
  );
  const peakScore = peak.hourScore ?? 0;
  const peakIndex = hours.indexOf(peak);
  const STEP      = 0.05;
  let threshold   = cfg.window_score_threshold ?? 0.93;

  let window: ScoredHour[] = [];

  while (true) {
    const minScore = peakScore * threshold;
    window = [peak];

    // Expand forward
    for (let i = peakIndex + 1; i < hours.length; i++) {
      const h    = hours[i];
      const prev = window[window.length - 1];
      if (h.localHour !== prev.localHour + 1) break;
      if (!h.isCandidateWindow)               break;
      if ((h.hourScore ?? 0) < minScore)      break;
      window.push(h);
    }

    // Expand backward
    for (let i = peakIndex - 1; i >= 0; i--) {
      const h    = hours[i];
      const next = window[0];
      if (next.localHour !== h.localHour + 1) break;
      if (!h.isCandidateWindow)               break;
      if ((h.hourScore ?? 0) < minScore)      break;
      window.unshift(h);
    }

    if (window.length >= 2) break;
    if (threshold <= 0)     break;
    threshold = Math.max(0, threshold - STEP);
  }

  if (window.length < 2) return null;

  const avgScore = average(window.map((h) => h.hourScore ?? 0));

  // Window status = worst status of any hour in the window
  const windowStatus = window.reduce<HourStatus>(
    (worst, h) => STATUS_RANK[h.hourStatus] > STATUS_RANK[worst] ? h.hourStatus : worst,
    "go",
  );

  return {
    hours:       window,
    startTs:     window[0].forecastTs,
    endTs:       window[window.length - 1].forecastTs,
    label:       buildWindowLabel(window[0].localHour, window[window.length - 1].localHour),
    windowScore: round2(avgScore),
    status:      windowStatus,
  };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

export function deriveBusynessCategory(
  score: number | null,
  cfg: ScoringConfig,
): BusynessCategory | null {
  if (score === null) return null;
  if (score <= cfg.busy_quiet_max)     return "quiet";
  if (score <= cfg.busy_moderate_max)  return "moderate";
  if (score <= cfg.busy_dog_party_max) return "dog_party";
  return "too_crowded";
}

export function buildHourLabel(hour: number): string {
  return formatHour(hour);
}

function buildWindowLabel(startHour: number, endHour: number): string {
  return `${formatHour(startHour)}–${formatHour(endHour + 1)}`;
}

function formatHour(hour: number): string {
  if (hour === 0 || hour === 24) return "12am";
  if (hour === 12) return "12pm";
  return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
}

function average(nums: number[]): number {
  if (nums.length === 0) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function clamp(val: number): number {
  return Math.max(0, Math.min(1, val));
}

function round1(val: number): number {
  return Math.round(val * 10) / 10;
}

function round2(val: number): number {
  return Math.round(val * 100) / 100;
}

function buildResult(
  raw:                  RawHourData,
  status:               HourStatus,
  score:                number | null,
  isCandidate:          boolean,
  sandTemp:             number | null,
  asphaltTemp:          number | null,
  passedChecks:         string[],
  failedChecks:         string[],
  positiveReasonCodes:  string[],
  riskReasonCodes:      string[],
  explainability:       Record<string, number>,
  busynessCategory:     BusynessCategory | null,
  metricStatuses:       Record<string, HourStatus | null>,
): ScoredHour {
  const parts: string[] = [];
  if (raw.tideHeight !== null) parts.push(`${raw.tideHeight.toFixed(1)}ft tide`);
  if (raw.windSpeed  !== null) parts.push(`${Math.round(raw.windSpeed)}mph wind`);
  if (busynessCategory)        parts.push(busynessCategory.replace("_", " "));
  const hourText = parts.join(" · ") || status;

  return {
    forecastTs:          raw.forecastTs,
    localDate:           raw.localDate,
    localHour:           raw.localHour,
    hourLabel:           raw.hourLabel,
    isDaylight:          raw.isDaylight,
    weatherCode:         raw.weatherCode,
    tempAir:             raw.tempAir,
    feelsLike:           raw.feelsLike,
    windSpeed:           raw.windSpeed,
    precipChance:        raw.precipChance,
    uvIndex:             raw.uvIndex,
    tideHeight:          raw.tideHeight,
    busynessScore:       raw.busynessScore,
    busynessCategory,
    sandTemp,
    asphaltTemp,
    hourStatus:          status,
    hourScore:           score,
    isCandidateWindow:   isCandidate,
    isInBestWindow:      false,
    passedChecks,
    failedChecks,
    positiveReasonCodes,
    riskReasonCodes,
    explainability,
    metricStatuses,
    hourText,
  };
}
