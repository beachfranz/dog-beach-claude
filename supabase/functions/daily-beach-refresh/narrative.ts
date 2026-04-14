// narrative.ts
// Generates plain-language narrative text for beach day recommendations
// using the Anthropic API (claude-sonnet-4-20250514).
//
// Produces four text fields that map directly to beach_day_recommendations:
//   - day_text:          2-3 sentence day overview
//   - best_window_text:  1-2 sentences on why this window was chosen
//   - caution_text:      1 sentence caveat (omitted for clean 'go' days)
//   - no_go_text:        1 sentence explanation for no_go days 
//
// Also produces hour_text for each hour in beach_day_hourly_scores —
// a short chip label like "0.3ft · calm · quiet"

import type { ScoredHour, BestWindow } from "./scoring.ts";
// Types inlined — no external path imports in edge functions
type DayStatus        = "go" | "caution" | "no_go";
type BusynessCategory = "quiet" | "moderate" | "dog_party" | "too_crowded";
type TideDirection    = "rising" | "falling" | "steady";

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages";
const MODEL = "claude-sonnet-4-20250514";
const MAX_TOKENS = 1000;

// ─── Input types ──────────────────────────────────────────────────────────────

export interface WindowHour {
  hour: string;           // "9am"
  tide: number | null;    // ft
  wind: number | null;    // mph
  temp: number | null;    // °F
  rain: number | null;    // %
  crowd: string | null;   // busyness category
  status: "go" | "caution";
}

export interface NarrativeInput {
  beachName: string;
  localDate: string;          // "YYYY-MM-DD"
  dayOfWeek: string;          // "Sunday"
  isWeekend: boolean;
  dayStatus: DayStatus;
  bestWindow: BestWindow | null;
  weatherCode: number | null;
  tideDirection: TideDirection;
  windowHourBreakdown: WindowHour[];
  // Aggregates over the best window hours (or full day if no window)
  avgTemp: number | null;
  avgWind: number | null;
  avgPrecip: number | null;
  avgTide: number | null;
  lowestTide: number | null;
  avgUv: number | null;
  avgBusyness: number | null;
  busynessCategory: BusynessCategory | null;
  positiveReasonCodes: string[];
  riskReasonCodes: string[];
  goHoursCount: number;
  cautionHoursCount: number;
  noGoHoursCount: number;
}

export interface NarrativeOutput {
  dayText: string;
  bestWindowText: string;
  cautionText: string;
  noGoText: string;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Generate day-level narrative for a single beach-date.
 */
export async function generateDayNarrative(
  input: NarrativeInput,
  anthropicApiKey: string,
): Promise<NarrativeOutput> {
  const prompt = buildDayPrompt(input);
  const raw = await callAnthropic(prompt, anthropicApiKey);
  return parseDayNarrative(raw, input.dayStatus);
}

/**
 * Generate short hour_text labels for a set of scored hours.
 * Batches all hours for a beach-day into a single API call to minimise cost.
 */
export async function generateHourLabels(
  hours: ScoredHour[],
  beachName: string,
  anthropicApiKey: string,
): Promise<Map<string, string>> {
  // Only generate labels for candidate hours — no_go hours get a static label
  const candidates = hours.filter((h) => h.isCandidateWindow);
  const staticLabels = new Map<string, string>();

  for (const h of hours) {
    if (!h.isCandidateWindow) {
      staticLabels.set(h.forecastTs, buildStaticHourText(h));
    }
  }

  if (candidates.length === 0) return staticLabels;

  const prompt = buildHourLabelsPrompt(candidates, beachName);
  const raw = await callAnthropic(prompt, anthropicApiKey);
  const generated = parseHourLabels(raw, candidates);

  return new Map([...staticLabels, ...generated]);
}

// ─── Prompt builders ──────────────────────────────────────────────────────────

function buildDayPrompt(input: NarrativeInput): string {
  // Feels-like temperature using wind chill formula
  const temp = input.avgTemp ?? null;
  const wind = input.avgWind ?? null;
  let feelsLike: number | null = null;
  if (temp !== null && wind !== null) {
    if (temp <= 50 || wind >= 3) {
      feelsLike = Math.round(
        35.74 + 0.6215 * temp - 35.75 * Math.pow(wind, 0.16) + 0.4275 * temp * Math.pow(wind, 0.16)
      );
    } else {
      feelsLike = Math.round(temp);
    }
  }

  // Practical tips based on conditions
  const tips: string[] = [];
  if (input.avgUv !== null && input.avgUv >= 6) {
    tips.push("sunscreen recommended (UV " + Math.round(input.avgUv) + ")");
  }
  if (feelsLike !== null && feelsLike < 62) {
    tips.push("bring a layer or hoodie (feels like " + feelsLike + "°F)");
  }
  if (input.lowestTide !== null && input.lowestTide <= 0.5) {
    tips.push("low tide means extra beach space");
  }
  if (input.busynessCategory === "moderate" || input.busynessCategory === "dog_party") {
    tips.push("go earlier in the window to beat the crowds");
  }

  const weatherDesc = input.weatherCode !== null
    ? wmoToDescription(input.weatherCode)
    : null;

  const weekendNote = input.isWeekend
    ? "Weekend — expect more people than a weekday."
    : "Weekday — likely quieter than usual.";

  const tideNote = `Tide is ${input.tideDirection} through the window.`;

  const windowSection = input.bestWindow
    ? `Best visit window: ${input.bestWindow.label} (${input.bestWindow.status})
  - Weather: ${weatherDesc ?? "unknown"}
  - Avg tide: ${fmt(input.avgTide, "ft")} (lowest: ${fmt(input.lowestTide, "ft")}, ${tideNote})
  - Avg wind: ${fmt(input.avgWind, "mph")}
  - Rain chance: ${fmt(input.avgPrecip, "%")}
  - Temperature: ${fmt(input.avgTemp, "°F")}${feelsLike !== null ? ` (feels like ${feelsLike}°F)` : ""}
  - UV index: ${fmt(input.avgUv, "")}
  - Crowds: ${input.busynessCategory ?? "unknown"}
  - ${weekendNote}${tips.length ? `\n  - Practical tips: ${tips.join("; ")}` : ""}

Hour-by-hour breakdown:
${input.windowHourBreakdown.map(h =>
  `  ${h.hour}: tide=${fmt(h.tide, "ft")} wind=${fmt(h.wind, "mph")} temp=${fmt(h.temp, "°F")} rain=${fmt(h.rain, "%")} crowd=${h.crowd ?? "unknown"} [${h.status.toUpperCase()}]`
).join("\n")}`
    : `No suitable visit window found today.
  - Weather: ${weatherDesc ?? "unknown"}
  - ${weekendNote}`;

  const reasonSection = [
    input.positiveReasonCodes.length
      ? `Positives: ${input.positiveReasonCodes.join(", ")}`
      : null,
    input.riskReasonCodes.length
      ? `Risks: ${input.riskReasonCodes.join(", ")}`
      : null,
  ]
    .filter(Boolean)
    .join("\n  ");

  return `You're a local who surfs and brings their dog to ${input.beachName} regularly. Write a beach forecast for dog owners.

DATE: ${input.dayOfWeek}, ${formatDisplayDate(input.localDate)}
STATUS: ${input.dayStatus.toUpperCase()}
HOURS BREAKDOWN: ${input.goHoursCount} go / ${input.cautionHoursCount} caution / ${input.noGoHoursCount} no-go

${windowSection}
${reasonSection ? `\n  ${reasonSection}` : ""}

Write four fields as a JSON object. Rules:
- day_text: 3-4 sentences. Lead with the most important condition (tide, weather, or crowds) with specific numbers. Reference tide direction, weather description, and any specific caution hours if relevant. Work in practical tips naturally. Casual first-person tone — like texting a friend. Not corny, not over-enthusiastic. Mention weekend/weekday context if it affects crowds.
- best_window_text: 1-2 sentences on exactly why this window beats the others. Reference specific hours or conditions from the breakdown. Omit if status is "no_go".
- caution_text: 1 sentence on the main caveat. Omit (empty string) if day_status is "go" with no risk reason codes.
- no_go_text: 1 sentence explaining why today's a skip. Omit (empty string) if day_status is not "no_go".

Tone: casual, first-person, like a local giving real advice. No fluff, no emojis. Use plain numbers (e.g. "0.3ft tide", "62°F", "14mph winds").
Crowd terms: quiet = few people, moderate = getting busy, dog_party = packed with dogs, too_crowded = avoid.

Respond ONLY with a valid JSON object, no markdown, no preamble:
{"day_text":"...","best_window_text":"...","caution_text":"...","no_go_text":"..."}`;
}

function buildHourLabelsPrompt(hours: ScoredHour[], beachName: string): string {
  const hourList = hours
    .map((h) => {
      const parts = [
        `hour: ${h.hourLabel}`,
        h.tideHeight !== null ? `tide: ${h.tideHeight.toFixed(1)}ft` : null,
        h.windSpeed  !== null ? `wind: ${Math.round(h.windSpeed)}mph` : null,
        h.tempAir    !== null ? `temp: ${Math.round(h.tempAir)}°F` : null,
        h.precipChance !== null ? `rain: ${Math.round(h.precipChance)}%` : null,
        h.busynessCategory ? `crowds: ${h.busynessCategory}` : null,
        `status: ${h.hourStatus}`,
        h.isInBestWindow ? "BEST WINDOW" : null,
      ]
        .filter(Boolean)
        .join(", ");
      return `  "${h.forecastTs}": {${parts}}`;
    })
    .join("\n");

  return `Write ultra-short hour labels for ${beachName} beach hours. Each label is 2-4 words max, shown as a chip in a mobile UI.

Hours:
${hourList}

Rules:
- Lead with the single most notable condition for that hour
- Best window hours: highlight what makes them good (e.g. "Low tide · calm")
- Caution hours: note the main concern (e.g. "Windy · moderate")
- Use plain numbers, no units unless needed for clarity
- No emojis, no punctuation other than "·" as separator
- Max 30 characters per label

Respond ONLY with valid JSON, no markdown:
{"<forecastTs>":"<label>", ...}`;
}

// ─── Response parsers ─────────────────────────────────────────────────────────

function parseDayNarrative(raw: string, dayStatus: DayStatus): NarrativeOutput {
  try {
    const clean = raw.replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(clean);
    return {
      dayText:        String(parsed.day_text        ?? ""),
      bestWindowText: String(parsed.best_window_text ?? ""),
      cautionText:    String(parsed.caution_text     ?? ""),
      noGoText:       String(parsed.no_go_text       ?? ""),
    };
  } catch {
    // Fallback: generate rule-based text if JSON parse fails
    return fallbackNarrative(dayStatus);
  }
}

function parseHourLabels(
  raw: string,
  hours: ScoredHour[],
): Map<string, string> {
  const map = new Map<string, string>();
  try {
    const clean = raw.replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(clean);
    for (const h of hours) {
      const label = parsed[h.forecastTs];
      map.set(h.forecastTs, typeof label === "string" ? label : buildStaticHourText(h));
    }
  } catch {
    // Fallback to static labels for all hours
    for (const h of hours) {
      map.set(h.forecastTs, buildStaticHourText(h));
    }
  }
  return map;
}

// ─── Anthropic API call ───────────────────────────────────────────────────────

async function callAnthropic(
  prompt: string,
  apiKey: string,
): Promise<string> {
  const res = await fetchWithRetry(
    ANTHROPIC_API_URL,
    {
      method: "POST",
      headers: {
        "Content-Type":      "application/json",
        "x-api-key":         apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model:      MODEL,
        max_tokens: MAX_TOKENS,
        messages: [
          { role: "user", content: prompt },
        ],
      }),
    },
    3,
  );

  if (!res.ok) {
    throw new Error(
      `Anthropic API error ${res.status}: ${await res.text()}`
    );
  }

  const json = await res.json();

  // Extract text from content blocks
  const text = (json.content ?? [])
    .map((block: { type: string; text?: string }) =>
      block.type === "text" ? (block.text ?? "") : ""
    )
    .filter(Boolean)
    .join("\n");

  if (!text) {
    throw new Error("Anthropic returned empty content");
  }

  return text;
}

// ─── Fallback / static text builders ─────────────────────────────────────────
// Used when the API call fails or JSON parsing fails.
// Ensures the pipeline never hard-fails due to narrative generation issues.

function fallbackNarrative(dayStatus: DayStatus): NarrativeOutput {
  switch (dayStatus) {
    case "go":
      return {
        dayText:        "Conditions look good for a beach visit today.",
        bestWindowText: "This window has the best combination of tide, wind, and crowd levels.",
        cautionText:    "",
        noGoText:       "",
      };
    case "caution":
      return {
        dayText:        "Conditions are marginal today — check the details before heading out.",
        bestWindowText: "This is the best available window despite some less-than-ideal conditions.",
        cautionText:    "Be prepared for less-than-ideal conditions during your visit.",
        noGoText:       "",
      };
    case "no_go":
      return {
        dayText:        "Today is not a good day for a beach visit.",
        bestWindowText: "",
        cautionText:    "",
        noGoText:       "Conditions today make for a poor beach experience — try another day.",
      };
  }
}

function buildStaticHourText(h: ScoredHour): string {
  if (h.hourStatus === "no_go") {
    const reason = h.failedChecks[0] ?? "no_go";
    const labels: Record<string, string> = {
      no_daylight:    "After dark",
      beach_closed:   "Beach closed",
      severe_weather: "Severe weather",
      high_rain_risk: "Heavy rain",
      dangerous_wind: "Dangerous wind",
      too_crowded:    "Too crowded",
    };
    return labels[reason] ?? "Not recommended";
  }
  const parts: string[] = [];
  if (h.tideHeight !== null) parts.push(`${h.tideHeight.toFixed(1)}ft`);
  if (h.windSpeed  !== null) parts.push(`${Math.round(h.windSpeed)}mph`);
  if (h.busynessCategory)   parts.push(h.busynessCategory.replace("_", " "));
  return parts.join(" · ") || h.hourStatus;
}

// ─── Misc helpers ─────────────────────────────────────────────────────────────

function wmoToDescription(code: number): string {
  if (code === 0)                          return "clear sky";
  if (code === 1)                          return "mainly clear";
  if (code === 2)                          return "partly cloudy";
  if (code === 3)                          return "overcast";
  if (code === 45 || code === 48)          return "foggy";
  if (code === 51 || code === 53)          return "light drizzle";
  if (code === 55)                         return "drizzle";
  if (code === 56 || code === 57)          return "freezing drizzle";
  if (code === 61)                         return "light rain";
  if (code === 63)                         return "rain";
  if (code === 65)                         return "heavy rain";
  if (code === 66 || code === 67)          return "freezing rain";
  if (code >= 71 && code <= 75)            return "snow";
  if (code === 77)                         return "snow grains";
  if (code === 80)                         return "light showers";
  if (code === 81)                         return "rain showers";
  if (code === 82)                         return "heavy showers";
  if (code === 85 || code === 86)          return "snow showers";
  if (code === 95)                         return "thunderstorm";
  if (code === 96 || code === 99)          return "thunderstorm with hail";
  return "unknown";
}

function fmt(val: number | null, unit: string): string {
  return val !== null ? `${val}${unit}` : "n/a";
}

function formatDisplayDate(isoDate: string): string {
  const [y, m, d] = isoDate.split("-").map(Number);
  const months = [
    "Jan","Feb","Mar","Apr","May","Jun",
    "Jul","Aug","Sep","Oct","Nov","Dec",
  ];
  return `${months[m - 1]} ${d}, ${y}`;
}

async function fetchWithRetry(
  url: string,
  init: RequestInit,
  attempts: number,
  delayMs = 1000,
): Promise<Response> {
  let lastError: Error | null = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await fetch(url, init);
      if (res.status < 500) return res;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastError = err as Error;
    }
    if (i < attempts - 1) {
      await new Promise((r) => setTimeout(r, delayMs * Math.pow(2, i)));
    }
  }
  throw lastError ?? new Error("fetchWithRetry: unknown error");
}
