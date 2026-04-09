// besttime.ts
// Fetches weekly crowd forecast from BestTime.app Forecasting API.
// Docs: https://besttime.app/api/v1/docs
//
// Flow:
//   1. If beach has no besttime_venue_id → call newForecast() to register the
//      venue and get a venue_id back. Persist it to the beaches table.
//   2. If beach already has a besttime_venue_id → call weekForecast() to get
//      the latest weekly pattern without consuming extra credits.
//
// Output: Map<"DAY_OF_WEEK:HOUR", number> where value is busyness 0–100.
// DAY_OF_WEEK: 0 = Monday … 6 = Sunday (BestTime convention).
// HOUR: 0–23.
//
// The caller maps this to actual calendar dates by using JavaScript's
// date.getDay() (0 = Sunday … 6 = Saturday) with a conversion offset.

const BASE_URL = "https://besttime.app/api/v1";

// BestTime day index → JS getDay() mapping:
//   BestTime: 0=Mon 1=Tue 2=Wed 3=Thu 4=Fri 5=Sat 6=Sun
//   JS:       0=Sun 1=Mon 2=Tue 3=Wed 4=Thu 5=Fri 6=Sat
// Convert JS getDay() → BestTime day index:
export function jsDayToBestTimeDay(jsDay: number): number {
  // jsDay: 0(Sun)→6, 1(Mon)→0, 2(Tue)→1 ... 6(Sat)→5
  return jsDay === 0 ? 6 : jsDay - 1;
}

export interface BestTimeWeekResult {
  // Keyed by `${bestTimeDayIndex}:${hour}` → busyness score 0–100
  busynessMap: Map<string, number>;
  // venue_id returned by BestTime — persist if this was a new venue registration
  venueId: string;
  // True if this was a new venue registration (caller should persist venue_id)
  isNewVenue: boolean;
}

export async function fetchCrowds(
  beach: {
    location_id: string;
    display_name: string;
    address: string | null;
    besttime_venue_id: string | null;
  },
  apiKeyPrivate: string,
  apiKeyPublic: string,
): Promise<BestTimeWeekResult> {
  if (beach.besttime_venue_id) {
    // Venue already registered — fetch existing weekly forecast (no credit cost)
    return fetchWeekForecast(beach.besttime_venue_id, apiKeyPublic);
  } else {
    // First time — register venue and get forecast (consumes credits)
    return newForecast(beach, apiKeyPrivate, apiKeyPublic);
  }
}

// ── New venue registration ────────────────────────────────────────────────────

async function newForecast(
  beach: {
    location_id: string;
    display_name: string;
    address: string | null;
  },
  apiKeyPrivate: string,
  apiKeyPublic: string,
): Promise<BestTimeWeekResult> {
  if (!beach.address) {
    throw new Error(
      `Beach ${beach.location_id} has no address — required for BestTime.app venue registration`
    );
  }

  const body = new URLSearchParams();
  body.append("api_key_private", apiKeyPrivate);
  body.append("venue_name",      beach.display_name);
  body.append("venue_address",   beach.address);

  console.log("BestTime request — venue_name:", beach.display_name);
  console.log("BestTime request — venue_address:", beach.address);
  console.log("BestTime request — api_key_private length:", apiKeyPrivate?.length ?? "UNDEFINED");

  const res = await fetchWithRetry(
    `${BASE_URL}/forecasts`,
    { method: "POST", body: body.toString(), headers: { "Content-Type": "application/x-www-form-urlencoded" } },
    3,
  );

  if (!res.ok) {
    throw new Error(
      `BestTime new forecast error ${res.status} for ${beach.location_id}: ${await res.text()}`
    );
  }

  const json = await res.json();

  if (!json.status || json.status !== "OK") {
    throw new Error(
      `BestTime new forecast failed for ${beach.location_id}: ${JSON.stringify(json)}`
    );
  }

  const venueId: string = json.venue_info?.venue_id ?? json.venue_id;
  if (!venueId) {
    throw new Error(
      `BestTime did not return a venue_id for ${beach.location_id}: ${JSON.stringify(json)}`
    );
  }

  const busynessMap = parseWeekAnalysis(json.analysis);

  return { busynessMap, venueId, isNewVenue: true };
}

// ── Existing venue weekly forecast ───────────────────────────────────────────

async function fetchWeekForecast(
  venueId: string,
  apiKeyPublic: string,
): Promise<BestTimeWeekResult> {
  const params = new URLSearchParams({
    api_key_public: apiKeyPublic,
    venue_id:       venueId,
  });

  const res = await fetchWithRetry(
    `${BASE_URL}/forecasts/week?${params}`,
    { method: "GET" },
    3,
  );

  if (!res.ok) {
    throw new Error(
      `BestTime week forecast error ${res.status} for venue ${venueId}: ${await res.text()}`
    );
  }

  const json = await res.json();

  if (!json.status || json.status !== "OK") {
    throw new Error(
      `BestTime week forecast failed for venue ${venueId}: ${JSON.stringify(json)}`
    );
  }

  const busynessMap = parseWeekAnalysis(json.analysis);

  return { busynessMap, venueId, isNewVenue: false };
}

// ── Response parser ───────────────────────────────────────────────────────────
//
// BestTime week analysis structure:
// analysis: [
//   {                              ← day 0 = Monday
//     day_raw: [
//       { hour: 6, intensity_nr: 12 },
//       { hour: 7, intensity_nr: 18 },
//       ...
//     ]
//   },
//   ...                            ← days 1–6
// ]

function parseWeekAnalysis(analysis: unknown): Map<string, number> {
  const map = new Map<string, number>();

  if (!Array.isArray(analysis)) return map;

  analysis.forEach((day: unknown, dayIndex: number) => {
    const d = day as { day_raw?: Array<{ hour: number; intensity_nr: number }> };
    if (!Array.isArray(d.day_raw)) return;

    for (const slot of d.day_raw) {
      if (typeof slot.hour === "number" && typeof slot.intensity_nr === "number") {
        const key = `${dayIndex}:${slot.hour}`;
        map.set(key, Math.max(0, Math.min(100, slot.intensity_nr)));
      }
    }
  });

  return map;
}

// ── Retry helper ──────────────────────────────────────────────────────────────

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
