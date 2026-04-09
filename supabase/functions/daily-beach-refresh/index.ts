// besttime.ts
// Fetches weekly crowd forecast from BestTime.app Forecasting API.
// Docs: https://besttime.app/api/v1/docs
//
// NOTE: BestTime requires all params as query string on POST requests,
// not in the request body. This matches the confirmed working pattern.

const BASE_URL = "https://besttime.app/api/v1";

// BestTime day index → JS getDay() mapping:
//   BestTime: 0=Mon 1=Tue 2=Wed 3=Thu 4=Fri 5=Sat 6=Sun
//   JS:       0=Sun 1=Mon 2=Tue 3=Wed 4=Thu 5=Fri 6=Sat
export function jsDayToBestTimeDay(jsDay: number): number {
  return jsDay === 0 ? 6 : jsDay - 1;
}

export interface BestTimeWeekResult {
  busynessMap: Map<string, number>;
  venueId: string;
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
    return fetchWeekForecast(beach.besttime_venue_id, apiKeyPublic);
  } else {
    return newForecast(beach, apiKeyPrivate);
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
): Promise<BestTimeWeekResult> {
  if (!beach.address) {
    throw new Error(
      `Beach ${beach.location_id} has no address — required for BestTime.app venue registration`
    );
  }

  // Send all params as query string on POST — BestTime's required pattern
  const params = new URLSearchParams({
    api_key_private: apiKeyPrivate,
    venue_name:      beach.display_name,
    venue_address:   beach.address,
  });

  console.log(`BestTime newForecast — venue: ${beach.display_name}`);
  console.log(`BestTime newForecast — address: ${beach.address}`);
  console.log(`BestTime newForecast — key length: ${apiKeyPrivate?.length ?? "UNDEFINED"}`);

  const res = await fetchWithRetry(
    `${BASE_URL}/forecasts?${params.toString()}`,
    { method: "POST" },
    3,
  );

  const raw = await res.text();
  console.log(`BestTime newForecast — HTTP status: ${res.status}`);
  console.log(`BestTime newForecast — response: ${raw.slice(0, 200)}`);

  if (!res.ok) {
    throw new Error(
      `BestTime new forecast error ${res.status} for ${beach.location_id}: ${raw}`
    );
  }

  let json: Record<string, unknown>;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new Error(`BestTime returned non-JSON: ${raw}`);
  }

  if (json.status !== "OK" || !Array.isArray(json.analysis)) {
    throw new Error(
      `BestTime new forecast failed for ${beach.location_id}: ${JSON.stringify(json)}`
    );
  }

  const venueInfo = json.venue_info as Record<string, unknown> | undefined;
  const venueId = (venueInfo?.venue_id ?? json.venue_id) as string;
  if (!venueId) {
    throw new Error(`BestTime did not return a venue_id for ${beach.location_id}`);
  }

  const busynessMap = parseWeekAnalysis(json.analysis as unknown[]);
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
    `${BASE_URL}/forecasts/week?${params.toString()}`,
    { method: "GET" },
    3,
  );

  const raw = await res.text();
  if (!res.ok) {
    throw new Error(
      `BestTime week forecast error ${res.status} for venue ${venueId}: ${raw}`
    );
  }

  let json: Record<string, unknown>;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new Error(`BestTime week forecast returned non-JSON: ${raw}`);
  }

  if (json.status !== "OK" || !Array.isArray(json.analysis)) {
    throw new Error(
      `BestTime week forecast failed for venue ${venueId}: ${JSON.stringify(json)}`
    );
  }

  const busynessMap = parseWeekAnalysis(json.analysis as unknown[]);
  return { busynessMap, venueId, isNewVenue: false };
}

// ── Response parser ───────────────────────────────────────────────────────────
//
// BestTime analysis structure:
// analysis: [
//   { day_info: { day_int: 0 }, day_raw: [score0, score1, ...] }  ← Monday
//   ...
// ]
// day_raw index 0 = 6am, index 17 = 11pm, index 18 = 12am, ..., index 23 = 5am

function parseWeekAnalysis(analysis: unknown[]): Map<string, number> {
  const map = new Map<string, number>();

  for (const day of analysis) {
    const d = day as { day_info?: { day_int?: number }; day_raw?: number[] };
    const dayIndex = d.day_info?.day_int;
    if (dayIndex === undefined || !Array.isArray(d.day_raw)) continue;

    d.day_raw.forEach((score: number, ix: number) => {
      // BestTime index 0 = 6am, wraps around midnight
      const actualHour = (ix + 6) % 24;
      const key = `${dayIndex}:${actualHour}`;
      map.set(key, Math.max(0, Math.min(100, score)));
    });
  }

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
