// noaa.ts
// Fetches 7-day hourly tide height predictions from NOAA CO-OPS.
// Docs: https://api.tidesandcurrents.noaa.gov/api/prod/
// No API key required.

interface Beach {
  location_id: string;
  noaa_station_id: string | null;
}

const BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter";

export interface NOAATideHour {
  // ISO local datetime string: "YYYY-MM-DD HH:MM" (NOAA format)
  time: string;
  // Tide height in feet above MLLW
  height: number;
}

export async function fetchTides(
  beach: Beach,
  startDate: Date,
): Promise<Map<string, number>> {
  if (!beach.noaa_station_id) {
    throw new Error(
      `Beach ${beach.location_id} has no noaa_station_id — cannot fetch tides`
    );
  }

  // NOAA requires YYYYMMDD format.
  const beginDate = formatNoaaDate(startDate);
  const endDate   = formatNoaaDate(addDays(startDate, 6));

  const params = new URLSearchParams({
    station:    beach.noaa_station_id,
    product:    "predictions",
    datum:      "MLLW",
    units:      "english",         // feet
    time_zone:  "lst_ldt",         // local standard/daylight time
    interval:   "h",               // hourly
    format:     "json",
    begin_date: beginDate,
    end_date:   endDate,
  });

  const url = `${BASE_URL}?${params}`;
  const res = await fetchWithRetry(url, 3);

  if (!res.ok) {
    throw new Error(
      `NOAA error ${res.status} for station ${beach.noaa_station_id}: ${await res.text()}`
    );
  }

  const json = await res.json();

  if (json.error) {
    throw new Error(
      `NOAA API error for station ${beach.noaa_station_id}: ${json.error.message}`
    );
  }

  if (!Array.isArray(json.predictions)) {
    throw new Error(
      `NOAA returned no predictions for station ${beach.noaa_station_id}`
    );
  }

  // Build a Map keyed by "YYYY-MM-DD HH" (hour key) → tide height (ft).
  // This key format is used downstream to join with Open-Meteo hourly data.
  const tideMap = new Map<string, number>();

  for (const p of json.predictions as Array<{ t: string; v: string }>) {
    // NOAA returns t as "YYYY-MM-DD HH:MM"
    const hourKey = p.t.slice(0, 13); // "YYYY-MM-DD HH"
    const height  = parseFloat(p.v);
    if (!isNaN(height)) {
      tideMap.set(hourKey, height);
    }
  }

  return tideMap;
}

// ── Date helpers ──────────────────────────────────────────────────────────────

function formatNoaaDate(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function addDays(date: Date, days: number): Date {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
}

// ── Retry helper ──────────────────────────────────────────────────────────────

async function fetchWithRetry(
  url: string,
  attempts: number,
  delayMs = 1000,
): Promise<Response> {
  let lastError: Error | null = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await fetch(url);
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
