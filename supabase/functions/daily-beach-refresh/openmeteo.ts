// openmeteo.ts
// Fetches 7-day hourly weather forecast from Open-Meteo (no API key required).
// Docs: https://open-meteo.com/en/docs

interface Beach {
  location_id: string;
  latitude: number;
  longitude: number;
  timezone: string;
}

const BASE_URL = "https://api.open-meteo.com/v1/forecast";

// WMO weather codes that are immediate NO-GO conditions.
// Thunderstorm (95-99), heavy rain (63-65), freezing rain (66-67), snow (71-77)
export const SEVERE_WMO_CODES = new Set([
  63, 64, 65, 66, 67,
  71, 72, 73, 74, 75, 76, 77,
  95, 96, 97, 98, 99,
]);

// Maps WMO code → summary_weather category used in beach_day_recommendations.
export function wmoToSummaryWeather(
  code: number,
  windSpeed: number,
): "sunny" | "partly_cloudy" | "cloudy" | "foggy" | "rainy" | "windy" {
  if (windSpeed >= 20) return "windy";
  if (code === 0) return "sunny";
  if (code <= 2) return "partly_cloudy";
  if (code === 3) return "cloudy";
  if (code >= 45 && code <= 48) return "foggy";
  if (code >= 51) return "rainy";
  return "partly_cloudy";
}

export interface OpenMeteoHour {
  time: string;                       // ISO local datetime, e.g. "2026-04-08T14:00"
  temperature_2m: number;             // °F
  apparent_temperature: number;       // °F — feels-like (wind chill / heat index)
  precipitation_probability: number;  // %
  precipitation: number;              // mm — actual (past hours) or forecast (future hours)
  weathercode: number;                // WMO code
  windspeed_10m: number;              // mph
  uv_index: number;
  is_day: number;                     // 1 = daylight, 0 = night
}

export interface OpenMeteoDay {
  date: string;      // "YYYY-MM-DD"
  sunrise: string;   // ISO local datetime
  sunset: string;    // ISO local datetime
}

export interface OpenMeteoResult {
  hours: OpenMeteoHour[];
  days: OpenMeteoDay[];
}

export async function fetchWeather(beach: Beach): Promise<OpenMeteoResult> {
  const params = new URLSearchParams({
    latitude:         String(beach.latitude),
    longitude:        String(beach.longitude),
    hourly:           [
      "temperature_2m",
      "apparent_temperature",
      "precipitation_probability",
      "precipitation",
      "weathercode",
      "windspeed_10m",
      "uv_index",
      "is_day",
    ].join(","),
    daily:            "sunrise,sunset",
    temperature_unit: "fahrenheit",
    windspeed_unit:   "mph",
    precipitation_unit: "mm",
    timezone:         beach.timezone,
    past_days:        "3",
    forecast_days:    "7",
  });

  const url = `${BASE_URL}?${params}`;
  const res = await fetchWithRetry(url, 3);

  if (!res.ok) {
    throw new Error(
      `Open-Meteo error ${res.status} for ${beach.location_id}: ${await res.text()}`
    );
  }

  const json = await res.json();

  // Zip the parallel hourly arrays into an array of objects for easier mapping.
  const times: string[]   = json.hourly.time;
  const hours: OpenMeteoHour[] = times.map((time: string, i: number) => ({
    time,
    temperature_2m:             json.hourly.temperature_2m[i],
    apparent_temperature:       json.hourly.apparent_temperature[i],
    precipitation_probability:  json.hourly.precipitation_probability[i],
    precipitation:              json.hourly.precipitation[i] ?? 0,
    weathercode:                json.hourly.weathercode[i],
    windspeed_10m:              json.hourly.windspeed_10m[i],
    uv_index:                   json.hourly.uv_index[i],
    is_day:                     json.hourly.is_day[i],
  }));

  const days: OpenMeteoDay[] = (json.daily.time as string[]).map(
    (date: string, i: number) => ({
      date,
      sunrise: json.daily.sunrise[i],
      sunset:  json.daily.sunset[i],
    })
  );

  return { hours, days };
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
      // Only retry on 5xx, not 4xx (those won't improve with retries).
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
