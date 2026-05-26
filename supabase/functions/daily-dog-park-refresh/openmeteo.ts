// openmeteo.ts (dog-park copy — generic, same shape as daily-beach-refresh/openmeteo.ts)
// Fetches 7-day hourly weather forecast from Open-Meteo.

interface Location {
  id: string;          // dog_park_fid (string for logging)
  latitude: number;
  longitude: number;
  timezone: string;
}

const BASE_URL = "https://api.open-meteo.com/v1/forecast";

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
  time: string;
  temperature_2m: number;
  apparent_temperature: number;
  precipitation_probability: number;
  precipitation: number;
  weathercode: number;
  windspeed_10m: number;
  uv_index: number;
  cloud_cover: number;
  is_day: number;
}

export interface OpenMeteoDay {
  date: string;
  sunrise: string;
  sunset: string;
}

export interface OpenMeteoResult {
  hours: OpenMeteoHour[];
  days: OpenMeteoDay[];
}

export async function fetchWeather(loc: Location): Promise<OpenMeteoResult> {
  const params = new URLSearchParams({
    latitude: String(loc.latitude),
    longitude: String(loc.longitude),
    hourly: [
      "temperature_2m",
      "apparent_temperature",
      "precipitation_probability",
      "precipitation",
      "weathercode",
      "windspeed_10m",
      "uv_index",
      "cloud_cover",
      "is_day",
    ].join(","),
    daily: "sunrise,sunset",
    temperature_unit: "fahrenheit",
    windspeed_unit: "mph",
    precipitation_unit: "mm",
    timezone: loc.timezone,
    past_days: "0",
    forecast_days: "7",
  });

  const res = await fetchWithRetry(`${BASE_URL}?${params}`, 3);
  if (!res.ok) {
    throw new Error(`Open-Meteo error ${res.status} for ${loc.id}: ${await res.text()}`);
  }
  const json = await res.json();
  const times: string[] = json.hourly.time;
  const hours: OpenMeteoHour[] = times.map((time: string, i: number) => ({
    time,
    temperature_2m: json.hourly.temperature_2m[i],
    apparent_temperature: json.hourly.apparent_temperature[i],
    precipitation_probability: json.hourly.precipitation_probability[i],
    precipitation: json.hourly.precipitation[i] ?? 0,
    weathercode: json.hourly.weathercode[i],
    windspeed_10m: json.hourly.windspeed_10m[i],
    uv_index: json.hourly.uv_index[i],
    cloud_cover: json.hourly.cloud_cover[i],
    is_day: json.hourly.is_day[i],
  }));
  const days: OpenMeteoDay[] = (json.daily.time as string[]).map((date: string, i: number) => ({
    date,
    sunrise: json.daily.sunrise[i],
    sunset: json.daily.sunset[i],
  }));
  return { hours, days };
}

async function fetchWithRetry(url: string, attempts: number, delayMs = 1000): Promise<Response> {
  let lastError: Error | null = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await fetch(url);
      if (res.status < 500) return res;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastError = err as Error;
    }
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs * Math.pow(2, i)));
  }
  throw lastError ?? new Error("fetchWithRetry: unknown error");
}
