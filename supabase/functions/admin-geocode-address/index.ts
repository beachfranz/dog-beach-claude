// admin-geocode-address/index.ts
// Forward-geocodes a free-text address/place query via Google Maps,
// returning lat/lng for the admin editor to drop a pin.
//
// Security model: same as other admin-* functions (obscure URL,
// service-role server-side, no auth layer). See admin-update-beach.
//
// POST { address: string }
// Returns { latitude, longitude, formatted_address, location_type }
// On failure: { error, status? }
//
// GOOGLE_MAPS_API_KEY — same key already used by geocode-beaches-staging
// and v2-geocode-context in the pipeline.

import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const GOOGLE_KEY  = Deno.env.get("GOOGLE_MAPS_API_KEY")!;
const GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  if (!GOOGLE_KEY) return json({ error: "GOOGLE_MAPS_API_KEY not configured" }, 500);

  let body: { address?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const address = (body.address ?? "").trim();
  if (!address)            return json({ error: "address required" }, 400);
  if (address.length > 400) return json({ error: "address too long (max 400 chars)" }, 400);

  const url = new URL(GEOCODE_URL);
  url.searchParams.set("address", address);
  url.searchParams.set("key", GOOGLE_KEY);

  let data: {
    status:  string;
    results: Array<{
      geometry:          { location: { lat: number; lng: number }; location_type?: string };
      formatted_address: string;
    }>;
    error_message?: string;
  };
  try {
    const resp = await fetch(url.toString());
    data = await resp.json();
  } catch (err) {
    return json({ error: `Google Maps fetch failed: ${(err as Error).message}` }, 502);
  }

  if (data.status !== "OK") {
    return json({
      error: data.error_message || `Google Maps returned ${data.status}`,
      status: data.status,
    }, data.status === "ZERO_RESULTS" ? 404 : 500);
  }
  if (!data.results?.length) return json({ error: "No results", status: "ZERO_RESULTS" }, 404);

  const hit = data.results[0];
  return json({
    latitude:          hit.geometry.location.lat,
    longitude:         hit.geometry.location.lng,
    formatted_address: hit.formatted_address,
    location_type:     hit.geometry.location_type ?? null,
  });
});
