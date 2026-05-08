// admin-google-places-search/index.ts
//
// Curator-side Google Places searchText helper. POST { name, county, lat?, lng? }
// Builds a text query from the beach name (+ "Beach" suffix if missing) + county,
// runs tiered searches: natural_feature → park → point_of_interest, dedupes by
// place id, returns up to 5 results.
//
// Auth: x-admin-secret + per-IP rate limit. Reuses existing _shared admin pattern.

import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const GMAPS_KEY = Deno.env.get("GOOGLE_MAPS_API_KEY")!;
const SEARCH_URL = "https://places.googleapis.com/v1/places:searchText";
const FIELD_MASK = "places.id,places.displayName,places.formattedAddress,places.location,places.types";

type Place = {
  id?: string;
  displayName?: { text?: string };
  formattedAddress?: string;
  location?: { latitude?: number; longitude?: number };
  types?: string[];
};

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { name?: string; county?: string; lat?: number; lng?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { name, county, lat, lng } = body;
  if (!name || typeof name !== "string") return json({ error: "name (string) required" }, 400);

  // Build query: append "Beach" if not already present; include county for disambiguation.
  const hasBeachWord = /\bbeach\b/i.test(name);
  const beachSuffix = hasBeachWord ? "" : " Beach";
  const countyPart = county ? `, ${county} County` : "";
  const textQuery = `${name}${beachSuffix}${countyPart}, CA`;

  // Optional location bias around the curator's current beach (~50 km circle).
  const bias = (typeof lat === "number" && typeof lng === "number")
    ? { circle: { center: { latitude: lat, longitude: lng }, radius: 50_000.0 } }
    : undefined;

  async function searchTier(includedType: string, maxCount: number): Promise<Place[]> {
    if (maxCount <= 0) return [];
    const reqBody: Record<string, unknown> = {
      textQuery,
      includedType,
      maxResultCount: maxCount,
    };
    if (bias) reqBody.locationBias = bias;
    try {
      const resp = await fetch(SEARCH_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Goog-Api-Key": GMAPS_KEY,
          "X-Goog-FieldMask": FIELD_MASK,
        },
        body: JSON.stringify(reqBody),
      });
      if (!resp.ok) {
        const t = await resp.text();
        console.error(`Places ${includedType} ${resp.status}: ${t.slice(0, 300)}`);
        return [];
      }
      const data = await resp.json();
      return data.places || [];
    } catch (e) {
      console.error(`Places ${includedType} threw: ${(e as Error).message}`);
      return [];
    }
  }

  const seen = new Set<string>();
  const results: Array<Record<string, unknown>> = [];

  // Tiered fan-out: natural_feature first, then park, then point_of_interest.
  // Stop as soon as we hit 5 deduped results.
  for (const tier of ["natural_feature", "park", "point_of_interest"]) {
    if (results.length >= 5) break;
    const places = await searchTier(tier, 5 - results.length);
    for (const p of places) {
      if (results.length >= 5) break;
      const pid = p.id || "";
      if (!pid || seen.has(pid)) continue;
      seen.add(pid);
      results.push({
        id: pid,
        name: p.displayName?.text || "?",
        address: p.formattedAddress || "",
        lat: p.location?.latitude,
        lng: p.location?.longitude,
        types: p.types || [],
        tier,
      });
    }
  }

  return json({ ok: true, query: textQuery, results });
});
