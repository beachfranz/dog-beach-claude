// get-dog-parks-find/index.ts
// Single-query dog-park list for find.html. Wraps find_dog_parks RPC.
//
// GET ?date=YYYY-MM-DD[&lat=X&lng=Y][&leash=any|off|on][&scored_only=true|false]
//     [&surface=grass][&fence=true][&water=true][&limit=N]
//
// Returns: { date, is_today, dog_parks: [...] }
//
// Sibling to get-beaches-find. Simpler v1 — no remaining-window
// re-computation. Frontend can call get-dog-park-summary for that.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  try {
    const url = new URL(req.url);
    const nowUtc = new Date();
    const pacificDate = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Los_Angeles",
      year: "numeric", month: "2-digit", day: "2-digit",
    }).format(nowUtc);

    const date = url.searchParams.get("date") ?? pacificDate;
    const leash = url.searchParams.get("leash") ?? "any";
    const latParam = url.searchParams.get("lat");
    const lngParam = url.searchParams.get("lng");
    const lat = latParam ? parseFloat(latParam) : null;
    const lng = lngParam ? parseFloat(lngParam) : null;
    const scoredOnly = url.searchParams.get("scored_only") === "true";
    const surface = url.searchParams.get("surface");
    const fenceParam = url.searchParams.get("fence");
    const waterParam = url.searchParams.get("water");
    const fence = fenceParam === "true" ? true : fenceParam === "false" ? false : null;
    const water = waterParam === "true" ? true : waterParam === "false" ? false : null;

    const MAX_LIMIT = 50;
    const limitParam = url.searchParams.get("limit");
    const limitParsed = limitParam ? parseInt(limitParam, 10) : NaN;
    const limit = Number.isFinite(limitParsed) && limitParsed > 0
      ? Math.min(limitParsed, MAX_LIMIT)
      : null;

    const isToday = date === pacificDate;

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    const { data, error } = await supabase.rpc("find_dog_parks", {
      p_date: date,
      p_lat: lat,
      p_lng: lng,
      p_leash: leash,
      p_limit: limit,
      p_scored_only: scoredOnly,
      p_surface: surface,
      p_fence: fence,
      p_water: water,
    });

    if (error) return json({ error: error.message }, 500);

    return json({
      date,
      is_today: isToday,
      dog_parks: data ?? [],
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
