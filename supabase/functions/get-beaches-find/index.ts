// get-beaches-find/index.ts
// Single-query beach list for find.html day-pill views.
//
// Backed by the find_beaches PostgreSQL RPC. The RPC does the full join
// (beaches_gold + beach_dog_policy + beach_day_recommendations), the
// distance KNN sort, the best-remaining-window detection, and the
// server-side LIMIT — so this fn is a thin passthrough with no follow-up
// .in() that could hit the PostgREST 1000-row cap.
//
// See supabase/migrations/20260606r_find_beaches_aggregating_rpc.sql for the
// SQL-native rewrite (the "option 3" pattern from yesterday's silent-cap
// triage).
//
// GET ?date=YYYY-MM-DD[&lat=33.6&lng=-117.9][&leash=any|off_leash|on_leash|mixed][&scored=true|false][&limit=N]
//
// Returns: { date, is_today: boolean, beaches: BeachRow[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { ensureNotTruncated } from "../_shared/safeSelect.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  try {
    const url    = new URL(req.url);
    const nowUtc = new Date();

    const pacificDate = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Los_Angeles",
      year: "numeric", month: "2-digit", day: "2-digit",
    }).format(nowUtc);

    const date  = url.searchParams.get("date")  ?? pacificDate;
    const leash = url.searchParams.get("leash") ?? "any";
    const latParam = url.searchParams.get("lat");
    const lngParam = url.searchParams.get("lng");
    const lat   = latParam ? parseFloat(latParam) : null;
    const lng   = lngParam ? parseFloat(lngParam) : null;
    const scoredParam = url.searchParams.get("scored");
    const scoredOnly  = scoredParam === "true";

    // Spatial-KNN result cap. When the client passes ?limit= it's honored
    // up to MAX_LIMIT. Without an explicit limit, the RPC's default of 500
    // applies (set in the SQL function signature).
    const MAX_LIMIT   = 50;
    const limitParam  = url.searchParams.get("limit");
    const limitParsed = limitParam ? parseInt(limitParam, 10) : NaN;
    const limit       = Number.isFinite(limitParsed) && limitParsed > 0
      ? Math.min(limitParsed, MAX_LIMIT)
      : null;

    const isToday = date === pacificDate;

    // Pacific current hour — fed into the RPC so the best-remaining-window
    // detection only considers hours from now onward. Null for non-today.
    const nowHour = isToday
      ? parseInt(
          new Intl.DateTimeFormat("en-US", {
            timeZone: "America/Los_Angeles",
            hour: "2-digit", hour12: false,
          }).formatToParts(nowUtc).find(p => p.type === "hour")?.value ?? "0"
        ) % 24
      : null;

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    // ── find_beaches RPC: full join + remaining-window + ORDER BY + LIMIT
    // The RPC enforces a server-side LIMIT (500 default, or p_limit when
    // present) so the response can never silently exceed PostgREST's
    // db-max-rows=1000 cap. ensureNotTruncated remains as a belt-and-suspenders
    // tripwire in case someone removes the SQL-side limit.
    const rpcResult = await supabase.rpc("find_beaches", {
      p_date:         date,
      p_lat:          lat,
      p_lng:          lng,
      p_leash:        leash,
      p_limit:        limit,
      p_scored_only:  scoredOnly,
      p_now_hour:     nowHour,
    }, { count: "exact" });
    ensureNotTruncated(rpcResult as never, "find_beaches RPC");
    const { data: beaches, error: rpcErr } = rpcResult;

    if (rpcErr || !beaches) return json({ error: rpcErr?.message ?? "RPC failed" }, 500);

    return json({ date, is_today: isToday, beaches });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
