// get-beach-summary/index.ts
// Serves 7-day beach recommendations for the summary screen.
// 
// GET /functions/v1/get-beach-summary?location_id=huntington-dog-beach
// Returns: { beach, days: DayRecommendation[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const corsHeaders = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Content-Type": "application/json",
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const url        = new URL(req.url);
    const locationId = url.searchParams.get("location_id") ?? "huntington-dog-beach";

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const today    = new Date().toISOString().slice(0, 10);

    // Fetch beach metadata
    const { data: beach, error: beachErr } = await supabase
      .from("beaches")
      .select("location_id, display_name, timezone, address, website")
      .eq("location_id", locationId)
      .single();

    if (beachErr || !beach) {
      return json({ error: "Beach not found" }, 404);
    }

    // Fetch 7 days of recommendations starting today
    const { data: days, error: daysErr } = await supabase
      .from("beach_day_recommendations")
      .select("*")
      .eq("location_id", locationId)
      .gte("local_date", today)
      .order("local_date", { ascending: true })
      .limit(7);

    if (daysErr) {
      return json({ error: daysErr.message }, 500);
    }

    // Fetch all active beaches for location switcher
    const { data: allBeaches } = await supabase
      .from("beaches")
      .select("location_id, display_name")
      .eq("is_active", true)
      .order("display_name");

    return json({ beach, days: days ?? [], allBeaches: allBeaches ?? [] });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: corsHeaders });
}
