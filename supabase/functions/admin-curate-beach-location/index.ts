// admin-curate-beach-location/index.ts
//
// Save a curator's location decision for a beach. Two cases:
//   1. Curator dragged the marker → POST { fid, lat, lng, curated_by }
//      Calls move_beach_gold_point RPC + records the move in
//      beach_location_curation_status (was_moved=true, before/after coords).
//   2. Curator confirmed marker is correct (no move) → POST { fid, no_move: true, curated_by }
//      Records was_moved=false in the status table.
//
// Auth: x-admin-secret + per-IP rate limit (same as admin-move-beach-point).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

function haversineM(la1: number, lo1: number, la2: number, lo2: number): number {
  const r = (d: number) => d * Math.PI / 180;
  const a = Math.sin(r(la2 - la1) / 2) ** 2
          + Math.cos(r(la1)) * Math.cos(r(la2)) * Math.sin(r(lo2 - lo1) / 2) ** 2;
  return Math.round(2 * 6_371_000 * Math.asin(Math.sqrt(a)));
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { fid?: number; lat?: number; lng?: number; no_move?: boolean; curated_by?: string; notes?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { fid, lat, lng, no_move, curated_by, notes } = body;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

  // Pull current coords for before-snapshot
  const { data: beachRows } = await supabase
    .from("beaches_gold").select("fid, name").eq("fid", fid).limit(1);
  if (!beachRows?.length) return json({ error: "Beach not found" }, 404);

  // Pull current geom via RPC since beaches_gold.geom isn't directly queryable
  const { data: info } = await supabase.rpc("get_beach_info", { p_fid: fid });
  const beach = (info as Record<string, unknown>)?.beach as Record<string, number> | undefined;
  const beforeLat = beach?.lat;
  const beforeLng = beach?.lng;

  let wasMoved = false;
  let afterLat = beforeLat;
  let afterLng = beforeLng;
  let distance: number | null = null;

  if (no_move !== true && typeof lat === "number" && typeof lng === "number") {
    // Move the point
    const { error: moveErr } = await supabase.rpc("move_beach_gold_point",
      { p_fid: fid, p_lat: lat, p_lon: lng });
    if (moveErr) return json({ error: `Move failed: ${moveErr.message}` }, 500);
    wasMoved = true;
    afterLat = lat;
    afterLng = lng;
    if (beforeLat != null && beforeLng != null) {
      distance = haversineM(beforeLat, beforeLng, lat, lng);
    }
  }

  // Upsert status row
  const { error: upErr } = await supabase
    .from("beach_location_curation_status")
    .upsert({
      arena_group_id: fid,
      curated_at:     new Date().toISOString(),
      curated_by:     curated_by ?? null,
      was_moved:      wasMoved,
      before_lat:     beforeLat ?? null,
      before_lng:     beforeLng ?? null,
      after_lat:      afterLat ?? null,
      after_lng:      afterLng ?? null,
      distance_moved_m: distance,
      notes:          notes ?? null,
    });
  if (upErr) return json({ error: `Status upsert failed: ${upErr.message}` }, 500);

  await logAdminWrite(supabase, {
    functionName: "admin-curate-beach-location",
    action:       wasMoved ? "update" : "confirm",
    req,
    before:       { lat: beforeLat, lng: beforeLng },
    after:        { lat: afterLat, lng: afterLng, distance_m: distance },
    success:      true,
  });

  return json({ ok: true, fid, was_moved: wasMoved, distance_moved_m: distance });
});
