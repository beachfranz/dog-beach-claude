// admin-save-truth-polygon/index.ts
// Save (or replace) a hand-drawn ground-truth beach polygon for one fid.
// Upserts into public.beach_polygons_truth — one truth row per fid, so
// re-drawing the same beach replaces the previous polygon.
//
// Body: { fid: number, geom: GeoJSON Polygon|MultiPolygon, drawn_by?, notes? }

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { fid?: number; geom?: unknown; drawn_by?: string; notes?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }
  const { fid, geom } = body;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);
  if (!geom)                   return json({ error: "geom (GeoJSON) required" }, 400);

  // Validate it's a Polygon/MultiPolygon
  const g = geom as { type?: string };
  if (g.type !== "Polygon" && g.type !== "MultiPolygon") {
    return json({ error: `geom.type must be Polygon or MultiPolygon, got ${g.type}` }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Read prior state (for audit)
  const { data: before } = await supabase
    .from("beach_polygons_truth_geojson")
    .select("id, fid, area_m2, drawn_by, drawn_at, notes")
    .eq("fid", fid)
    .maybeSingle();

  // Upsert via RPC: easiest way to convert GeoJSON → geometry server-side.
  // We use a one-shot SQL via the rest layer.
  const sql = `
    insert into public.beach_polygons_truth (fid, geom, drawn_by, notes, source)
    values (
      $1,
      st_setsrid(st_geomfromgeojson($2), 4326),
      coalesce($3, 'curator'),
      $4,
      'admin_ui'
    )
    on conflict (fid) do update set
      geom       = excluded.geom,
      drawn_by   = excluded.drawn_by,
      notes      = excluded.notes,
      drawn_at   = now(),
      source     = 'admin_ui'
    returning id, fid, area_m2, drawn_at, drawn_by, notes
  `;
  const { data: upserted, error } = await supabase.rpc("exec_truth_polygon_upsert", {
    p_fid: fid,
    p_geom_json: JSON.stringify(geom),
    p_drawn_by: body.drawn_by ?? null,
    p_notes:    body.notes    ?? null,
  });

  if (error) return json({ error: error.message, hint: "if RPC missing, see migration 20260506_save_truth_polygon_rpc.sql" }, 500);

  await logAdminWrite(supabase, {
    functionName: "admin-save-truth-polygon",
    action:       before ? "update" : "insert",
    req,
    before:       before ?? null,
    after:        { ...(upserted ?? {})[0], __resolution_mode: "truth_drawn" },
    success:      true,
  });

  return json({ ok: true, ...(upserted ?? [])[0] });
});
