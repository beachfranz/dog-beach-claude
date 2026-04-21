// v2-state-operator-override/index.ts
// Pipeline stage 6b — applies curated operating-authority overrides for state
// parks that are operationally managed by a city or county under contract.
//
// Source table: state_park_operators
//
// For each beach where governing_body_source = 'state_polygon' and the
// governing_body (i.e. the state park unit name) appears in state_park_operators,
// override the jurisdiction and body to the operating authority. Records the
// original state park in governing_body_notes for traceability.
//
// Runs AFTER v2-state-classify, BEFORE v2-ccc-crossref.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: overrides, error: oErr } = await supabase
    .from("state_park_operators")
    .select("state_park_name, operator_jurisdiction, operator_body, notes");
  if (oErr) return json({ error: oErr.message }, 500);
  if (!overrides?.length) return json({ processed: 0, updated: 0, note: "no overrides defined" });

  const byName = new Map<string, { juris: string; body: string; notes: string | null }>();
  for (const o of overrides) {
    byName.set(o.state_park_name, {
      juris: o.operator_jurisdiction,
      body:  o.operator_body,
      notes: o.notes,
    });
  }

  const { data: rows, error: fErr } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_body, governing_jurisdiction")
    .eq("governing_body_source", "state_polygon");
  if (fErr) return json({ error: fErr.message }, 500);
  if (!rows?.length) return json({ processed: 0, updated: 0 });

  const matches = rows.filter(r => byName.has(r.governing_body));

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      preview:   matches.map(m => {
        const o = byName.get(m.governing_body)!;
        return {
          id:              m.id,
          display_name:    m.display_name,
          was_jurisdiction: m.governing_jurisdiction,
          was_body:        m.governing_body,
          new_jurisdiction: o.juris,
          new_body:        o.body,
        };
      }),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const o = byName.get(m.governing_body)!;
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: o.juris,
        governing_body:         o.body,
        governing_body_source:  "state_operator_override",
        governing_body_notes:   `State-owned ${m.governing_body}, operationally managed by ${o.body}. ${o.notes ?? ""}`.trim(),
        review_notes:           `Jurisdiction routed to operating authority per state_park_operators mapping.`,
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({
    processed: rows.length,
    matched:   matches.length,
    updated,
    errors:    writeErrors,
  });
});
