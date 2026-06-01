// admin-orch-status/index.ts
//
// Returns the data the cron-audit dashboard needs:
//   - jobs        : per-row orch_jobs state (cadence, shadow, last_*)
//   - runs        : non-skipped orch_runs in the last N hours
//   - summary     : counts by status in the same window
//
// GET (or POST) ?hours=24
// Security: x-admin-secret + per-IP rate limit.

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = corsHeaders(req, "GET, POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

  const fail = await requireAdmin(req, headers);
  if (fail) return fail;

  const url = new URL(req.url);
  let hours = parseInt(url.searchParams.get("hours") ?? "24", 10);
  if (!Number.isFinite(hours) || hours < 1) hours = 24;
  if (hours > 168) hours = 168;  // cap at 7 days

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const sinceIso = new Date(Date.now() - hours * 3600 * 1000).toISOString();

  const [jobsRes, runsRes] = await Promise.all([
    supabase
      .from("orch_jobs")
      .select(
        "job_name, description, shadow_mode, enabled, " +
        "cadence_kind, cadence_param, cron_expr, depends_on, " +
        "worker_kind, worker_target, " +
        "last_attempted_at, last_succeeded_at, last_failed_at, " +
        "last_error, last_skip_reason, is_running, running_since"
      )
      .order("job_name"),
    supabase
      .from("orch_runs")
      .select(
        "id, job_name, intended_fire_at, started_at, ended_at, " +
        "status, skip_reason, error_msg, duration_ms, worker_request_id"
      )
      .gte("started_at", sinceIso)
      .not("status", "like", "skipped_%")
      .order("started_at", { ascending: false })
      .limit(500),
  ]);

  if (jobsRes.error || runsRes.error) {
    return new Response(
      JSON.stringify({
        error: "query_failed",
        jobs_err: jobsRes.error?.message,
        runs_err: runsRes.error?.message,
      }),
      { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  // Summary counts via a separate aggregate query (includes skips)
  const { data: summaryRows, error: summaryErr } = await supabase
    .rpc("_orch_summary_last_hours", { p_hours: hours });

  // Best-effort: if the RPC isn't created yet, compute summary inline from runs
  let summary: Record<string, number> = {};
  if (summaryErr || !summaryRows) {
    for (const r of (runsRes.data ?? [])) {
      summary[r.status] = (summary[r.status] ?? 0) + 1;
    }
  } else {
    for (const r of summaryRows) {
      summary[r.status] = Number(r.n);
    }
  }

  return new Response(
    JSON.stringify({
      window_hours: hours,
      generated_at: new Date().toISOString(),
      summary,
      jobs: jobsRes.data ?? [],
      runs: runsRes.data ?? [],
    }),
    { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
  );
});
