// _shared/admin-audit.ts
// Write an append-only audit row for each admin-* mutation. Called from
// admin-create-beach, admin-update-beach, admin-delete-beach after the
// main DB action completes (whether it succeeded or failed).
//
// Failure modes: the audit write itself can fail (DB blip, schema
// mismatch, etc.). We log the error to console but do NOT fail the
// outer request — the user's create/update/delete already happened,
// and we'd rather lose an audit row than double-execute an action.

// deno-lint-ignore-file no-explicit-any

export type AdminAction = "create" | "update" | "delete";

export interface AdminAuditEntry {
  functionName:    string;
  action:          AdminAction;
  req:             Request;
  locationId?:     string | null;
  before?:         Record<string, unknown> | null;
  after?:          Record<string, unknown> | null;
  success:         boolean;
  error?:          string | null;
}

function clientIp(req: Request): string | null {
  const xff = req.headers.get("x-forwarded-for") ?? "";
  return xff.split(",")[0]?.trim() || null;
}

// Compute which keys changed between before and after. Uses JSON stringify
// for deep equality — good enough for the shapes we store (scalars + jsonb
// dog-policy structures), and avoids a dependency.
function diffKeys(
  before: Record<string, unknown> | null | undefined,
  after:  Record<string, unknown> | null | undefined,
): string[] {
  if (!before || !after) return [];
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  const changed: string[] = [];
  for (const k of keys) {
    if (JSON.stringify(before[k]) !== JSON.stringify(after[k])) changed.push(k);
  }
  return changed.sort();
}

export async function logAdminWrite(
  supabase: any,
  entry:    AdminAuditEntry,
): Promise<void> {
  const row = {
    actor_ip:       clientIp(entry.req),
    function_name:  entry.functionName,
    action:         entry.action,
    location_id:    entry.locationId ?? null,
    before:         entry.before ?? null,
    after:          entry.after  ?? null,
    changed_fields: entry.action === "update"
                      ? diffKeys(entry.before, entry.after)
                      : null,
    success:        entry.success,
    error:          entry.error ?? null,
  };
  try {
    const { error } = await supabase.from("admin_audit").insert(row);
    if (error) console.warn(`[admin-audit] insert failed: ${error.message}`);
  } catch (e) {
    console.warn(`[admin-audit] insert threw: ${(e as Error).message}`);
  }
}
