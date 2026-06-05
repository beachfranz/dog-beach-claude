// safeSelect — detect PostgREST silent-truncation at the call site.
//
// Problem: Supabase applies `db-max-rows=1000` to every REST and RPC
// response. Queries matching more rows are silently capped to 1000.
// PostgREST DOES signal this via HTTP 206 Partial Content + a
// Content-Range header like `Content-Range: 0-999/73079`, and
// supabase-js exposes it as { count, status } on the response — but
// our edge functions weren't reading those fields. Result: today's
// daily-beach-refresh blackout, 1,889 of 2,889 beaches invisible to
// the function for days.
//
// Fix: every PostgREST .select() against a potentially-large table
// should request `{ count: 'exact' }` and pass the result through
// `ensureNotTruncated()`. The helper compares `data.length` to `count`
// and warns (or throws if `hard: true`) if they differ — making the
// failure loud at the call site instead of silent.
//
// Usage:
//
//   const result = await supabase
//     .from('beach_day_recommendations')
//     .select('arena_group_id', { count: 'exact' })
//     .in('arena_group_id', fids);
//   ensureNotTruncated(result, 'beach-recs by fid');
//   const { data, error } = result;
//
// Or hard-fail:
//   ensureNotTruncated(result, 'beach-recs by fid', { hard: true });
//
// Encoded 2026-06-05 after the 14-day-stuck-Luhr-Jensen incident.
// Related: [[claim-tested-without-end-state-verification]] (don't trust
// upstream silence as success); [[paired-functions-port-fixes-both-sides]]
// (same shape can lurk on dog-park sibling functions).

export type PostgrestResponseShape = {
  data: unknown[] | null;
  error: { message: string; details?: string | null; hint?: string | null; code?: string } | null;
  count: number | null;
  status: number;
  statusText?: string;
};

export function ensureNotTruncated(
  result: PostgrestResponseShape,
  label: string,
  opts?: { hard?: boolean },
): void {
  if (result.error) return;  // caller's responsibility to handle
  if (result.count === null) {
    // `{ count: 'exact' }` was not requested — can't compare.
    console.warn(
      `[safeSelect] ${label}: count not requested; pass {count:'exact'} to .select() to enable truncation detection.`
    );
    return;
  }
  const got = result.data?.length ?? 0;
  if (got < result.count) {
    const msg =
      `[safeSelect] ${label}: PostgREST truncated — got ${got} of ${result.count} rows ` +
      `(db-max-rows=1000). Switch to .range() pagination or call an aggregating RPC. ` +
      `See beaches_due_for_refresh for the canonical RPC pattern.`;
    if (opts?.hard) throw new Error(msg);
    console.warn(msg);
  }
}
