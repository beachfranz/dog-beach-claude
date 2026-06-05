-- Rollback 20260605e_postgrest_cap_audit.sql.
--
-- The SQL audit was a half-measure: it canaried on table SIZE (would
-- fire daily with the same 9 tables, becoming noise) but couldn't
-- detect ACTUAL silent-truncations. The real signal lives at the call
-- site — PostgREST sends HTTP 206 Partial Content + Content-Range
-- header when truncation happens, and supabase-js surfaces `count` +
-- `status` on the response — but our edge functions weren't checking.
--
-- Replaced with _shared/safeSelect.ts which wraps queries and checks
-- count vs data.length per call. Catches the bug at the moment it
-- happens, with the call-site label, instead of just listing "large
-- tables exist."

DROP FUNCTION IF EXISTS public._orch_w_postgrest_cap_audit(jsonb);

DELETE FROM public.orch_jobs WHERE job_name = 'postgrest_cap_audit';
