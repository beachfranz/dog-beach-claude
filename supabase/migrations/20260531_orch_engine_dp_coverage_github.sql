-- 20260531_orch_engine_dp_coverage_github.sql
--
-- Wire monthly_dog_park_coverage to the new GitHub Actions runner via the
-- dispatch-github-workflow edge function. The 9-op DP pipeline runs Python
-- with Playwright + Anthropic LLM calls — too heavy for PL/pgSQL or Deno
-- edge. GitHub Actions ubuntu-latest gives ~6h runtime + ~14GB RAM, well
-- in excess of the ~1h MVP+ wall.
--
-- Edge function source: supabase/functions/dispatch-github-workflow/index.ts
-- Workflow definition:   .github/workflows/dp_coverage.yml
--
-- Required env vars on the edge function (Supabase Dashboard → Edge Functions → Secrets):
--   ADMIN_SECRET        — already set
--   GH_DISPATCH_TOKEN   — NEW, GitHub PAT or fine-grained token with
--                         actions:write scope on beachfranz/dog-beach-claude
--
-- Required GitHub repo secrets (Settings → Secrets and variables → Actions):
--   SUPABASE_URL
--   SUPABASE_SERVICE_KEY
--   SUPABASE_DB_PASSWORD
--   ANTHROPIC_API_KEY
--   ADMIN_SECRET
--   GOOGLE_PLACES_API_KEY  (optional, ingest_queue geocoding)

BEGIN;

UPDATE public.orch_jobs
   SET worker_kind   = 'edge_function',
       worker_target = 'dispatch-github-workflow',
       worker_payload = jsonb_build_object(
         'workflow', 'dp_coverage.yml',
         'ref',      'main',
         'inputs',   jsonb_build_object(
           'states',   'CA,OR,WA,MD,UT'
         )
       ),
       description = 'Monthly dog-park coverage refresh (9-op pipeline). Dispatches GitHub Actions workflow dp_coverage.yml via dispatch-github-workflow edge function. Heavy Python with Playwright + Anthropic; not portable to SQL/Deno.',
       updated_at = now()
 WHERE job_name = 'monthly_dog_park_coverage';

COMMIT;
