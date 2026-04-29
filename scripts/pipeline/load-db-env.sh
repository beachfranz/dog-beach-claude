#!/usr/bin/env bash
# Source this script (don't execute it) to load Supabase DB connection env vars.
# Combines: pooler-url cache (host/user/port/db) + .env (password).
#
# Usage:
#   source scripts/pipeline/load-db-env.sh
#   dbt debug --profiles-dir .
#   dagster dev
#
# After sourcing, these are exported:
#   SUPABASE_DB_HOST     from supabase/.temp/pooler-url
#   SUPABASE_DB_PORT     from supabase/.temp/pooler-url
#   SUPABASE_DB_USER     from supabase/.temp/pooler-url
#   SUPABASE_DB_NAME     from supabase/.temp/pooler-url
#   SUPABASE_DB_PASSWORD from scripts/pipeline/.env

# Find repo root (this script is at scripts/pipeline/, repo root is two levels up)
_repo_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." && pwd )"

# Load .env (everything in it, including SUPABASE_DB_PASSWORD)
if [ -f "$_repo_root/scripts/pipeline/.env" ]; then
  set -a
  source "$_repo_root/scripts/pipeline/.env"
  set +a
else
  echo "warning: scripts/pipeline/.env not found" >&2
fi

# Parse host/user/port/db from pooler-url (this file is auto-maintained by `supabase link`)
if [ -f "$_repo_root/supabase/.temp/pooler-url" ]; then
  eval "$(python -c "
import urllib.parse
with open('$_repo_root/supabase/.temp/pooler-url') as f:
    p = urllib.parse.urlparse(f.read().strip())
print(f'export SUPABASE_DB_HOST={p.hostname}')
print(f'export SUPABASE_DB_PORT={p.port}')
print(f'export SUPABASE_DB_USER={p.username}')
print(f'export SUPABASE_DB_NAME={p.path.lstrip(chr(47))}')
")"
else
  echo "warning: supabase/.temp/pooler-url not found — run 'supabase link --project-ref <ref>' first" >&2
fi

unset _repo_root
