#!/usr/bin/env bash
#
# Canonical wrapper for `supabase functions deploy`.
#
# Why this exists: --no-verify-jwt is mandatory for every function in
# this project. The sb_publishable_ anon key (used by the browser AND
# by cron's net.http_post call) is NOT a JWT — Supabase JWT validation
# rejects it with UNAUTHORIZED_INVALID_JWT_FORMAT. A function deployed
# without --no-verify-jwt returns 401 on every call until redeployed.
#
# That blind spot took out daily-beach-refresh for 4 days (2026-05-30 →
# 2026-06-03) — the cron fired every 2 minutes and got 401 silently
# because orch_runs only tracked dispatch, not the HTTP response. Per
# CLAUDE.md:
#
#   "--no-verify-jwt is required because the sb_publishable_ anon key
#    format does not pass Supabase JWT verification."
#
# Always use this wrapper instead of `supabase functions deploy`.
#
# Usage:
#   scripts/deploy_edge_function.sh <function-name> [<function-name> ...]
#   scripts/deploy_edge_function.sh --all
#   scripts/deploy_edge_function.sh --cron       # deploys cron-targeted only

set -euo pipefail

PROJECT_REF="ehlzbwtrsxaaukurekau"
SUPABASE_BIN="${SUPABASE_BIN:-$HOME/scoop/shims/supabase}"

# Cron-targeted edge functions — kept here for `--cron` mode. Source of
# truth is orch_jobs.worker_target (+ the hardcoded URLs in
# _fire_daily_*_refresh SQL functions). Update both when adding a new
# cron-targeted function.
CRON_FUNCTIONS=(
  daily-beach-refresh
  daily-dog-park-refresh
  get-beach-now
  get-dog-park-now
  refresh-weather-grid
  dispatch-github-workflow
  send-daily-alerts
)

usage() {
  cat <<EOF
Deploy Supabase edge functions with --no-verify-jwt (mandatory for this project).

Usage:
  $0 <function-name> [<function-name> ...]   # deploy named functions
  $0 --all                                   # deploy every function under supabase/functions/
  $0 --cron                                  # deploy only cron-targeted functions
  $0 --help

Environment:
  SUPABASE_BIN   path to supabase CLI (default: ~/scoop/shims/supabase)
EOF
}

deploy_one() {
  local fn="$1"
  echo "→ deploying ${fn} (--no-verify-jwt)"
  "${SUPABASE_BIN}" functions deploy "${fn}" \
    --no-verify-jwt --project-ref "${PROJECT_REF}"
}

case "${1:-}" in
  ""|--help|-h)
    usage; exit 0 ;;
  --all)
    cd "$(dirname "$0")/.."
    for d in supabase/functions/*/; do
      fn="$(basename "$d")"
      [[ "$fn" == "_shared" ]] && continue
      deploy_one "$fn"
    done
    ;;
  --cron)
    for fn in "${CRON_FUNCTIONS[@]}"; do
      deploy_one "$fn"
    done
    ;;
  *)
    for fn in "$@"; do
      deploy_one "$fn"
    done
    ;;
esac

echo "done."
