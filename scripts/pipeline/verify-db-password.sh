#!/usr/bin/env bash
# Cryptographically verify that scripts/pipeline/.env's SUPABASE_DB_PASSWORD
# matches the stored postgres hash in pg_authid. Prints MATCH or MISMATCH.
#
# This is the diagnostic to run after any "Reset database password" cycle —
# it tells us in <2s whether the .env value is what Supabase actually stored,
# without making auth attempts that depend on pooler propagation timing or
# error-message interpretation.
#
# Reads pg_authid via `supabase db query --linked` (postgres-role connection
# through the management API). Computes SCRAM-SHA-256 stored_key using the
# same salt + iterations Supabase used, and compares.
#
# Usage: bash scripts/pipeline/verify-db-password.sh

set -e
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

# Activate venv if present
if [ -f .venv-pipeline/Scripts/activate ]; then
  source .venv-pipeline/Scripts/activate
fi

# Load .env to get SUPABASE_DB_PASSWORD
set -a; source scripts/pipeline/.env; set +a

if [ -z "${SUPABASE_DB_PASSWORD:-}" ]; then
  echo "ERROR: SUPABASE_DB_PASSWORD not set in scripts/pipeline/.env" >&2
  exit 1
fi

# Pull stored hash via management API
HASH=$(supabase db query --linked "select rolpassword from pg_authid where rolname='postgres'" 2>&1 | python -c "
import sys, re
m = re.search(r'\"rolpassword\":\s*\"([^\"]+)\"', sys.stdin.read())
print(m.group(1) if m else 'NOT_FOUND')
")

if [ "$HASH" = "NOT_FOUND" ]; then
  echo "ERROR: could not read pg_authid (need postgres-role access via supabase CLI)" >&2
  exit 2
fi

# SCRAM verify
python - <<PYEOF
import os, base64, hashlib, hmac
stored = """$HASH"""
pwd = os.environ['SUPABASE_DB_PASSWORD'].encode('utf-8')
prefix, rest = stored.split('\$', 1)
iters_salt, keys = rest.split('\$', 1)
iters_str, salt_b64 = iters_salt.split(':', 1)
stored_key_b64, _ = keys.split(':', 1)
salt = base64.b64decode(salt_b64)
salted = hashlib.pbkdf2_hmac('sha256', pwd, salt, int(iters_str))
client_key = hmac.new(salted, b'Client Key', 'sha256').digest()
computed = base64.b64encode(hashlib.sha256(client_key).digest()).decode()
if computed == stored_key_b64:
    print('MATCH: .env password matches stored postgres hash')
    raise SystemExit(0)
else:
    print('MISMATCH: .env password does NOT match stored postgres hash')
    print('  Reset password in Supabase Dashboard, update .env, re-run this script.')
    raise SystemExit(3)
PYEOF
