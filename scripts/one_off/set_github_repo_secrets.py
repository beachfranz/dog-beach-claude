"""Set GitHub repo Actions secrets from .env.

Used 2026-06-01 to bootstrap the GH Actions runner for the
monthly_dog_park_coverage orch_jobs row. Reads scripts/pipeline/.env
and PUSHes the 5 required secrets to GitHub via the REST API with
libsodium sealed-box encryption per GitHub's spec.

Usage:
  python scripts/one_off/set_github_repo_secrets.py \
    --pat github_pat_xxx
"""
from __future__ import annotations
import argparse, os, sys, base64, json
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from dotenv import dotenv_values
from nacl import encoding, public
import urllib.request, urllib.error

ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = ROOT / "scripts" / "pipeline" / ".env"

OWNER = "beachfranz"
REPO  = "dog-beach-claude"

SECRETS_TO_SET = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
    "SUPABASE_DB_PASSWORD",
    "ANTHROPIC_API_KEY",
    "ADMIN_SECRET",
]


def gh_request(method: str, path: str, pat: str, body: dict | None = None):
    url = f"https://api.github.com{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {pat}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "dog-beach-orch-bootstrap",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def encrypt(public_key_b64: str, value: str) -> str:
    pk = public.PublicKey(public_key_b64.encode(), encoding.Base64Encoder())
    sealed = public.SealedBox(pk)
    return base64.b64encode(sealed.encrypt(value.encode())).decode()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pat", required=True)
    args = ap.parse_args()

    env = dotenv_values(ENV_PATH)
    print(f"Loaded {len(env)} env vars from {ENV_PATH}")

    # 1. Get repo public key
    status, body = gh_request(
        "GET", f"/repos/{OWNER}/{REPO}/actions/secrets/public-key", args.pat
    )
    if status != 200:
        print(f"FAIL get-public-key: {status} {body[:200]}")
        return 1
    pk_info = json.loads(body)
    key_id   = pk_info["key_id"]
    pub_key  = pk_info["key"]
    print(f"Repo public key key_id={key_id}")

    # 2. Set each secret
    for name in SECRETS_TO_SET:
        val = env.get(name)
        if not val:
            print(f"  SKIP {name}: not in .env")
            continue
        enc = encrypt(pub_key, val)
        status, body = gh_request(
            "PUT",
            f"/repos/{OWNER}/{REPO}/actions/secrets/{name}",
            args.pat,
            {"encrypted_value": enc, "key_id": key_id},
        )
        if status in (201, 204):
            print(f"  OK   {name}: status={status}")
        else:
            print(f"  FAIL {name}: status={status} body={body[:200]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
