"""Dagster Definitions root for Dog Beach Scout.

Wires:
  - dbt assets auto-loaded from scripts/dbt/dog_beach
  - Procedural ingest assets (CDPR scrape, operator extract, merge)
  - Verdict cascade asset (recompute_all_dogs_verdicts_by_origin)
  - DB + dbt resources

Env vars:
  Loaded automatically from scripts/pipeline/.env if present.
  SUPABASE_DB_HOST/PORT/USER/NAME also auto-populated from the supabase
  CLI's pooler-url cache so host/user/port/db never need explicit env
  vars — only SUPABASE_DB_PASSWORD has to be in .env.
"""
from __future__ import annotations
import os
import urllib.parse
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions

# Repo root = parents[4] from this file (see resources.py for the math).
_REPO_ROOT = Path(__file__).resolve().parents[4]

# .env lives at <repo>/scripts/pipeline/.env
_ENV_FILE = _REPO_ROOT / "scripts" / "pipeline" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)

# Pooler-url cache lives at <repo>/supabase/.temp/pooler-url
_POOLER_URL_FILE = _REPO_ROOT / "supabase" / ".temp" / "pooler-url"
if _POOLER_URL_FILE.exists() and "SUPABASE_DB_HOST" not in os.environ:
    p = urllib.parse.urlparse(_POOLER_URL_FILE.read_text().strip())
    os.environ.setdefault("SUPABASE_DB_HOST", p.hostname or "")
    os.environ.setdefault("SUPABASE_DB_PORT", str(p.port or 5432))
    os.environ.setdefault("SUPABASE_DB_USER", p.username or "")
    os.environ.setdefault("SUPABASE_DB_NAME", (p.path or "/postgres").lstrip("/"))

# Imports below depend on env being populated above
from .assets import all_assets
from .resources import SupabaseDbResource, make_dbt_resource


defs = Definitions(
    assets=all_assets,
    resources={
        "supabase_db": SupabaseDbResource(),
        "dbt":         make_dbt_resource(),
    },
)
