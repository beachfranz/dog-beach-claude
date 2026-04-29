"""Dagster resources — DB connection + dbt CLI handle.

Both pull credentials from the same env that scripts/pipeline/load-db-env.sh
populates (SUPABASE_DB_HOST/PORT/USER/PASSWORD/NAME). Definitions root
auto-loads .env via python-dotenv.
"""
from __future__ import annotations
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator
import psycopg2
from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource


# Repo root: this file lives at
#   <repo>/scripts/dagster/dog_beach/dog_beach/resources.py
# parents[0] = .../dog_beach (python package)  →  parents[4] = repo root.
REPO_ROOT = Path(__file__).resolve().parents[4]
DBT_PROJECT_DIR = REPO_ROOT / "scripts" / "dbt" / "dog_beach"


class SupabaseDbResource(ConfigurableResource):
    """Direct-Postgres resource. Wraps psycopg2 with a context manager.

    Usage:
        with my_db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute('select 1')
    """

    @contextmanager
    def connect(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(
            host=os.environ["SUPABASE_DB_HOST"],
            port=int(os.environ.get("SUPABASE_DB_PORT", "5432")),
            user=os.environ["SUPABASE_DB_USER"],
            password=os.environ["SUPABASE_DB_PASSWORD"],
            dbname=os.environ.get("SUPABASE_DB_NAME", "postgres"),
            sslmode="require",
            connect_timeout=10,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def make_dbt_resource() -> DbtCliResource:
    """Resolve the dbt project so dagster-dbt can invoke it."""
    return DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROJECT_DIR),  # profiles.yml lives in the project
    )
