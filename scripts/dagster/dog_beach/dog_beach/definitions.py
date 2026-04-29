"""Dagster Definitions root for the dog_beach project.

Day 1: thin scaffolding. dbt assets and procedural ops get wired in
on Days 3-4.
"""
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions

# Load env vars (Supabase DB creds) the same way the rest of the repo does.
PIPELINE_ENV = Path(__file__).resolve().parents[3] / "pipeline" / ".env"
if PIPELINE_ENV.exists():
    load_dotenv(PIPELINE_ENV)

# Day 3+ : assets will be loaded here.
# from .assets import all_assets
# from .resources import dbt_resource, supabase_resource

defs = Definitions(
    assets=[],            # populated Day 3 (dbt) + Day 4 (procedural)
    resources={
        # placeholder; Day 3 adds the dbt resource and a Postgres / Supabase resource
    },
    schedules=[],
    sensors=[],
)
