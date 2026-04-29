"""dbt models surfaced as Dagster assets.

dagster-dbt parses the dbt manifest and creates one asset per dbt
model. dbt's source nodes become "external assets" that don't have
materializations from Dagster's POV — they're observed only.

Materializing a Dagster asset that maps to a dbt model invokes
`dbt build --select <model>`, which runs both the model and its
column-level tests. So one click in Dagit = build + test.

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

from ..resources import DBT_PROJECT_DIR


_DBT_PROJECT = DbtProject(project_dir=DBT_PROJECT_DIR)
_DBT_PROJECT.prepare_if_dev()  # generate manifest if missing


@dbt_assets(manifest=_DBT_PROJECT.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Convenience export for assets/__init__.py
dbt_models_assets_list = [dbt_models]
