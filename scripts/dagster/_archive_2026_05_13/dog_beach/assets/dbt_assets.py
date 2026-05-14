"""dbt models surfaced as Dagster assets.

dagster-dbt parses the dbt manifest and creates one asset per dbt
model. dbt's source nodes become "external assets" that don't have
materializations from Dagster's POV — they're observed only.

Materializing a Dagster asset that maps to a dbt model invokes
`dbt build --select <model>`, which runs both the model and its
column-level tests. So one click in Dagit = build + test.

The `db_source_specs` below declare explicit AssetSpec entries for
every public.* table dbt reads as a source, with group_name set to
`db_sources`. Sharing the same AssetKey as the dbt source merges
them into a single graph node — same pattern as `public/beaches`.
Sources that ARE owned by another Dagster asset (e.g. public/beaches
in `consumer` group) keep their owning group; we just don't list
them here.

NOTE: no `from __future__ import annotations` here — Dagster's runtime
context-type validator doesn't resolve PEP 563 string annotations.
"""
from dagster import AssetExecutionContext, AssetSpec, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

from ..resources import DBT_PROJECT_DIR


_DBT_PROJECT = DbtProject(project_dir=DBT_PROJECT_DIR)
_DBT_PROJECT.prepare_if_dev()  # generate manifest if missing


@dbt_assets(manifest=_DBT_PROJECT.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Group the public.* dbt source assets together. Excludes tables that
# already have a Dagster asset claiming the key (those keep their
# owning asset's group):
#   - public.beaches                     -> verdicts (STALE — table dropped 2026-05-02)
#   - public.beaches_gold                -> gold (path-3 spine)
#   - public.beach_dog_policy            -> gold (curated overlay)
#   - public.beach_verdicts              -> verdicts (cascade asset)
#   - public.operator_dogs_policy        -> ingest
#   - public.operator_policy_exceptions  -> ingest
#   - public.cpad_unit_dogs_policy       -> ingest
#   - public.cpad_unit_policy_exceptions -> ingest
#   - public.us_beach_points             -> ingest (legacy spine; no longer consumed)
#   - public.cpad_units                  -> ingest (805 spine)
#   - public.osm_features                -> ingest (805 spine)
#   - public.ccc_access_points           -> ingest (805 spine)
# The remaining tables here are sourced upstream of Dagster (manual
# migrations, db views, no Dagster owner).
_db_source_table_names = [
    "counties",
    "operators",
    "truth_external",
]
db_source_specs = [
    AssetSpec(
        key=AssetKey(["public", t]),
        description=f"Database source — public.{t}. Read by dbt staging "
                    f"models; not materialized from Dagster (loaded by "
                    f"upstream pipelines or migration scripts).",
        group_name="db_sources",
        kinds={"sql", "table"},
    )
    for t in _db_source_table_names
]

# public.beach_locations is owned by the Dagster cheap-obs asset in
# the ingest group (see assets/ingest.py). It claims the same AssetKey
# so dagster-dbt's source merges into the executable node.


# Convenience export for assets/__init__.py
dbt_models_assets_list = [dbt_models, *db_source_specs]
