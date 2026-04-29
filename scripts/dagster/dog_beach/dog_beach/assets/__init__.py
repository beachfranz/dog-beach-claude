"""Aggregate every asset module into a single all_assets list for the
Definitions root."""
from . import dbt_assets, ingest, verdicts, consumer_pipeline

all_assets = [
    *dbt_assets.dbt_models_assets_list,
    *ingest.assets,
    *verdicts.assets,
    *consumer_pipeline.assets,
]
