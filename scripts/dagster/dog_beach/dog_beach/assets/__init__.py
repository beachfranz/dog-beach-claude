"""Aggregate every asset module into a single all_assets list for the
Definitions root."""
from . import (dbt_assets, ingest, verdicts, consumer_pipeline,
               frontend, external_sources)

all_assets = [
    *dbt_assets.dbt_models_assets_list,
    *ingest.assets,
    *verdicts.assets,
    *consumer_pipeline.assets,
    *frontend.assets,
    *external_sources.assets,
]
