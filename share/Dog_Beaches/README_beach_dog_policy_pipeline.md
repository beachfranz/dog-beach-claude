# Beach dog-policy enrichment pipeline

This pipeline reads your `US_beaches.csv`, researches dog policy **per beach**, and appends structured columns to a single final CSV.

## What it appends

- `dog_policy`
  - `dog_friendly` = the page explicitly says the beach is dog-friendly or clearly frames it as a dog beach / dog destination
  - `allowed`
  - `restricted` = leash / time / seasonal / designated-area limits
  - `prohibited`
  - `unknown`
- `dog_policy_detail`
- `source`
- `source_url`
- `confidence`
- `matched_text`
- `review_required`
- `evidence_count`
- `conflict_detected`
- `research_notes`
- `reverse_geocode_hint`
- `search_queries`
- `processed_at_epoch`

## Input schema the script already matches

The script is tailored to these columns from your file:

- `WKT`
- `fid`
- `COUNTRY`
- `NAME`
- `ADDR1`
- `ADDR2`
- `ADDR3`
- `ADDR4`
- `ADDR5`
- `CAT_MOD`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements_beach_dog_policy.txt
```

Set one search provider key:

```bash
export SERPAPI_API_KEY="your_key_here"
# or
export TAVILY_API_KEY="your_key_here"
```

## Run

```bash
python beach_dog_policy_pipeline.py \
  --input /absolute/path/US_beaches.csv \
  --output /absolute/path/US_beaches_dog_policy.csv \
  --checkpoint-dir /absolute/path/beach_policy_ckpt \
  --workers 6
```

## Resume after interruption

```bash
python beach_dog_policy_pipeline.py \
  --input /absolute/path/US_beaches.csv \
  --output /absolute/path/US_beaches_dog_policy.csv \
  --checkpoint-dir /absolute/path/beach_policy_ckpt \
  --workers 6 \
  --resume
```

## Safe test run on the first 25 rows

```bash
python beach_dog_policy_pipeline.py \
  --input /absolute/path/US_beaches.csv \
  --output /absolute/path/US_beaches_dog_policy_sample.csv \
  --checkpoint-dir /absolute/path/beach_policy_ckpt_sample \
  --workers 4 \
  --limit 25
```

## Tracking progress

The script writes all of these during execution:

- terminal progress bar
- `partial_results.csv` in the checkpoint directory
- `progress.json` in the checkpoint directory
- `manual_review.csv` in the checkpoint directory

A quick status check while it runs:

```bash
cat /absolute/path/beach_policy_ckpt/progress.json
```

Or watch the partial row count grow:

```bash
python - <<'PY'
import pandas as pd
p = "/absolute/path/beach_policy_ckpt/partial_results.csv"
try:
    df = pd.read_csv(p)
    print(len(df))
except FileNotFoundError:
    print(0)
PY
```

## Notes

- Reverse geocoding is used only as a **search hint**, not as the final policy source.
- The script intentionally creates a `manual_review.csv` because some beaches will have conflicting or weak signals.
- The default classification is conservative. That helps preserve your strict `dog_friendly` rule.


## Querying and ranking behavior

The revised pipeline now uses staged per-beach queries in this order:
1. `official_first`
2. `exact_broad`
3. `geo_disambiguation`
4. `dog_friendly_probe`
5. `restriction_probe`
6. address / coordinate fallbacks

It also writes these QA columns into the output:
- `search_query_used`
- `query_stage_used`
- `search_queries`

Ranking is domain-weighted with preference for official and parks-authority domains, then tourism, local media, aggregators, blogs, and other sites.

Additional official-source biasing is now state-aware: the pipeline ships with a built-in `STATE_ABBR_TO_OFFICIAL_DOMAINS` table covering all coastal states plus broader nationwide state-domain coverage, so Tavily official-first searches can be narrowed toward the most likely government, parks, and wildlife domains for the beach's inferred state.
