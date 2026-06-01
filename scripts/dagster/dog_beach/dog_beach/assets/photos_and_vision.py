"""Photo enrichment + vision tagging + ML scoring.

Closes the lineage gaps Franz called out 2026-05-13: photo loaders
beyond Wikimedia were declared as resources but had no asset wired,
and the two-pass Haiku vision tagger + the logreg keep_prob model
weren't in Dagster at all.

Layout:
  photos_<source>(state)          — per-state photo loaders
  photo_vision_tags(source)       — Haiku two-pass tagger, per-source partition
  photo_keep_prob_model()         — non-partitioned: train + write predictions

The photo loaders mirror photos_wikimedia's chunked-fanout pattern:
  - Per-state partition
  - Pulls tier-1+2 fids for the state via the same _tier12_fids helper
  - Chunked subprocess to the load_*_photos.py script
  - Counts photos saved by parsing the script's stdout

Vision tagging is per-source-partitioned (not per-state) because the
script's filter is `source`, not `state` — and budget control is more
naturally per-source (Flickr is cheap, Wikimedia ran $9.76 / 2,144 photos).
A second-pass call against an existing source is the common operational
mode (e.g. retag after a prompt change).

ML training is non-partitioned. One model, one prediction set, refreshed
when curator labels grow enough to move the needle.
"""

import re

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
)

from ..partitions import state_partitions
from ..resources import PostgresPoolerResource, SubprocessResource
# DP photo loaders wait for coverage classification (dp_triage_needs_review is
# the last DP coverage asset) so we only photo parks with settled is_active /
# is_scoreable flags.
from .dog_park_coverage import dp_triage_needs_review


# Per-source partition def for vision tagging. Mirrors the photo loader
# inventory; if you add another loader, add it here too. Pixabay + pexels
# dropped 2026-05-26 (replaced by websearch — generic stock didn't pay off).
vision_source_partitions = StaticPartitionsDefinition(
    ["wikimedia", "flickr", "ccc", "websearch"]
)


def _tier12_fids(postgres: PostgresPoolerResource, state: str) -> list[int]:
    """Same gate as per_fid_enrichment._tier12_fids — kept duplicated to
    avoid cross-module imports for one helper. If a third file needs it,
    promote to a shared helpers module."""
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT array_agg(distinct g.fid) "
                "  FROM public.beaches_gold g "
                " WHERE g.state=%s AND g.is_active AND g.scoring_tier IN ('daily','hourly')",
                (state,),
            )
            return cur.fetchone()[0] or []
    finally:
        conn.close()


def _run_chunked(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
    script_path: str,
    fids: list[int],
    chunk_size: int,
    per_chunk_timeout: int,
    parse_pattern: str,
    extra_args: list[str] | None = None,
) -> tuple[int, int, int]:
    """Returns (total_parsed_value, chunks_done, chunks_failed)."""
    total = 0
    done = 0
    failed = 0
    extra = list(extra_args or [])
    for i in range(0, len(fids), chunk_size):
        chunk = fids[i:i + chunk_size]
        result = subproc.run(
            script_path,
            args=extra + ["--fids", ",".join(str(x) for x in chunk)],
            timeout=per_chunk_timeout,
        )
        if result.returncode != 0:
            failed += 1
            context.log.warning(
                f"Chunk {done+1} failed (fids {chunk[0]}..{chunk[-1]}): "
                f"{result.stderr[-300:]}"
            )
            continue
        m = re.search(parse_pattern, result.stdout)
        if m:
            total += int(m.group(1))
        done += 1
    return total, done, failed


# ════════════════════════════════════════════════════════════════════════
#  Photo loaders — one per source, per-state partitioned
# ════════════════════════════════════════════════════════════════════════

def _make_photo_loader_asset(
    *,
    name: str,
    script: str,
    chunk_size: int,
    timeout: int,
    parse_pattern: str,
    description: str,
    entity: str | None = None,
    workers: int | None = None,
):
    """Factory for the photo-loader-per-source asset. All loaders share
    the same shape (per-state, chunked subprocess, parse "N saved").

    entity / workers: optional pass-through args for entity-aware loaders
    (Franz 2026-05-26 — websearch loader defaults --entity dog_park; for
    beach use we pass entity='beach' explicitly. workers used by
    parallelized loaders like websearch/flickr)."""
    @asset(
        name=name,
        partitions_def=state_partitions,
        group_name="phase_31_photos",
        description=description,
    )
    def _asset(
        context: AssetExecutionContext,
        postgres: PostgresPoolerResource,
        subproc: SubprocessResource,
    ) -> MaterializeResult:
        state = context.partition_key
        fids = _tier12_fids(postgres, state)
        if not fids:
            return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})
        extra_args = []
        if entity:
            extra_args += ["--entity", entity]
        if workers and workers > 1:
            extra_args += ["--workers", str(workers)]
        total, done, failed = _run_chunked(
            context, subproc, script, fids,
            chunk_size=chunk_size, per_chunk_timeout=timeout,
            parse_pattern=parse_pattern,
            extra_args=extra_args,
        )
        return MaterializeResult(metadata={
            "state":         MetadataValue.text(state),
            "fids_total":    MetadataValue.int(len(fids)),
            "photos_saved":  MetadataValue.int(total),
            "chunks_done":   MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        })
    return _asset


photos_flickr = _make_photo_loader_asset(
    name="photos_flickr",
    script="scripts/load_flickr_photos.py",
    chunk_size=50, timeout=600,
    parse_pattern=r"saved=(\d+)",
    description=(
        "Flickr Commons photo loader. CC-licensed only (skips All Rights "
        "Reserved). Spatial search by beach lat/lng + radius. Variable "
        "coverage — urban beaches yield more."
    ),
)

# Web image search (Tavily). Per Franz 2026-05-26 — replaces
# Pixabay + Pexels for beaches (stock sources produced generic stock
# photos rarely tied to the specific beach; websearch + Tavily image
# descriptions are dramatically more relevant). Same loader the
# dog-park pipeline uses; --entity beach swaps the FK col + photo
# table + query keyword. workers=8 because Tavily handles parallel
# search well.
photos_websearch = _make_photo_loader_asset(
    name="photos_websearch",
    script="scripts/load_websearch_photos.py",
    chunk_size=30, timeout=900,
    parse_pattern=r"saved=(\d+)",
    entity="beach",
    workers=8,
    description=(
        "Beach web-image-search loader (Tavily). Embeds third-party-hosted "
        "image URLs with link-back attribution; vision tagger + photos_curate "
        "filter for relevance + quality before consumer surface."
    ),
)

# photos_unsplash REMOVED 2026-05-26 — generic stock photos returned
# the same image for multiple distinct parks (Maryland City Dog Park vs
# Ma & Pa Dog Park 64 km apart both got photo-1709790970068-adf7a460659d).
# Same failure mode as pixabay+pexels earlier. Script kept on disk; no
# longer wired into any Dagster asset/job.

photos_ccc = _make_photo_loader_asset(
    name="photos_ccc",
    script="scripts/load_ccc_photos.py",
    chunk_size=80, timeout=600,
    parse_pattern=r"saved=(\d+)",
    description=(
        "California Coastal Commission photo loader. CA-only by source "
        "scope — non-CA partitions will find 0 matches and skip."
    ),
)


# ════════════════════════════════════════════════════════════════════════
#  Phase 31b — vision tagging (Haiku two-pass), per-source partition
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=vision_source_partitions,
    group_name="phase_31_photos",
    description=(
        "Two-pass Haiku vision tagger for beach_photos.source_meta.vision. "
        "Writes has_dog / has_birds / has_surfing / has_active_people / "
        "has_human_face_closeup / scene / atmosphere / quality_issue / etc. "
        "Skips photos already tagged unless --no-skip-existing. Budget gate "
        "is per-source via --budget-usd (default $20). Pricing reference: "
        "Wikimedia ran ~$9.76 / 2,144 photos. Chunk-size 50/call (Haiku 4.5)."
    ),
)
def photo_vision_tags(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    source = context.partition_key
    result = subproc.run(
        "scripts/load_photo_vision_tags.py",
        args=["--source", source, "--chunk-size", "50", "--workers", "1"],
        timeout=14400,  # 4h — Wikimedia ran ~3h
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"photo_vision_tags failed for source={source}: "
            f"{result.stderr[-500:]}"
        )
    # Parse the "ok=N err=N" + "est cost=$X" footer
    ok_match  = re.search(r"ok=(\d+)\s+err=(\d+)", result.stdout)
    cost_match = re.search(r"est cost=\$([\d.]+)", result.stdout)
    ok_n  = int(ok_match.group(1))  if ok_match  else 0
    err_n = int(ok_match.group(2))  if ok_match  else 0
    cost  = float(cost_match.group(1)) if cost_match else 0.0
    return MaterializeResult(metadata={
        "source":     MetadataValue.text(source),
        "photos_ok":  MetadataValue.int(ok_n),
        "photos_err": MetadataValue.int(err_n),
        "est_cost_usd": MetadataValue.float(round(cost, 2)),
    })


# ════════════════════════════════════════════════════════════════════════
#  Phase 31c — keep_prob model (logreg) training + prediction write
# ════════════════════════════════════════════════════════════════════════

@asset(
    deps=[photo_vision_tags],
    group_name="phase_31_photos",
    description=(
        "Trains the logreg classifier on curator labels + vision features, "
        "then writes predicted_keep_prob to beach_photos.source_meta for "
        "EVERY photo in the table. Output is consumed by curate.html (Tier 2 "
        "ML badge) and get_beach_photos_diverse (eligibility floor 0.65). "
        "Also applies the apply_vision_rules layer (has_dog=0.85 floor, "
        "has_human_face_closeup=0.05 hard reject, screenshot/interior=0.10). "
        "Non-partitioned — one model, one prediction set."
    ),
)
def photo_keep_prob_model(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    result = subproc.run(
        "scripts/train_photo_model.py",
        args=[],
        timeout=1800,  # 30 min — model fits in <2 min, prediction write is the long part
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"photo_keep_prob_model failed: {result.stderr[-500:]}"
        )
    # Parse "Writing N predictions" and "vision rules applied: X/Y"
    write_match  = re.search(r"Writing (\d+) predictions", result.stdout)
    rules_match  = re.search(r"vision rules applied:\s+(\d+)/(\d+)", result.stdout)
    written = int(write_match.group(1)) if write_match else 0
    rules_changed = int(rules_match.group(1)) if rules_match else 0
    rules_total   = int(rules_match.group(2)) if rules_match else 0
    return MaterializeResult(metadata={
        "predictions_written": MetadataValue.int(written),
        "vision_rules_changed": MetadataValue.int(rules_changed),
        "vision_rules_total":   MetadataValue.int(rules_total),
    })


# ════════════════════════════════════════════════════════════════════════
#  State photo galleries (state-parks photo loaders + auxiliaries)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    group_name="phase_31_photos",
    description=(
        "Per-state state-parks photo loaders (CDPR/OPRD/WSPRC/MD-DNR/"
        "DNREC-DE/NHSP per scripts/state_park_urls.json) + NPS gallery "
        "fallback. Routes via run_state_pipeline.action_state_photo_galleries "
        "which fans out to the registry-driven per-state loader."
    ),
)
def state_photo_galleries(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Call run_state_pipeline.action_state_photo_galleries directly
    (no subprocess) — it's a Python action that internally fans out to
    per-source subprocess calls for each registry entry. Calling it
    in-process is simpler + cheaper than shell-out + lets the action
    inherit our Dagster log context.
    """
    import sys
    from pathlib import Path
    state = context.partition_key
    # Make scripts/ importable. Dagster's cwd is the dagster project dir,
    # so we walk up to repo root.
    repo_root = Path(__file__).resolve().parents[5]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.run_state_pipeline import action_state_photo_galleries
    rc = action_state_photo_galleries(state)
    if rc < 0:
        raise RuntimeError(
            f"state_photo_galleries action returned negative ({rc}) for {state}"
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "action_return_code": MetadataValue.int(rc),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[state_photo_galleries],
    group_name="phase_31_photos",
    description=(
        "Backfill beach_photos.distance_m using polygon centroid for fanout "
        "URLs that haven't been per-fid attributed yet. Per-source pass."
    ),
)
def photo_centroid_backfill(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    # Script defaults to 16 workers, but Supabase pooler session-mode cap
    # is 15. 16 workers from one script = guaranteed pool exhaustion before
    # any other Dagster asset can connect. Lower to 6 to leave headroom for
    # parallel work (~9 connections for the pool's other tenants). Tuned
    # 2026-05-23 EVENING after NH state_launch_job hit EMAXCONNSESSION even
    # with the connect() retry in scripts/common/db.py — retries can't help
    # when one script demands more than the pool's capacity.
    result = subproc.run(
        "scripts/backfill_agency_photo_centroid.py",
        args=["--state", state, "--apply", "--workers", "6"],
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"photo_centroid_backfill failed for {state}: "
            f"{(result.stderr or '')[-500:]}"
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "log_tail": MetadataValue.text((result.stdout or "")[-2000:]),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[photo_keep_prob_model, photo_centroid_backfill],
    group_name="phase_31_photos",
    description=(
        "Final per-beach photo selection. Reads predicted_keep_prob + vision "
        "tags + per-source diversity rules + distance_m and picks the "
        "best-N photos for each beach's consumer surface."
    ),
)
def photos_curate(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    # Real script is scripts/auto_curate.py — earlier code referenced a
    # curate_beach_photos.py that doesn't exist. Includes --skip-if-fresh-within
    # 7 freshness guard (matches the runner's pattern).
    result = subproc.run(
        "scripts/auto_curate.py",
        args=["--state", state, "--skip-if-fresh-within", "7"],
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"photos_curate failed for {state}: "
            f"{(result.stderr or '')[-500:]}"
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "log_tail": MetadataValue.text((result.stdout or "")[-2000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Dog-park photo pipeline (Franz 2026-05-26)
#  Mirrors the beach pipeline but reads dog_parks_gold + writes
#  dog_park_photos. Same scripts, --entity dog_park dispatcher.
# ════════════════════════════════════════════════════════════════════════

def _tier_fids_dog_park(postgres: PostgresPoolerResource, state: str) -> list[int]:
    """Active+scoreable dog parks per state. Equivalent of _tier12_fids
    but against dog_parks_gold (no scoring_tier — all definitionally
    scoreable when is_scoreable=true)."""
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT array_agg(distinct fid) "
                "  FROM public.dog_parks_gold "
                " WHERE state=%s AND is_active AND is_scoreable",
                (state,),
            )
            return cur.fetchone()[0] or []
    finally:
        conn.close()


def _make_dp_photo_loader_asset(
    *, name: str, script: str, chunk_size: int, timeout: int,
    parse_pattern: str, description: str, deps: list | None = None,
    workers: int | None = None,
):
    """Factory for dog-park photo-loader assets. Mirrors
    _make_photo_loader_asset but uses dog-park fid source + threads
    --entity dog_park through to the subprocess script."""
    @asset(
        name=name,
        partitions_def=state_partitions,
        deps=deps or [],
        group_name="phase_31_dp_photos",
        description=description,
    )
    def _asset(
        context: AssetExecutionContext,
        postgres: PostgresPoolerResource,
        subproc: SubprocessResource,
    ) -> MaterializeResult:
        state = context.partition_key
        fids = _tier_fids_dog_park(postgres, state)
        if not fids:
            return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})
        total = done = failed = 0
        for i in range(0, len(fids), chunk_size):
            chunk = fids[i:i + chunk_size]
            args_list = ["--entity", "dog_park",
                         "--fids", ",".join(str(x) for x in chunk)]
            if workers and workers > 1:
                args_list += ["--workers", str(workers)]
            result = subproc.run(
                script,
                args=args_list,
                timeout=timeout,
            )
            if result.returncode != 0:
                failed += 1
                context.log.warning(
                    f"DP chunk {done+1} failed (fids {chunk[0]}..{chunk[-1]}): "
                    f"{(result.stderr or '')[-300:]}"
                )
                continue
            m = re.search(parse_pattern, result.stdout or "")
            if m:
                total += int(m.group(1))
            done += 1
        return MaterializeResult(metadata={
            "state":         MetadataValue.text(state),
            "fids_total":    MetadataValue.int(len(fids)),
            "photos_saved":  MetadataValue.int(total),
            "chunks_done":   MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        })
    return _asset


dp_photos_flickr = _make_dp_photo_loader_asset(
    name="dp_photos_flickr",
    script="scripts/load_flickr_photos.py",
    chunk_size=50, timeout=600,
    parse_pattern=r"saved=(\d+)",
    deps=[dp_triage_needs_review],
    # 4 workers × 2.5s throttle = ~1.6 QPS effective — well under Flickr's
    # documented limit. Soft-flag at high burst rates is the real concern;
    # keep this conservative.
    workers=4,
    description="Dog-park Flickr photo loader. Geo search + entity-aware filter.",
)

dp_photos_wikimedia = _make_dp_photo_loader_asset(
    name="dp_photos_wikimedia",
    script="scripts/load_wikimedia_commons_photos.py",
    chunk_size=30, timeout=900,
    parse_pattern=r"saved=(\d+)",
    deps=[dp_triage_needs_review],
    description="Dog-park Wikimedia Commons loader. Geosearch by park lat/lng + 500m radius.",
)

# dp_photos_unsplash REMOVED 2026-05-26 — generic stock; Maryland City
# Dog Park + Ma & Pa Dog Park (64 km apart) both got the same Unsplash
# photo as their hero. Same failure mode as pixabay/pexels earlier.

# Web-search source (Tavily). Most relevant for dog parks since geo-tagged
# CC sources (Wikimedia/Flickr) miss most parks. Tavily returns image URLs
# + AI descriptions; vision tagger gates display.
dp_photos_websearch = _make_dp_photo_loader_asset(
    name="dp_photos_websearch",
    script="scripts/load_websearch_photos.py",
    chunk_size=30, timeout=900,
    parse_pattern=r"saved=(\d+)",
    deps=[dp_triage_needs_review],
    workers=8,  # Tavily handles this throughput well
    description=(
        "Dog-park web-image-search loader (Tavily). Embeds third-party-hosted "
        "image URLs with link-back attribution; vision tagger + dp_photos_curate "
        "filter for relevance + quality before consumer surface."
    ),
)


@asset(
    partitions_def=state_partitions,
    deps=[dp_photos_flickr, dp_photos_wikimedia, dp_photos_websearch],
    group_name="phase_31_dp_photos",
    description=(
        "Two-pass Haiku vision tagger for dog-park photos. Reads "
        "dog_park_photos rows with no current vision tag, writes "
        "source_meta.vision JSON. Per-state partition, chunked."
    ),
)
def dp_photo_vision_tags(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    chunks_per_state = 10
    chunk_size = 50
    total_tagged = total_err = 0
    for i in range(chunks_per_state):
        result = subproc.run(
            "scripts/load_photo_vision_tags.py",
            args=[
                "--entity",     "dog_park",
                "--state",      state,
                "--chunk-size", str(chunk_size),
                "--workers",    "4",
                "--budget-usd", "5.0",
            ],
            timeout=900,
        )
        if result.returncode != 0:
            context.log.warning(
                f"DP vision chunk {i+1} failed: {(result.stderr or '')[-300:]}"
            )
            total_err += 1
            break
        out = result.stdout or ""
        m_tagged = re.search(r"ok=(\d+)", out)
        if m_tagged:
            n = int(m_tagged.group(1))
            total_tagged += n
            if n == 0:
                break
        else:
            break
    return MaterializeResult(metadata={
        "state":         MetadataValue.text(state),
        "photos_tagged": MetadataValue.int(total_tagged),
        "chunks_failed": MetadataValue.int(total_err),
    })


@asset(
    partitions_def=state_partitions,
    deps=[dp_photo_vision_tags],
    group_name="phase_31_dp_photos",
    description=(
        "Vision-gated curation for dog parks. Top 3 photos per park ranked "
        "by sort_order, filtered to those whose vision tag passes "
        "(has_dog=true OR scene in outdoor/park/beach) AND no quality_issue. "
        "Marks curated_at=now() so they render via anon RLS."
    ),
)
def dp_photos_curate(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH ranked AS (
                  SELECT p.id, p.dog_park_fid,
                         ROW_NUMBER() OVER (
                           PARTITION BY p.dog_park_fid
                           ORDER BY p.sort_order ASC NULLS LAST, p.id ASC
                         ) AS rk
                    FROM public.dog_park_photos p
                    JOIN public.dog_parks_gold g ON g.fid = p.dog_park_fid
                   WHERE g.state = %s
                     AND p.curated_at IS NULL
                     AND p.hidden_at IS NULL
                     -- Policy 2026-06-01 [[dp-photos-are-dog-only]]:
                     -- DP photos must show a visible dog. is_dog_park_relevant
                     -- captures fences / equipment / signage which are valid
                     -- DP content but not what users want to see.
                     AND (p.source_meta -> 'vision' ->> 'has_dog')::boolean = true
                     AND COALESCE(p.source_meta -> 'vision' ->> 'quality_issue', 'none') = 'none'
                )
                UPDATE public.dog_park_photos p
                   SET curated_at = now(),
                       curated_by = 'vision-auto',
                       match_quality = 'medium'
                  FROM ranked r
                 WHERE p.id = r.id AND r.rk <= 3
            """, (state,))
            n_curated = cur.rowcount
            conn.commit()
            cur.execute("""
                SELECT count(DISTINCT p.dog_park_fid)
                  FROM public.dog_park_photos p
                  JOIN public.dog_parks_gold g ON g.fid = p.dog_park_fid
                 WHERE g.state = %s AND p.curated_at IS NOT NULL AND p.hidden_at IS NULL
            """, (state,))
            parks_with_photo = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM public.dog_parks_gold "
                " WHERE state = %s AND is_active AND is_scoreable",
                (state,),
            )
            total_parks = cur.fetchone()[0]
    finally:
        conn.close()
    return MaterializeResult(metadata={
        "state":            MetadataValue.text(state),
        "photos_curated":   MetadataValue.int(n_curated),
        "parks_with_photo": MetadataValue.int(parks_with_photo),
        "total_parks":      MetadataValue.int(total_parks),
        "coverage_pct":     MetadataValue.float(
            round(100.0 * parks_with_photo / max(total_parks, 1), 1)
        ),
    })
