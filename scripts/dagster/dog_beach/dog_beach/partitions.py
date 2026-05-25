"""Partitions definitions.

Pipeline work is naturally per-state, per-operator, or per-beach (fid).
Each dimension gets a PartitionsDefinition; assets reference whichever
applies. The framework handles per-partition status tracking, retries,
and concurrency.

──── Static: state_partitions ───────────────────────────────────────────
MVP+ priority-1 (CA/OR/WA) + next-tier picks (MA/FL/MI) per
project_mvp_plus_ca_or_wa.md. To activate another state: add its code
here and load PAD-US for it. The pad_us_loaded_sensor will then fire
the per-state asset chain for that state's first run.

──── Dynamic: operator_partitions ───────────────────────────────────────
One partition per operator row in public.operators (level in {city,
county, federal, state, special-district, private, joint, tribal}).
Populated by new_operators_sensor — when populate_operators_for_state
adds rows (Phases 1-12), the sensor adds them as partitions on its next
tick. Phase 26 (operator LLM extract) fans out across this set.

DynamicPartitionsDefinition is the Dagster primitive for cases where
the partition set isn't known at code-load time and grows via sensor.
Partition keys are strings — we use operator_id rendered as text.

──── Dynamic: fid_partitions ────────────────────────────────────────────
One partition per active+scoreable beaches_gold row. Populated by
new_fids_sensor. Used by Phases 29 (section_extract), 30 (descriptions),
31 (photos_wikimedia) — anything that fans out per beach.

──── Multi-partitioned (composite) ──────────────────────────────────────
Not used in v0.2.0. Could be useful for "per-state per-day" scoring
recomputes — see Dagster's MultiPartitionsDefinition. Future.
"""

from dagster import StaticPartitionsDefinition, DynamicPartitionsDefinition


# Static — known at code-load time.
state_partitions = StaticPartitionsDefinition(
    [
        # MVP+ priority-1 (in launch order)
        "CA",
        "OR",
        "WA",
        # Next-tier picks (states #4-6 per Franz 2026-05-12)
        "MA",
        "FL",
        "MI",
        "MD",
        "OH",
        # Subsequent launches (2026-05-22 onward):
        "DE",   # 2026-05-22
        "RI",   # 2026-05-22
        "NH",   # 2026-05-23 virgin-test launch (51/51 phases)
        "AL",   # 2026-05-24 — Wikimedia photos (no state-park-specific loader yet)
    ]
)

# Dynamic — populated by sensors from current DB rows.
operator_partitions = DynamicPartitionsDefinition(name="operator_id")
fid_partitions      = DynamicPartitionsDefinition(name="fid")
