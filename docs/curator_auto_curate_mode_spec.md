# Curator auto-curate review mode — spec

**Status:** draft 2026-05-19
**Author:** Claude (Franz directive 2026-05-19 — make auto-curate picks reviewable)
**Related:** `scripts/auto_curate.py`, `curate.html`, `[[photo-curation-v3-spec]]`

## Why

`auto_curate.py` writes `beach_photos.curated_by = 'auto:n=6'` (or `'auto:n=8'`, etc.) for the top-N model-picked photos per beach. These photos surface on the consumer side immediately via the existing diverse selector. We need a curator-side review path so Franz can:

- Quickly see what auto-curate picked vs the alternatives
- Approve picks en masse (state-wide) when scrolling reveals them as solid
- Approve / override per-beach when one needs attention
- Clear the auto-curate decision on a beach so the next `auto_curate.py` run (possibly with a different `n`, e.g., 6 → 8) re-decides

## What

A new **Mode** dropdown at the top of `curate.html`:

```
Mode: [ Manual curate ▼ ]
      [ Manual curate    ]
      [ Auto-curate review ]
```

Mode is a **global filter** — switches the entire curator UI between two flows. Persisted in URL (`?mode=auto`) and localStorage.

### Manual curate (current behavior)

Unchanged.

### Auto-curate review mode

1. **Beach picker** filters to beaches where ≥1 photo has `curated_by LIKE 'auto:%'`.
2. **Per-beach panel** (when a beach is loaded):
   - Top row: the auto-curated set (badged with the n=, sorted by `sort_order`)
   - Bottom row: alternatives — uncurated photos with `predicted_keep_prob` ≥ some floor (e.g., 0.5), so curator sees what the model didn't pick
   - **Optional comment field** — single-line text input adjacent to the Approve / Clear buttons. Captures *why* the curator approved or cleared (e.g., "all looked clean", "wrong-beach photos slipped in"). Persisted to `beach_curator_review_log` (new table — see Data below). Always optional; empty submit is fine.
   - Two per-beach buttons (comment value attached to whichever fires):
     - **Approve** — flips `curated_by` from `'auto:n=N'` to the curator's name (locks; future `auto_curate.py` runs skip this beach per the script's existing `LIKE 'auto:%'` guard). Writes one log row with `action='approve'` + comment.
     - **Clear & re-decide** — sets `curated_by=NULL, curated_at=NULL` on the auto rows. No tombstone (intentional — see "Why not tombstone" below). Next auto-curate run will re-pick. Writes one log row with `action='clear'` + comment.
3. **Bulk action**: at the top of the beach list, "Approve all auto-curated in {state}" button — applies Approve to every auto-curated beach in the current state filter (with confirm dialog showing row count).

### Why not tombstone on Clear

Future `auto_curate.py` runs may use a different `n` (Franz wants to try `n=8`). If we tombstoned rejected picks, increasing `n` would never resurface them — losing potentially better candidates. Clear-without-tombstone trusts the model to re-pick correctly when the budget changes.

## Implementation notes

### Data
- `curated_by` text column on `beach_photos` already discriminates (`auto:n=6` vs human name). No change there.
- `curated_by` filter: `curated_by IS NOT NULL AND curated_by LIKE 'auto:%'` for auto-curated; `IS NOT NULL AND curated_by NOT LIKE 'auto:%'` for human.
- **New table `beach_curator_review_log`** captures the comment + action audit:
  ```
  CREATE TABLE beach_curator_review_log (
    id           bigserial PRIMARY KEY,
    beach_fid    bigint NOT NULL REFERENCES beaches_gold(fid) ON DELETE CASCADE,
    curator      text   NOT NULL,
    action       text   NOT NULL CHECK (action IN ('approve','clear','override')),
    auto_marker  text,            -- e.g., 'auto:n=6' at time of review
    n_photos     int,             -- count of auto rows touched
    comment      text,            -- optional curator note (≤500 chars)
    created_at   timestamptz NOT NULL DEFAULT now()
  );
  CREATE INDEX ON beach_curator_review_log(beach_fid, created_at DESC);
  CREATE INDEX ON beach_curator_review_log(curator, created_at DESC);
  ```
  Bulk approve writes one row per beach with `action='approve'` and `comment = '<bulk> in {state}'` (or whatever the curator typed in the bulk-confirm dialog).

### RPCs (probably need)
- `get_auto_curated_beach_fids(state, county?, ...)` — returns fids with ≥1 auto-curated photo. Drives the beach picker.
- `approve_auto_curated_beach(fid, curator_name, comment text DEFAULT NULL)` — UPDATE `curated_by = curator_name` WHERE `arena_group_id = fid AND curated_by LIKE 'auto:%'` + INSERT into `beach_curator_review_log` with action='approve'. Returns row count.
- `clear_auto_curated_beach(fid, curator_name, comment text DEFAULT NULL)` — UPDATE `curated_by = NULL, curated_at = NULL` WHERE same predicate + INSERT log row with action='clear'. Returns row count.
- `approve_auto_curated_state(state, curator_name, comment text DEFAULT NULL)` — bulk apply. Confirm-dialog precondition: caller passes expected row count + state; RPC compares actual vs expected, refuses if mismatch (avoids race surprises). Logs one row per beach.

### UI
- Single Mode dropdown insert at top of `curate.html` controls section.
- State/county/beach pickers unchanged in both modes.
- Per-beach panel: small CSS treatment to badge auto-curated photos (e.g., bot icon + `n=6` overlay).

## Effort estimate

- 4 RPCs + migration: ~1 hr
- `curate.html` mode toggle + auto-review panel + bulk button: ~2-3 hr
- Test on first auto-curate run output (WA WSPRC beaches): ~30 min

**Total: ~4 hr.** Queue after the in-flight WSPRC vision-tag + retrain + auto-curate run completes.

## Out of scope (v1)

- Tombstoning rejected auto-curate picks (see above — intentionally no)
- Per-photo override inside auto-review mode (use Manual mode for that)
- Auto-curate dry-run preview (the script has `--dry-run` already)
- Auto-curate scheduling / cron — manual `auto_curate.py` invocation continues
