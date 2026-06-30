---
name: seasonal-policy
description: Use this skill when working with SEASONAL dog policy — beaches where dogs are prohibited (or allowed) only during certain date ranges (plover nesting, summer bans, winter-vs-summer hour windows). Triggers include "this beach should be closed to dogs in summer", "seasonal closure isn't showing", "the NOW card shows a seasonal beach as open during its closed window", "dogs_prohibited shows year-round but it's seasonal", "plover closure", "add a seasonal window for <beach>". Covers the temporal → zone_rules → v3-score → NOW-card data flow and how to verify a seasonal beach end-to-end. DO NOT use for year-round daily prohibitions (those use the flat dogs_prohibited_start/end fields directly), for off-leash carve-outs (use codify-state Step 7.5), or for non-seasonal scoring.
---

# seasonal-policy — how seasonal dog closures flow + how to verify

Seasonal closures (e.g. Playa Tortuga SD: prohibited 9am–6pm Apr 1–Oct 31, 9am–4pm Nov 1–Mar 31) are **beach-only** — dog parks have no seasonal model (no temporal table / zone_rules). The data flows through several layers; know which layer is authoritative before "fixing" a wrong-looking closure.

## The data flow (authoritative layer first)

```
beach_policy_source_temporal          ← extraction writes seasonal date-ranges here
  (window_kind: 'daily' | 'seasonal' | 'seasonal_and_daily';
   effective_from_md/effective_to_md = 'MM-DD'; daily_start/daily_end = time)
      │  _zr_inject_from_policy_sources (H3 injector)
      ▼
beach_dog_policy.zone_rules.seasons[].regions[].sections[].time_windows[]   ← seasons live HERE
      │  _beach_closures_from_policy(dogs_allowed, zone_rules) → closures jsonb
      │  _v2_is_hour_closed(closures, date, hour)  ← SEASONAL-AWARE, Oct–Mar wraparound
      ▼
v3 score  (compute_beach_hourly_v2 → apply_v2_best_window_to_beach_recommendations_bulk)
      → beach_day_hourly_scores.hour_score_v3 = NULL during a closed hour
      → this is the CONSUMER-FACING score (find/index/detail/beach.html all read v3)
```

**KEY:** the v3 score IS seasonal-correct. The seasonal logic lives in `zone_rules`, not the flat fields.

## The flat fields are YEAR-ROUND ONLY (mig 20260630c)

`beach_dog_policy.dogs_prohibited_start/end` carry **only year-round** (`window_kind='daily'`)
prohibitions. The aggregator `_canonical_dogs_from_policy_sources` `daily_prohib` CTE was gated to
`window_kind = 'daily'` so a seasonal window never stamps a season-less flat field. Two consumers
read the flat fields — both now correct:
- **beach.html Hours line** — only shows genuine year-round windows; seasonal beaches show their
  season via the zone-rules block (already rendered). No season-less "9am–6pm year-round" lie.
- **get-beach-now (NOW card, hourly-tier beaches)** — no longer reads flat fields for closure; calls
  **`dogs_closed_for_hour(fid, local_date, local_hour)`** which reuses `_beach_closures_from_policy`
  + `_v2_is_hour_closed` (same helpers as v3) so the NOW card matches the v3 score exactly.

`daily`-tier beaches don't hit get-beach-now — their NOW display comes from the v3 forecast (already
seasonal-correct). The NOW fix only matters for `scoring_tier='hourly'` beaches.

## Diagnosing "the seasonal closure is wrong"

1. **Is the score wrong, or just the display?** Check `beach_day_hourly_scores.hour_score_v3` for the
   beach on a date in vs out of season. If v3 is right, the score path is fine — the bug is display/NOW.
2. **Is the season in zone_rules?** `SELECT public._beach_closures_from_policy(dogs_allowed, zone_rules)
   FROM beach_dog_policy WHERE arena_group_id=<fid>;` — look for `season_from_md`/`season_to_md` on the
   closure objects. If absent, the H3 injector didn't carry the season → check the temporal row.
3. **Is the temporal row right?** `SELECT window_kind, effective_from_md, effective_to_md, daily_start,
   daily_end, exception_rule FROM beach_policy_source_temporal WHERE beach_fid=<fid>;` — seasonal needs
   `effective_from_md`/`to_md`; `seasonal_and_daily` needs both season + daily times.
4. **After fixing temporal/zone_rules**, re-promote: `SELECT public._refresh_beaches_from_ps(ARRAY[<fid>]::bigint[]);`
   (recomputes flat fields + re-promotes). Then re-score via `apply_v2_best_window_to_beach_recommendations_bulk(NULL, ARRAY[<fid>], NULL, 6)`.

## Verifying a seasonal beach end-to-end

Pick a beach + an hour that DIFFERS by season (the discriminating test):
```sql
-- Playa Tortuga 6429: summer 9–6, winter 9–4 → 5pm differs
SELECT public.dogs_closed_for_hour(6429,'2026-06-30',17) AS summer_5pm,  -- expect true
       public.dogs_closed_for_hour(6429,'2026-12-15',17) AS winter_5pm;  -- expect false
```
Then confirm on the consumer surface ([[claim-tested-without-end-state-verification]]):
`beach.html?fid=<n>` → Hours/zone-rules block shows the season; bar charts close during the in-season
window and open out of season.

## Gotchas

- **Don't "fix" the flat fields to carry seasons.** They're year-round-by-design; seasons go via
  zone_rules. Re-widening `daily_prohib` to `seasonal_and_daily` re-introduces the season-less lie.
- **v1/v2 engine residual** ([[v1-scoring-engine-residual]]): daily-beach-refresh's TS v2 path is
  season-blind, but v2 is NOT consumer-facing (v3 is). Don't chase v2 seasonal correctness.
- **Beach-only.** Dog parks have no seasonal model — no port owed ([[paired-functions-port-fixes-both-sides]]).
- **Wraparound seasons** (e.g. Nov–Mar) are handled by `_v2_is_hour_closed` (`from_md > to_md` branch).
