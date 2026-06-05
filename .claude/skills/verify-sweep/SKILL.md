---
name: verify-sweep
description: Use this skill when shipping a change that touches the consumer surface (beach.html / dog-park.html / find.html / index.html / detail.html) or any edge function/RPC/SQL that those pages read. Triggers include "verify the deploy", "audit Phase X", "verify sweep", "we just shipped a resolver/cascade/operator change — verify it landed". Builds an `admin/verify-YYYY-MM-DD.html` index page of click-through targets covering multiple attribution paths × multiple states × multiple page types, and gates "done" on Franz actually clicking the links per HARD rule [[claim-tested-without-end-state-verification]]. Do NOT use for backend-only changes (Dagster asset, dbt model, internal table refactor) that don't touch consumer reads — those don't need the sweep.
---

# verify-sweep — end-state click-through audit

The rule, restated: a change is "done" when the consumer pages render correctly, not when SQL counts agree, the migration applied, or the edge function compiled. This skill operationalizes [[claim-tested-without-end-state-verification]] — the rule we re-violated three times in one session today before Franz had to manually catch the fourth round.

Today's proof point (2026-06-04): Phase 14 PAD-US resolver rewrite. Three rounds of "I missed it" — 14h (RPCs), 14i (edge functions), dog-park window — were caught by clicking through `admin/verify-2026-06-04.html` after each round, not by re-grepping.

## When to fire

Build the verify page IMMEDIATELY after:
- A resolver / cascade / propagation change (operator attribution, dog-policy promotion, beach_dog_policy writes)
- An edge function deploy that consumer pages read (`get-beach-summary`, `get-beach-detail`, `get-beaches-find`, `nearest_dog_parks`, etc.)
- Renaming or dropping a column / table / RPC that downstream code might still reference
- A Phase N migration that lands new fields on `beaches_gold` / `beach_dog_policy` / `dog_parks_gold`

Don't bother for:
- Pure backend (Dagster asset, dbt model, audit script)
- Internal table refactors with no consumer read path
- Documentation, memory pins, skill files

## Step 1 — Pick the coverage matrix

Don't just pick "5 beaches I happened to test." The sweep should cover the dimensions the change might break:

| Dimension | Examples |
|---|---|
| **State** | CA, OR, WA, MA, MD, NH, VA — at minimum 3 distinct |
| **Attribution path** | manual_curator / PAD-US federal (NPS, USFS, BLM, USFWS) / PAD-US state / CPAD CA / TIGER city / no operator |
| **Page type** | beach.html, dog-park.html, find.html (all want-modes), index.html, detail.html, mobile-beach.html |
| **Policy type** | off_leash / on_leash / not_allowed / seasonal / varies_by_zone / unknown |
| **Edge case** | beach with `zone_rules` JSONB, no operator inferred, recently-curated, sentinel full-day prohibition |

Aim for 6-8 beaches across these axes. If today's change is operator-specific, weight toward operator-path coverage; if it's resolver-wide, spread evenly.

## Step 2 — Write `admin/verify-YYYY-MM-DD.html`

Template: copy `admin/verify-2026-06-04.html` and edit. Structure:
1. **Lead paragraph** — what was just shipped and what specifically to look for
2. **"What to look for on each page"** checklist — 500/blank, missing operator name, missing Scout blurb, wrong leash rule, no best window
3. **Section per page type** (beach.html / dog-park.html / find.html / detail.html / index.html / mobile + misc)
4. **Pills indicating state + attribution path** so Franz can see the matrix at a glance without clicking
5. **Raw API endpoints section** for JSON spot-checks if a page renders weird
6. **"If you find something broken" footer** — dev tools → Network tab → 4xx/5xx → paste error body → likely names the missing column/RPC

Inline `<script>` at bottom builds today's date for detail.html and full Supabase JSON URLs with the anon key. Anon key is safe to commit (RLS is the gate).

## Step 3 — Tell Franz, give him the link

Output text — short:
- "Verify page built at `file:///C:/Users/beach/Documents/dog-beach-claude/admin/verify-YYYY-MM-DD.html`"
- One sentence on the matrix coverage ("8 beaches × 5 attribution paths × 5 page types")
- Don't summarize the changes — Franz knows what was shipped, the verify page is to catch what he doesn't yet know is broken

## Step 4 — When Franz reports a hit

Franz pastes an error body or page screenshot. The shape of the fix is usually obvious from the error:
- `operators_1.name does not exist` → a SELECT in an edge function or RPC references the renamed column
- `column "X" does not exist on table Y` → migration didn't land or a function is stale
- Blank Scout blurb → narrative generation throwing; check `beach-chat` function logs
- Wrong leash rule → propagation didn't run; rerun `compute_beach_field_consensus + promote_canonical_dogs_to_beach_dog_policy` for the fid

After fixing, **redeploy/re-propagate, then re-test the same fid** before declaring done. Don't move on to other beaches.

## Step 5 — Iterate until clean

If you fix one and Franz finds another, that's still inside the same sweep. Don't rebuild the verify page — re-use it.

The sweep is done when Franz has clicked everything and reported no hits. Saying "I checked the SQL" is not closing the sweep. Per [[never-solve-same-problem-twice]] + [[claim-tested-without-end-state-verification]]: the click is the verification.

## Anti-patterns

- **Verifying with `curl` instead of browser**: curl skips JS, so a broken Scout blurb or front-end null-handling bug won't surface. Browser only.
- **Picking only the same 3 beaches you always use**: if change is cross-cutting, your 3 favorites miss the breaking case. Use the matrix.
- **Skipping the dog-park.html page** because "we're only touching beaches": today's 14i bug was dog-park.html 500 from a beach-side operator rename. Adjacent pages share substrate.
- **Single-state sweep**: today's CA-only audit would have missed the MA fid 1355333 issue. Always ≥3 states.
- **Building the verify page after Franz asks "did you test it"**: too late. Build it during the change, not after.

## Anchors

The verify-page recipe is also useful to keep around for re-running the same sweep after future changes — date-stamping the filename keeps history. Old verify pages serve as the "what we covered last time" cheat sheet for the next sweep.

Per [[regular-data-quality-audits]]: verify sweeps are surveillance, not crisis forensics. Build them by default for any consumer-touching change.
