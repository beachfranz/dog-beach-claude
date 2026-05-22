"""gen_state_park_baseline.py — emit a state-park-system codify migration.

Every state has a state-park-system regulation governing pets on state-park
lands (COMAR 08.07.06.17 in MD, OAR 736-010-0030 in OR, WAC 352-32-070 in
WA, CA Title 14 §4313 / CDPR rules in CA). Today we hand-code each. This
script standardizes the SQL emission once an agent has gathered the values.

Encodes our 2026-05-22 learnings:
  * agency INSERT uses WHERE NOT EXISTS (not ON CONFLICT — public.agency
    has no UNIQUE (name, type)).
  * policy_source INSERT uses WHERE NOT EXISTS on citation prefix.
  * BPS uses the 5-col ON CONFLICT matching beach_policy_source_dedupe_idx
    (`(beach_fid, policy_source_id, section, COALESCE(region_name,
    '__default__'::text), rule)`).
  * state_dogs_policy uses DELETE+INSERT (table has no PK on prod).
  * full_text uses $$..$$ dollar-quoting (Supabase CLI HTTP endpoint
    rejects adjacent E-string concat).
  * If --has-statewide-law false (the MD case), state_dogs_policy seed is
    framed honestly as "no statewide leash law; state-park rule at <cite>"
    rather than misattributing the state-park rule as statewide default.

Usage:
  python scripts/gen_state_park_baseline.py \
    --state MD --state-name Maryland \
    --agency 'Maryland Department of Natural Resources' \
    --agency-short 'MD DNR' --agency-web https://dnr.maryland.gov/ \
    --citation 'COMAR 08.07.06.17 (Pets) — Maryland Park Service' \
    --subtype state_regulation \
    --source-url https://regs.maryland.gov/us/md/exec/comar/08.07.06.17 \
    --full-text-file tmp/state_park_rules/MD_comar.txt \
    --effective-date 1993-10-11 \
    --rule on_leash \
    --has-statewide-law false \
    --sd-source-quote-file tmp/state_park_rules/MD_state_dogs_quote.txt \
    --sd-ordinance-ref '(no statewide leash law; state-park rule at COMAR 08.07.06.17)' \
    --fids 6310347,18144392 \
    --label md_state_park_baseline

Does NOT apply the migration. Path is printed on success.
"""
from __future__ import annotations
import argparse
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


def _sql_str(s: str) -> str:
    """Single-quote escape for SQL literals (NOT for dollar-quoted blocks)."""
    return s.replace("'", "''")


def _validate_url(url: str) -> None:
    """Sanity check: URL should be a deep link, not an agency homepage."""
    if not url.startswith(("http://", "https://")):
        raise SystemExit(f"--source-url must be http(s): got {url}")
    # Reject obvious homepages — per page-level-over-agency-level pin.
    if re.fullmatch(r"https?://[^/]+/?", url):
        raise SystemExit(
            f"--source-url is a homepage ({url}); deep-link to the section/chapter "
            "per page-level-over-agency-level pin"
        )


def _validate_subtype(subtype: str) -> None:
    """Per playbook §7 valid subtypes."""
    valid = {
        "state_statute", "federal_regulation", "state_regulation", "municipal_code",
        "special_district_ordinance", "tribal_resolution",
        "mou", "lease_agreement", "operating_agreement", "concession_lease",
        "agency_administrative_policy", "superintendents_compendium",
        "operator_posted_policy", "withdrawn_rulemaking",
        "community_attestation", "promotional_listing", "inferred",
    }
    if subtype not in valid:
        raise SystemExit(f"--subtype {subtype!r} not in valid set: {sorted(valid)}")


def _validate_rule(rule: str) -> None:
    valid = {"on_leash", "off_leash", "off_leash_voice_control", "not_allowed"}
    if rule not in valid:
        raise SystemExit(f"--rule {rule!r} not in valid set: {sorted(valid)}")


def _validate_default_rule(s: str) -> None:
    valid = {"yes", "no", "mixed", "unknown"}
    if s not in valid:
        raise SystemExit(f"--sd-default-rule {s!r} not in valid set: {sorted(valid)}")


def _read_file(path: Path) -> str:
    if not path.exists():
        raise SystemExit(f"file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def _emit_migration(args: argparse.Namespace) -> str:
    """Build the SQL string."""
    state = args.state.upper()
    full_text = _read_file(Path(args.full_text_file))
    if "$$" in full_text or args.dollar_tag in full_text:
        raise SystemExit(
            f"full_text contains '{args.dollar_tag}' or '$$' which collides with "
            "dollar-quoting; pick a different --dollar-tag (default $body$)"
        )

    fids = []
    if args.fids:
        for tok in args.fids.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                fids.append(int(tok))
            except ValueError:
                raise SystemExit(f"--fids includes non-int token: {tok!r}")

    citation_lit = _sql_str(args.citation)
    citation_prefix = _sql_str(args.citation.split("—")[0].strip()[:60])

    # Honest state_dogs_policy framing
    sd_default_rule = args.sd_default_rule
    sd_has_on_leash = "true" if args.sd_has_on_leash else "false"
    sd_has_off_leash = "true" if args.sd_has_off_leash else "false"
    sd_confidence = args.sd_confidence
    sd_dogs_allowed = args.sd_dogs_allowed

    sd_source_quote = _read_file(Path(args.sd_source_quote_file)) if args.sd_source_quote_file else (
        f"State-park system governed by {args.citation}. "
        + ("This is a statewide leash law applying beyond state parks." if args.has_statewide_law
           else f"{args.state_name} has no statewide leash law beyond this state-park-system rule. "
                f"Local ordinances (counties + cities) govern non-state-park beaches.")
    )
    sd_source_quote_lit = _sql_str(sd_source_quote)

    sd_ordinance_ref = args.sd_ordinance_ref or (
        args.citation if args.has_statewide_law
        else f"(no statewide leash law; state-park rule at {args.citation.split('—')[0].strip()})"
    )
    sd_ordinance_ref_lit = _sql_str(sd_ordinance_ref)

    sd_source_url = args.sd_source_url or args.source_url
    sd_source_url_lit = _sql_str(sd_source_url)

    sd_scope_notes = args.sd_scope_notes or (
        "Last-resort fallback only. Beaches inside state-park polygons get the "
        f"{args.citation.split('—')[0].strip()} rule via explicit BPS rows. "
        "Municipal/county beaches via local codify. Federal-land beaches via "
        "federal codify. This row fires only for beaches not covered by any "
        "per-jurisdiction codify."
    )
    sd_scope_notes_lit = _sql_str(sd_scope_notes)

    eff_date_lit = f"DATE '{args.effective_date}'" if args.effective_date else "NULL::date"

    # ── Build sections ──────────────────────────────────────────────────
    header = f"""-- {args.label}.sql
--
-- {args.state_name.upper()} STATE-PARK SYSTEM CODIFY
-- Generated by scripts/gen_state_park_baseline.py on {date.today().isoformat()}.
--
-- ─── Critical framing ───────────────────────────────────────────────
--
-- {args.state_name} has {'a' if args.has_statewide_law else 'NO'} statewide leash law.
-- {args.citation} governs ONLY beaches inside the {args.state_name}
-- state-park system. {'It also serves as the statewide default.' if args.has_statewide_law else
'Beyond state parks, leash rules are creatures of local ordinances (counties + cities) which vary widely.'}
--
-- THREE parts:
--   1. policy_source row for {args.citation.split('—')[0].strip()}
--   2. BPS rows attaching the ps to {len(fids)} state-park beach(es): {fids if fids else 'NONE provided'}
--   3. state_dogs_policy seed ({'statewide-law' if args.has_statewide_law else 'no-statewide-law'} framing)
--
-- Per [[page-level-over-agency-level]]: source_url is the deep-link, not the agency homepage.
-- Per [[state-dogs-policy-honest-framing]]: state_dogs_policy seed reflects
-- {'actual statewide rule' if args.has_statewide_law else 'absence of statewide law'}.
"""

    sec1_agency = f"""
begin;

-- ─── 1. Ensure agency row (canonical bare name) ────────────────────

INSERT INTO public.agency (name, type, short_name, web_url)
SELECT '{_sql_str(args.agency)}',
       '{args.agency_type}'::agency_type,
       {"NULL" if not args.agency_short else f"'{_sql_str(args.agency_short)}'"},
       {"NULL" if not args.agency_web else f"'{_sql_str(args.agency_web)}'"}
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
   WHERE name = '{_sql_str(args.agency)}'
     AND type = '{args.agency_type}'::agency_type
);
"""

    sec2_ps = f"""
-- ─── 2. Insert policy_source row for the state-park rule ───────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text, effective_date)
SELECT
  '{args.subtype}',
  '{citation_lit}',
  (SELECT id FROM public.agency
    WHERE name = '{_sql_str(args.agency)}'
      AND type = '{args.agency_type}'::agency_type),
  ARRAY['dog_policy']::text[],
  '{_sql_str(args.source_url)}',
  {args.dollar_tag}{full_text}{args.dollar_tag},
  {eff_date_lit}
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE '{citation_prefix}%'
);
"""

    sec3_bps = ""
    if fids:
        fid_values = ",\n  ".join(f"({fid}::bigint)" for fid in fids)
        sec3_bps = f"""
-- ─── 3. BPS rows: attach ps to state-park beaches ──────────────────
--
-- Beach fids passed via --fids. Each beach inside the state-park
-- system gets one BPS row attaching the COMAR/OAR/WAC/equivalent
-- citation. Section = 'sand'; rule = '{args.rule}'.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', '{args.rule}', 'operative'::operative_status,
       {args.dollar_tag}{_sql_str(args.citation)}: see policy_source.full_text for verbatim quote.{args.dollar_tag},
       ps.source_url,
       NULL,
       NULL
FROM (VALUES
  {fid_values}
) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE '{citation_prefix}%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
"""
    else:
        sec3_bps = """
-- ─── 3. BPS rows: NONE (no --fids provided) ────────────────────────
--
-- No state-park beaches enumerated. Add explicit BPS rows in a
-- follow-up migration as state-park beaches surface in ingestion.
"""

    sec4_state_default = f"""
-- ─── 4. state_dogs_policy seed ({'statewide-law' if args.has_statewide_law else 'honest no-statewide-law'} framing) ─
--
-- DELETE+INSERT pattern (the prod table has no PRIMARY KEY on state_code;
-- ON CONFLICT (state_code) DO UPDATE fails plan-time on prod — see
-- [[state-dogs-policy-honest-framing]] + task #150).

DELETE FROM public.state_dogs_policy WHERE state_code = '{state}';

INSERT INTO public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
VALUES
  ('{state}', '{_sql_str(args.state_name)}', '{sd_dogs_allowed}', '{sd_default_rule}',
   {sd_has_on_leash}, {sd_has_off_leash},
   '{sd_source_quote_lit}',
   '{sd_source_url_lit}',
   '{sd_ordinance_ref_lit}',
   '{sd_scope_notes_lit}',
   {sd_confidence},
   now());

commit;
"""

    footer = f"""
-- ─── Notes / follow-ups ────────────────────────────────────────────
--
-- 1. Other {args.state_name} state-park beaches not in --fids will not
--    automatically attach to this ps. Extend Part 3 with more fids as
--    they surface in ingestion.
-- 2. If a state-park beach has a seasonal carve-out (e.g., Memorial-Day-
--    through-Labor-Day prohibition), run extract_temporal_from_policy_source.py
--    on this ps_id post-apply to lift the window into beach_policy_source_temporal.
-- 3. Apply with:
--      supabase db query --linked -f supabase/migrations/{args.label}.sql
"""

    return header + sec1_agency + sec2_ps + sec3_bps + sec4_state_default + footer


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--state", required=True, help="2-letter state code (e.g. MD)")
    ap.add_argument("--state-name", required=True, help="Full state name (e.g. Maryland)")
    ap.add_argument("--agency", required=True,
                    help="Canonical bare agency name (e.g. 'Maryland Department of Natural Resources')")
    ap.add_argument("--agency-short", default=None, help="Short name (e.g. 'MD DNR')")
    ap.add_argument("--agency-web", default=None, help="Agency homepage URL")
    ap.add_argument("--agency-type", default="state_department",
                    help="agency_type enum (default state_department)")
    ap.add_argument("--citation", required=True,
                    help="Canonical citation (e.g. 'COMAR 08.07.06.17 (Pets) — Maryland Park Service')")
    ap.add_argument("--subtype", required=True, help="policy_source.subtype (state_regulation typical)")
    ap.add_argument("--source-url", required=True, help="Deep-link URL (not a homepage)")
    ap.add_argument("--full-text-file", required=True,
                    help="Path to file containing verbatim ps full_text (with context comment)")
    ap.add_argument("--effective-date", default=None, help="YYYY-MM-DD adoption date")
    ap.add_argument("--rule", required=True,
                    help="BPS rule for in-park beaches (on_leash | not_allowed | off_leash | off_leash_voice_control)")
    ap.add_argument("--fids", default=None,
                    help="Comma-separated fids of state-park beaches to attach (optional)")
    ap.add_argument("--label", required=True,
                    help="Migration filename slug (e.g. md_state_park_baseline)")
    ap.add_argument("--has-statewide-law", choices=["true", "false"], required=True,
                    help="True iff state has a statewide leash law beyond state-park lands. "
                         "False for MD (most states); true for OR (Beach Bill applies broadly).")
    # state_dogs_policy knobs
    ap.add_argument("--sd-default-rule", default="mixed",
                    help="state_dogs_policy.default_rule (mixed|yes|no|unknown; default mixed)")
    ap.add_argument("--sd-dogs-allowed", default="mixed",
                    help="state_dogs_policy.dogs_allowed (mixed|yes|no|unknown; default mixed)")
    ap.add_argument("--sd-has-on-leash", action="store_true",
                    help="state_dogs_policy.has_on_leash (set true ONLY if statewide on-leash rule)")
    ap.add_argument("--sd-has-off-leash", action="store_true",
                    help="state_dogs_policy.has_off_leash (set true ONLY if statewide off-leash provision)")
    ap.add_argument("--sd-confidence", type=float, default=0.30,
                    help="state_dogs_policy.confidence (default 0.30 for honest fallback)")
    ap.add_argument("--sd-source-url", default=None,
                    help="state_dogs_policy.source_url (default = --source-url)")
    ap.add_argument("--sd-source-quote-file", default=None,
                    help="Path to file with state_dogs_policy source_quote (else auto-generated)")
    ap.add_argument("--sd-ordinance-ref", default=None,
                    help="state_dogs_policy.ordinance_ref (else auto-generated)")
    ap.add_argument("--sd-scope-notes", default=None,
                    help="state_dogs_policy.scope_notes (else auto-generated)")
    ap.add_argument("--dollar-tag", default="$body$",
                    help="Dollar-quote tag for full_text (default $body$; pick different "
                         "if your text contains $body$)")
    ap.add_argument("--out-dir", default="supabase/migrations",
                    help="Where to write the migration SQL")
    args = ap.parse_args()

    # Convert --has-statewide-law string → bool
    args.has_statewide_law = args.has_statewide_law == "true"

    # Validate
    _validate_url(args.source_url)
    _validate_subtype(args.subtype)
    _validate_rule(args.rule)
    _validate_default_rule(args.sd_default_rule)
    _validate_default_rule(args.sd_dogs_allowed)

    sql = _emit_migration(args)
    ts = date.today().strftime("%Y%m%d")
    out_path = ROOT / args.out_dir / f"{ts}_{args.label}.sql"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(sql, encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"  fids attached: {args.fids or '(none)'}")
    print(f"  has_statewide_law: {args.has_statewide_law}")
    print()
    print("Apply with:")
    print(f"  supabase db query --linked -f {out_path.relative_to(ROOT).as_posix()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
