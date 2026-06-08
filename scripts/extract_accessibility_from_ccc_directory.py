"""Extract accessibility signals from the California Coastal Commission
beach-wheelchair directory page (https://www.coastal.ca.gov/access/beach-wheelchairs.html).

Single-URL → many-entries shape:
  1. Fetch + BS4-strip the CCC page
  2. Send to Haiku, request an array of beach entries (one per beach listed)
  3. Match each entry to a CA beaches_gold.fid via trigram + county hint
  4. Write a JSON manifest at tmp/ccc_ada_matches.json with per-entry
     candidates and the auto-match decision. NO DB writes by default.
  5. On --apply, read the manifest and write BEP rows for auto-matched
     entries (skipping ambiguous + unmatched; those need manual review).

Auto-match rule (conservative):
  - sim(top_candidate.name, ccc_beach_name) >= 0.85, AND
  - top candidate's county_name matches the CCC-stated county (case-insensitive
    substring either direction), AND
  - top candidate's sim is at least 0.10 higher than the second candidate's.
  Otherwise the entry is logged for human review in the manifest.

Usage:
  python scripts/extract_accessibility_from_ccc_directory.py            # preview, write manifest
  python scripts/extract_accessibility_from_ccc_directory.py --apply    # write BEP rows from manifest
  python scripts/extract_accessibility_from_ccc_directory.py --manifest tmp/foo.json
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# sys.path bootstrap so `from scripts.common.X import Y` works whether invoked
# as `python scripts/extract_accessibility_from_ccc_directory.py` or imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bs4 import BeautifulSoup

from scripts.common.db import connect
from scripts.common.llm import HAIKU

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = ROOT / "tmp" / "ccc_ada_matches.json"

CCC_URL = "https://www.coastal.ca.gov/access/beach-wheelchairs.html"
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = HAIKU

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
MAX_INPUT_CHARS = 40000

AUTO_MATCH_MIN_SIM = 0.85
AUTO_MATCH_MARGIN = 0.10
CANDIDATE_MIN_SIM = 0.30
NORMALIZED_MIN_SIM = 0.95   # 2nd-pass: name with suffixes stripped
TIEBREAK_WINDOW = 0.05      # when top-2 sim within this, prefer county-matched

STRIP_SUFFIXES = (
    " State Beach", " City Beach", " County Beach",
    " State Park", " Regional Park", " Park",
    " Pier",
    " Beach",
)


def normalize_name(n: str) -> str:
    """Strip trailing 'Beach'/'State Beach'/'Park'/etc. variants for 2nd-pass trigram."""
    n = (n or "").strip()
    # Apply suffix strips repeatedly (e.g. "Foo State Beach" -> "Foo State" -> "Foo")
    while True:
        before = n
        for sfx in STRIP_SUFFIXES:
            if n.lower().endswith(sfx.lower()) and len(n) > len(sfx):
                n = n[: -len(sfx)].strip()
                break
        if n == before:
            return n

PROMPT = """Extract every beach listed on this California Coastal Commission wheelchair-access directory.

The page is a county-organized index of California beaches with wheelchair / mobility access programs.

Output ONE JSON object with key "entries" mapping to an array. One element per distinct beach. Use null for any field not explicitly stated for THAT beach.

Each entry shape:

{
  "beach_name":           "<proper-noun beach name as stated, e.g. 'Imperial Beach', 'Silver Strand State Beach'>",
  "city":                 "<city if mentioned, e.g. 'Coronado', 'Watsonville', or null>",
  "county":               "<California county name as stated, e.g. 'San Diego', 'Santa Cruz'>",
  "accessible_parking":   true | false | null,
  "accessible_restrooms": true | false | null,
  "path_to_sand":         "boardwalk" | "ramp" | "mat" | "stairs_only" | "none" | null,
  "beach_mat":            true | false | null,
  "wheelchair_rental":    {"available": true, "provider": "<org name>", "phone": "<number>" | null, "url": "<URL>" | null} | null,
  "accessible_viewing":   true | false | null,
  "notes":                "<short paraphrase of beach-specific access detail>" | null
}

Rules:
- "beach_name" must be the actual beach proper noun, never "Two motorized chairs" or "lifeguard tower".
- "wheelchair_rental.available" = true if any rental program is mentioned for THIS beach. "provider" = the organization (e.g., "Heal the Bay", "Sonoma County Regional Parks"). Leave phone/url null if not given.
- "beach_mat" = true ONLY if a sand mat / Mobi-Mat / accessible-path-onto-sand is mentioned.
- "path_to_sand" = pick ONE if mentioned. "mat" for sand mats; "boardwalk" for wood paths; "ramp" for paved ramps. Generic "accessible" without a path description → null.
- "notes" = short concrete details like "two-hour checkout limit" or "reservations recommended via thatsmypark.org". Keep under 200 chars. Do NOT repeat fields captured above.
- Skip the introductory / grant-program paragraphs — only beach-level entries.
- DO NOT INFER. DO NOT HALLUCINATE. If the page only says "beach access available" without specifying wheelchair detail, skip that entry entirely.

Output ONLY the JSON object. Compact JSON (no pretty-print, no extra whitespace). No commentary, no markdown code fence.

Text:
%s
"""


# ───────────────────────── HTTP + LLM ─────────────────────────

def fetch_html(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


def bs4_strip(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "header", "footer", "nav"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_INPUT_CHARS]


def call_haiku(text: str) -> tuple[dict, dict]:
    body = {
        "model": MODEL,
        "max_tokens": 32000,  # CCC page has 130+ entries; allow room (16k truncated mid-JSON)
        "messages": [{"role": "user", "content": PROMPT % text}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        resp = json.loads(r.read())
    out = "".join(p["text"] for p in resp["content"] if p["type"] == "text").strip()
    if out.startswith("```"):
        out = out.split("```", 2)[1]
        if out.startswith("json\n"):
            out = out[5:]
        out = out.rstrip("` \n")
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError as e:
        print(f"  PARSE_FAIL: {e}", file=sys.stderr)
        print(f"  raw[:400]={out[:400]}", file=sys.stderr)
        parsed = {"entries": []}
    return parsed, resp.get("usage", {})


# ───────────────────────── Matching ─────────────────────────

def find_candidates(cur, beach_name: str, county: str | None, k: int = 3) -> list[dict]:
    """Return up to k CA gold candidates ranked by trigram on name.
    Also computes a 'sim_norm' (suffix-stripped trigram) so the decision
    function can fall through to it when the raw form doesn't clear the bar.
    State-filtered; county is metadata only (not a filter) so we don't
    miss cross-county misattributions in the CCC source."""
    norm_probe = normalize_name(beach_name)
    cur.execute("""
        SELECT fid, name, county_name,
               round(similarity(name, %(raw)s)::numeric, 3)        AS sim,
               round(similarity(
                   regexp_replace(name,
                     '\\s+(State Beach|City Beach|County Beach|State Park|Regional Park|Park|Pier|Beach)\\s*$',
                     '', 'i'),
                   %(norm)s
               )::numeric, 3) AS sim_norm
          FROM beaches_gold
         WHERE state = 'CA' AND is_active
           AND (similarity(name, %(raw)s) >= %(min)s
                OR similarity(
                     regexp_replace(name,
                       '\\s+(State Beach|City Beach|County Beach|State Park|Regional Park|Park|Pier|Beach)\\s*$',
                       '', 'i'),
                     %(norm)s) >= %(min)s)
         ORDER BY GREATEST(similarity(name, %(raw)s),
                           similarity(
                             regexp_replace(name,
                               '\\s+(State Beach|City Beach|County Beach|State Park|Regional Park|Park|Pier|Beach)\\s*$',
                               '', 'i'),
                             %(norm)s)) DESC,
                  fid ASC
         LIMIT %(k)s
    """, dict(raw=beach_name, norm=norm_probe, min=CANDIDATE_MIN_SIM, k=k))
    return [dict(fid=r[0], name=r[1], county_name=r[2],
                 sim=float(r[3]), sim_norm=float(r[4])) for r in cur.fetchall()]


def county_match(gold_county: str | None, ccc_county: str | None) -> bool:
    if not ccc_county or not gold_county:
        return False
    gc, cc = gold_county.lower().strip(), ccc_county.lower().strip()
    return cc in gc or gc in cc


def apply_county_tiebreak(candidates: list[dict], ccc_county: str | None) -> list[dict]:
    """If top two candidates have sim within TIEBREAK_WINDOW AND one matches
    county, move it to the top. Returns the (possibly reordered) list."""
    if not ccc_county or len(candidates) < 2:
        return candidates
    top, second = candidates[0], candidates[1]
    if abs(top["sim"] - second["sim"]) > TIEBREAK_WINDOW:
        return candidates
    top_match = county_match(top.get("county_name"), ccc_county)
    second_match = county_match(second.get("county_name"), ccc_county)
    if second_match and not top_match:
        return [second, top] + candidates[2:]
    return candidates


def auto_match_decision(candidates: list[dict], ccc_county: str | None) -> tuple[bool, str]:
    """Return (auto_match, reason). Candidates assumed already county-tiebroken."""
    if not candidates:
        return False, "no candidates above min_sim"
    top = candidates[0]
    # Raw-name clears bar
    raw_ok = top["sim"] >= AUTO_MATCH_MIN_SIM
    # 2nd-pass: suffix-stripped sim
    norm_ok = top.get("sim_norm", 0.0) >= NORMALIZED_MIN_SIM
    if not (raw_ok or norm_ok):
        return False, (f"top sim {top['sim']:.2f} / norm {top.get('sim_norm', 0):.2f} "
                       f"below threshold ({AUTO_MATCH_MIN_SIM}/{NORMALIZED_MIN_SIM})")
    # County check (case-insensitive substring either direction)
    if ccc_county and not county_match(top.get("county_name"), ccc_county):
        return False, f"county mismatch (CCC={ccc_county!r}, gold={top['county_name']!r})"
    # Margin over runner-up (use whichever sim we matched on)
    if len(candidates) >= 2:
        top_score = max(top["sim"], top.get("sim_norm", 0.0))
        second_score = max(candidates[1]["sim"], candidates[1].get("sim_norm", 0.0))
        if (top_score - second_score) < AUTO_MATCH_MARGIN:
            return False, (f"top {top_score:.2f} not decisively above "
                           f"second {second_score:.2f} (<{AUTO_MATCH_MARGIN} margin)")
    return True, ("ok" if raw_ok else "ok (suffix-normalized)")


LLM_RESOLVE_PROMPT = """You are matching a beach mentioned on a California Coastal Commission wheelchair-access directory to a row in our internal beach catalog. Pick the single best match from the candidates, or return null if none describe the same physical beach.

CCC entry:
  beach_name: %(beach_name)s
  city:       %(city)s
  county:     %(county)s

Candidates (CA, active, ordered by trigram similarity):
%(candidates_block)s

Output ONE JSON object with exactly this shape (no prose, no markdown):
{"chosen_fid": <integer fid> | null, "reason": "<one short sentence>"}

Rules:
- Choose ONLY when the candidate clearly refers to the SAME physical beach. Adjacent / sub-areas / piers / different beaches with similar names = null.
- A candidate named e.g. "City of <X>" is the SAME as the CCC beach "<X>" if the city is on the coast and the beach is the city's signature beach.
- A candidate from a DIFFERENT county than the CCC entry is almost certainly a different beach with a coincidentally-similar name. Choose null unless you're certain.
- "Santa Monica Beach at <some venue/address>" = same as "Santa Monica Beach" (just a sub-spot).
- Suffix-only differences ("Foo Beach" vs "Foo City Beach", "Foo Beach" vs "Foo State Beach") usually mean the SAME beach when the city/county also matches.
"""


def llm_resolve_entry(entry: dict) -> tuple[int | None, str, dict]:
    """Send one CCC entry + its candidates to Haiku, return (chosen_fid|None, reason, usage_dict)."""
    cands = entry.get("candidates") or []
    if not cands:
        return None, "no candidates", {}
    cand_lines = "\n".join(
        f"  fid={c['fid']:<6d} name={c['name']!r:40s} county={c.get('county_name')!r}  sim={c['sim']:.2f}"
        for c in cands
    )
    prompt = LLM_RESOLVE_PROMPT % {
        "beach_name": entry["ccc_beach_name"],
        "city": entry.get("ccc_city") or "<not stated>",
        "county": entry.get("ccc_county") or "<not stated>",
        "candidates_block": cand_lines,
    }
    body = {
        "model": MODEL,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read())
    except Exception as e:
        return None, f"llm_error: {str(e)[:120]}", {}
    out = "".join(p["text"] for p in resp["content"] if p["type"] == "text").strip()
    if out.startswith("```"):
        out = out.split("```", 2)[1]
        if out.startswith("json\n"):
            out = out[5:]
        out = out.rstrip("` \n")
    try:
        obj = json.loads(out)
        chosen = obj.get("chosen_fid")
        reason = obj.get("reason") or ""
        if chosen is not None:
            chosen = int(chosen)
            # Sanity: chosen fid must be in the candidate set
            valid_fids = {c["fid"] for c in cands}
            if chosen not in valid_fids:
                return None, f"llm chose fid {chosen} not in candidate set", resp.get("usage", {})
        return chosen, reason, resp.get("usage", {})
    except Exception as e:
        return None, f"llm_parse_fail: {str(e)[:120]} raw={out[:120]!r}", resp.get("usage", {})


def claimed_values(entry: dict) -> dict:
    """Strip CCC-page metadata and keep only the BEP-shape ADA fields."""
    keys = ("accessible_parking", "accessible_restrooms", "path_to_sand",
            "beach_mat", "wheelchair_rental", "accessible_viewing", "notes")
    out = {}
    for k in keys:
        v = entry.get(k)
        if v is not None and v != "" and v != {}:
            out[k] = v
    return out


# ───────────────────────── Modes ─────────────────────────

def run_preview(manifest_path: Path, limit: int | None, llm_resolve: bool) -> int:
    print(f"  Fetching {CCC_URL} ...", flush=True)
    html = fetch_html(CCC_URL)
    page = bs4_strip(html)
    print(f"  page content: {len(page)} chars  (cap={MAX_INPUT_CHARS})", flush=True)

    print(f"  Calling {MODEL} (max_tokens=16000) ...", flush=True)
    t0 = time.time()
    parsed, usage = call_haiku(page)
    print(f"  LLM done in {time.time()-t0:.1f}s  in={usage.get('input_tokens')} out={usage.get('output_tokens')}", flush=True)

    entries_raw = parsed.get("entries") or []
    if limit:
        entries_raw = entries_raw[:limit]
    print(f"  entries extracted: {len(entries_raw)}", flush=True)

    manifest_entries = []
    auto_det = unmatch_count = 0

    with connect() as c, c.cursor() as cur:
        for idx, e in enumerate(entries_raw):
            beach_name = (e.get("beach_name") or "").strip()
            ccc_county = (e.get("county") or "").strip() or None
            cv = claimed_values(e)
            if not beach_name or not cv:
                continue
            cands = find_candidates(cur, beach_name, ccc_county, k=3)
            cands = apply_county_tiebreak(cands, ccc_county)
            auto_ok, reason = auto_match_decision(cands, ccc_county)
            match_fid = cands[0]["fid"] if (auto_ok and cands) else None
            if auto_ok: auto_det += 1
            else: unmatch_count += 1
            manifest_entries.append({
                "ccc_index": idx,
                "ccc_beach_name": beach_name,
                "ccc_city": e.get("city"),
                "ccc_county": ccc_county,
                "claimed_values": cv,
                "candidates": cands,
                "auto_match": auto_ok,
                "match_fid": match_fid,
                "decision_reason": reason,
                "resolved_by": "deterministic" if auto_ok else None,
            })

    # 2nd-pass: LLM-resolve the unmatched residue
    llm_resolved = 0
    llm_in_tot = llm_out_tot = 0
    if llm_resolve:
        residue = [m for m in manifest_entries if not m["auto_match"] and m["candidates"]]
        print(f"\n  LLM-resolve pass over {len(residue)} unmatched entries ...", flush=True)
        for i, me in enumerate(residue, 1):
            chosen, reason, llm_usage = llm_resolve_entry(me)
            llm_in_tot += llm_usage.get("input_tokens", 0) or 0
            llm_out_tot += llm_usage.get("output_tokens", 0) or 0
            if chosen is not None:
                me["auto_match"] = True
                me["match_fid"] = chosen
                me["decision_reason"] = f"llm_resolved: {reason}"
                me["resolved_by"] = "llm"
                # Move chosen candidate to position 0 so manifest reads naturally
                me["candidates"] = sorted(me["candidates"], key=lambda c: 0 if c["fid"] == chosen else 1)
                llm_resolved += 1
            else:
                me["decision_reason"] = f"{me['decision_reason']} | llm:{reason}"
            if i % 10 == 0 or i == len(residue):
                print(f"    [{i}/{len(residue)}]  resolved_so_far={llm_resolved}", flush=True)
        unmatch_count = sum(1 for m in manifest_entries if not m["auto_match"])

    manifest = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "source_url": CCC_URL,
        "model": MODEL,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "llm_resolve_in_tokens": llm_in_tot,
        "llm_resolve_out_tokens": llm_out_tot,
        "auto_matched_deterministic": auto_det,
        "auto_matched_llm": llm_resolved,
        "needs_review": unmatch_count,
        "total": len(manifest_entries),
        "entries": manifest_entries,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n  manifest written: {manifest_path}")
    print(f"  total={len(manifest_entries)}  deterministic={auto_det}  llm_resolved={llm_resolved}  needs_review={unmatch_count}")
    if llm_resolve:
        cost = llm_in_tot * 1 / 1e6 + llm_out_tot * 5 / 1e6
        print(f"  llm_resolve tokens={llm_in_tot}/{llm_out_tot}  est_cost=${cost:.4f}")

    print(f"\n  Auto-matched DETERMINISTIC (first 12):")
    for me in [m for m in manifest_entries if m.get("resolved_by") == "deterministic"][:12]:
        top = me["candidates"][0]
        ascii_name = me['ccc_beach_name'].encode('ascii', 'replace').decode('ascii')
        print(f"    fid={top['fid']:5d}  sim={top['sim']:.2f}  CCC={ascii_name!r:40s}  -> gold={top['name']!r}")
    print(f"\n  Auto-matched LLM (first 12):")
    for me in [m for m in manifest_entries if m.get("resolved_by") == "llm"][:12]:
        top = me["candidates"][0]
        ascii_name = me['ccc_beach_name'].encode('ascii', 'replace').decode('ascii')
        print(f"    fid={top['fid']:5d}  sim={top['sim']:.2f}  CCC={ascii_name!r:40s}  -> gold={top['name']!r}  reason={me['decision_reason'][:90]!r}")
    print(f"\n  Needs review (all {unmatch_count}):")
    for me in [m for m in manifest_entries if not m["auto_match"]]:
        ascii_name = me['ccc_beach_name'].encode('ascii', 'replace').decode('ascii')
        top = me["candidates"][0] if me["candidates"] else None
        if top:
            print(f"    CCC={ascii_name!r:40s} ({me['ccc_county']})  top fid={top['fid']} {top['name']!r} sim={top['sim']:.2f}  ({me['decision_reason'][:100]})")
        else:
            print(f"    CCC={ascii_name!r:40s} ({me['ccc_county']})  no candidates  ({me['decision_reason'][:100]})")
    return 0


def run_apply(manifest_path: Path) -> int:
    if not manifest_path.exists():
        sys.exit(f"ERROR: manifest not found at {manifest_path}. Run without --apply first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    auto_entries = [e for e in manifest["entries"] if e.get("auto_match") and e.get("match_fid")]
    print(f"  applying {len(auto_entries)} auto-matched entries (skipping {len(manifest['entries']) - len(auto_entries)} unmatched)")
    if not auto_entries:
        print("  nothing to apply.")
        return 0

    inserted = updated = 0
    with connect() as c, c.cursor() as cur:
        for me in auto_entries:
            fid = me["match_fid"]
            cv = me["claimed_values"]
            cur.execute("""
                INSERT INTO public.beach_enrichment_provenance
                  (gold_fid, field_group, source, source_url, claimed_values,
                   confidence, is_canonical, extraction_type, updated_at)
                VALUES (%s, 'accessibility', 'ccc_directory', %s, %s::jsonb,
                        0.85, false, 'derived_url_crawl', now())
                ON CONFLICT (gold_fid, field_group, source) DO UPDATE
                  SET claimed_values = excluded.claimed_values,
                      confidence     = excluded.confidence,
                      source_url     = excluded.source_url,
                      updated_at     = now()
                  WHERE beach_enrichment_provenance.claimed_values IS DISTINCT FROM excluded.claimed_values
                RETURNING (xmax = 0) AS inserted
            """, (fid, manifest["source_url"], json.dumps(cv)))
            r = cur.fetchone()
            if r is None:
                # no-op (claimed_values unchanged)
                continue
            if r[0]:
                inserted += 1
            else:
                updated += 1
        c.commit()
    print(f"  BEP write done. inserted={inserted}  updated={updated}")

    # Fire resolver + promote for accessibility — the `tg_after_change_promote_other_chain`
    # trigger runs consensus + promote but does NOT call `_resolve_field_group_gold`,
    # so is_canonical stays false and consumer surface stays empty. Pin:
    # [[non-dogs-bep-needs-manual-resolver]]. Sweep all CA fids that have CCC rows.
    print("  Firing resolver + promote sweep for accessibility ...")
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT public._resolve_field_group_gold('accessibility', NULL)")
        canonical_flipped = cur.fetchone()[0]
        cur.execute("SELECT rows_inserted, rows_updated FROM public.promote_canonical_accessibility_to_beach_amenities(NULL)")
        ins, upd = cur.fetchone()
        c.commit()
    print(f"  resolver flipped is_canonical on {canonical_flipped} rows")
    print(f"  promote_canonical_accessibility_to_beach_amenities: inserted={ins} updated={upd}")
    return 0


def _ccc_is_fresh(days: int) -> bool:
    """True if ANY ccc_directory BEP row was updated within N days."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT EXISTS(
              SELECT 1
                FROM public.beach_enrichment_provenance
               WHERE field_group = 'accessibility'
                 AND source = 'ccc_directory'
                 AND updated_at > now() - (%s || ' days')::interval
            )
        """, (days,))
        return bool(cur.fetchone()[0])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Read manifest and write BEP rows (default: preview-only)")
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST,
                    help=f"manifest path (default: {DEFAULT_MANIFEST.relative_to(ROOT)})")
    ap.add_argument("--limit", type=int, default=None,
                    help="(preview only) limit number of entries processed")
    ap.add_argument("--llm-resolve", action="store_true",
                    help="(preview only) after deterministic matching, send each unmatched "
                         "entry + top-3 candidates to Haiku for a same-beach? decision (~$0.05)")
    ap.add_argument("--skip-if-fresh-within", type=int, default=0, metavar="DAYS",
                    help="skip the run if ANY ccc_directory BEP row was updated within N days "
                         "(default 0 = always run; pipeline passes 14)")
    args = ap.parse_args()

    if args.skip_if_fresh_within > 0 and _ccc_is_fresh(args.skip_if_fresh_within):
        print(f"  SKIP — ccc_directory BEP rows fresh within {args.skip_if_fresh_within}d")
        return 0

    if args.apply:
        return run_apply(args.manifest)
    return run_preview(args.manifest, args.limit, args.llm_resolve)


if __name__ == "__main__":
    sys.exit(main())
