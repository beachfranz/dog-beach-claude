"""Extract ADA accessibility from per-state directory pages (e.g. Travel
Oregon's accessible-coast guide, future MA / WA / etc analogs).

Generalization of `extract_accessibility_from_ccc_directory.py`:
  - CCC's page was site-only (each entry = one named beach)
  - State tourism pages typically MIX site entries with city-program
    entries (e.g. "David's Chair serves Seaside" applies to every active
    beach in Seaside, not just one)

The DIRECTORIES dict at the top declares which state → URL has been
scraped. Add a new (state, url) row to register additional sources.

Flow:
  1. Fetch the configured URL, BS4-strip, send to Haiku
  2. Haiku returns array of entries; each tagged match_kind="site"|"city"
  3. Matcher:
     - site:  trigram on beaches_gold.name within state, like CCC
     - city:  fan out to ALL active fids where address_city matches
  4. Preview writes manifest at tmp/<state>_directory_matches.json
  5. --apply reads manifest, writes BEP rows with
     source="<state_lower>_directory", fires resolver+promote sweep
     (per [[non-dogs-bep-needs-manual-resolver]])

Usage:
  python scripts/extract_accessibility_from_state_directory.py --state OR
  python scripts/extract_accessibility_from_state_directory.py --state OR --llm-resolve
  python scripts/extract_accessibility_from_state_directory.py --state OR --apply
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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bs4 import BeautifulSoup

from scripts.common.db import connect
from scripts.common.llm import HAIKU

ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = ROOT / "tmp"

ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = HAIKU
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
MAX_INPUT_CHARS = 40000

# Playwright fallback for Cloudflare/403-blocked hosts (mass.gov, michigan.gov,
# etc.). Pattern ported from scripts/extract_per_beach_offleash_v2.py:181.
# Pin: [[403-means-playwright-skip-ua-tricks]].
try:
    from scripts.fetch.fetch_html import fetch as playwright_fetch  # type: ignore
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    playwright_fetch = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

# Known Cloudflare/JS-blocker hosts — go straight to Playwright, skip the
# urllib round-trip. Add new hosts as discovered.
PLAYWRIGHT_HOSTS = {
    "mass.gov", "www.mass.gov",
    "michigan.gov", "www.michigan.gov",
}

AUTO_MATCH_MIN_SIM = 0.85
AUTO_MATCH_MARGIN = 0.10
CANDIDATE_MIN_SIM = 0.30
NORMALIZED_MIN_SIM = 0.95
TIEBREAK_WINDOW = 0.05

# Registered state directory URLs. To add a state, append a row here and
# run preview → apply. The state key is the 2-letter code (uppercase).
DIRECTORIES: dict[str, dict] = {
    "OR": {
        "url": "https://traveloregon.com/things-to-do/trip-ideas/accessible-and-inclusive-travel-on-the-oregon-coast/",
        "notes": "Travel Oregon — accessible & inclusive Oregon coast guide. ~25-30 sites N-to-S; David's Chair program covers multiple cities.",
    },
    "ME": {
        "url": "https://visitmaine.com/articles/beach-accessibility/",
        "notes": "Visit Maine — Maine BPL three-star-rated beaches. 7 named beaches with mobi-mat / beach wheelchair detail.",
    },
    "HI": {
        "url": "https://www.gohawaii.com/trip-planning/accessibility",
        "notes": "Go Hawaii — accessibility guide. 10 named beaches across Oahu + Kauai with all-terrain wheelchair availability.",
    },
    "MA": {
        "url": "https://www.mass.gov/info-details/accessible-beaches",
        "notes": "Mass.gov DCR — official MA accessible beaches. State parks (Nickerson, Scusset, South Cape, Horseneck) + Cape Cod's 15 town beaches. Cloudflare-blocked → Playwright fallback.",
    },
    "MI": {
        "url": "https://www.michigan.gov/dnr/about/accessibility/track-chairs",
        "notes": "Michigan DNR — official track-chair program (25+ state-park locations). Cloudflare-blocked → Playwright fallback.",
    },
}

STRIP_SUFFIXES = (
    " State Beach", " City Beach", " County Beach",
    " State Park", " State Natural Area", " State Recreation Area",
    " State Scenic Area", " State Natural Site",
    " Regional Park", " Outstanding Natural Area",
    " Park", " Pier", " Beach",
)


def normalize_name(n: str) -> str:
    n = (n or "").strip()
    while True:
        before = n
        for sfx in STRIP_SUFFIXES:
            if n.lower().endswith(sfx.lower()) and len(n) > len(sfx):
                n = n[: -len(sfx)].strip()
                break
        if n == before:
            return n


# ───────────────────────── HTTP + LLM ─────────────────────────

def fetch_html(url: str, timeout: int = 30) -> str:
    """Fetch URL via urllib; auto-escalate to Playwright on 403/503 (Cloudflare).
    Hosts in PLAYWRIGHT_HOSTS skip urllib and go straight to Playwright.
    Pattern: scripts/extract_per_beach_offleash_v2.py:181.
    """
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    use_playwright = any(host.endswith(h.removeprefix("www.")) for h in PLAYWRIGHT_HOSTS)

    if use_playwright and PLAYWRIGHT_AVAILABLE:
        print(f"    using Playwright for known-blocked host {host!r}", flush=True)
        text = playwright_fetch(url, selector=None, raw_html=True,
                                wait_seconds=2.0, timeout_ms=45000)
        return text or ""

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (403, 503) and PLAYWRIGHT_AVAILABLE:
            print(f"    urllib {e.code} on {host!r}; escalating to Playwright", flush=True)
            text = playwright_fetch(url, selector=None, raw_html=True,
                                    wait_seconds=4.0, timeout_ms=45000)
            return text or ""
        raise


def bs4_strip(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "header", "footer", "nav"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_INPUT_CHARS]


def make_prompt(state_code: str) -> str:
    full_state = {"OR": "Oregon", "WA": "Washington", "MA": "Massachusetts",
                  "MI": "Michigan", "HI": "Hawaii", "ME": "Maine", "FL": "Florida",
                  "AL": "Alabama", "MD": "Maryland", "DE": "Delaware",
                  "NH": "New Hampshire", "RI": "Rhode Island", "VA": "Virginia",
                  "OH": "Ohio"}.get(state_code, state_code)
    return f"""Extract every beach, park, coastal site, or accessibility program mentioned on this {full_state} accessible-coast directory.

Output ONE JSON object with key "entries" mapping to an array. ONE element per distinct site OR per city-program. Use null for any field not explicitly stated for THAT entry.

Each entry:

{{
  "match_kind":          "site" | "city",
  "site_name":           "<proper-noun name as stated>" | null,
  "city":                "<city if mentioned>" | null,
  "county":              "<{full_state} county if mentioned>" | null,
  "accessible_parking":  true | false | null,
  "accessible_restrooms": true | false | null,
  "path_to_sand":        "boardwalk" | "ramp" | "mat" | "stairs_only" | "none" | null,
  "beach_mat":           true | false | null,
  "wheelchair_rental":   {{"available": true, "provider": "<org name>", "phone": "<phone>" | null, "url": "<URL>" | null}} | null,
  "accessible_viewing":  true | false | null,
  "notes":               "<short concrete detail>" | null
}}

match_kind rules:
- "site": the entry names a SPECIFIC beach or park (e.g. "Beverly Beach State Park", "Yaquina Head", "Cape Perpetua"). site_name MUST be set; city may also be set if stated.
- "city": the entry describes a roving / city-wide program (e.g. "Seaside - David's Chair", "Florence - Mobi-mats") without naming a single beach. site_name MUST be null; city MUST be set.

Field rules:
- "wheelchair_rental.provider": org/program name (e.g. "David's Chair", "Adventures Without Limits", a city Parks & Rec). url/phone only if explicitly stated.
- "beach_mat": true ONLY if a sand mat / Mobi-Mat / accessible-path-onto-sand is mentioned for THIS entry.
- "path_to_sand": pick ONE if a specific path type is named ("mat" for sand mats, "boardwalk" for wood paths, "ramp" for paved ramps).
- "notes": short concrete details that won't be captured elsewhere ("year-round track-chair program", "paved 3/4-mile path to lighthouse"). Keep under 200 chars. Do NOT repeat fields above.
- Skip the introductory / navigation / aboutus paragraphs — only site/city-level accessibility entries.
- DO NOT INFER. DO NOT HALLUCINATE. If text only says "accessible beach access" without specifics, skip that entry.

Output ONLY the JSON object. Compact JSON (no pretty-print). No commentary, no markdown code fence.

Text:
%s
"""


def call_haiku(text: str, state_code: str) -> tuple[dict, dict]:
    body = {
        "model": MODEL,
        "max_tokens": 24000,
        "messages": [{"role": "user", "content": make_prompt(state_code) % text}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(), method="POST",
        headers={"x-api-key": ANTHROPIC_KEY,
                 "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=180) as r:
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

def find_site_candidates(cur, beach_name: str, state: str, county: str | None, k: int = 3) -> list[dict]:
    """State-filtered trigram on name; sim_norm via suffix-stripped form."""
    norm_probe = normalize_name(beach_name)
    cur.execute("""
        SELECT fid, name, county_name, address_city,
               round(similarity(name, %(raw)s)::numeric, 3)        AS sim,
               round(similarity(
                   regexp_replace(name,
                     '\\s+(State Beach|City Beach|County Beach|State Park|State Natural Area|State Recreation Area|State Scenic Area|State Natural Site|Regional Park|Outstanding Natural Area|Park|Pier|Beach)\\s*$',
                     '', 'i'),
                   %(norm)s
               )::numeric, 3) AS sim_norm
          FROM beaches_gold
         WHERE state = %(st)s AND is_active
           AND (similarity(name, %(raw)s) >= %(min)s
                OR similarity(
                     regexp_replace(name,
                       '\\s+(State Beach|City Beach|County Beach|State Park|State Natural Area|State Recreation Area|State Scenic Area|State Natural Site|Regional Park|Outstanding Natural Area|Park|Pier|Beach)\\s*$',
                       '', 'i'),
                     %(norm)s) >= %(min)s)
         ORDER BY GREATEST(similarity(name, %(raw)s),
                           similarity(
                             regexp_replace(name,
                               '\\s+(State Beach|City Beach|County Beach|State Park|State Natural Area|State Recreation Area|State Scenic Area|State Natural Site|Regional Park|Outstanding Natural Area|Park|Pier|Beach)\\s*$',
                               '', 'i'),
                             %(norm)s)) DESC,
                  fid ASC
         LIMIT %(k)s
    """, dict(st=state, raw=beach_name, norm=norm_probe, min=CANDIDATE_MIN_SIM, k=k))
    return [dict(fid=r[0], name=r[1], county_name=r[2], address_city=r[3],
                 sim=float(r[4]), sim_norm=float(r[5])) for r in cur.fetchall()]


def find_city_fids(cur, city: str, state: str) -> list[dict]:
    """Return ALL active beach fids in this address_city within state."""
    cur.execute("""
        SELECT fid, name, county_name, address_city
          FROM beaches_gold
         WHERE state = %s AND is_active
           AND address_city ILIKE %s
         ORDER BY fid
    """, (state, city))
    return [dict(fid=r[0], name=r[1], county_name=r[2], address_city=r[3])
            for r in cur.fetchall()]


def county_match(gold_county: str | None, probe_county: str | None) -> bool:
    if not probe_county or not gold_county:
        return False
    gc, pc = gold_county.lower().strip(), probe_county.lower().strip()
    return pc in gc or gc in pc


def apply_county_tiebreak(candidates: list[dict], probe_county: str | None) -> list[dict]:
    if not probe_county or len(candidates) < 2:
        return candidates
    top, second = candidates[0], candidates[1]
    if abs(top["sim"] - second["sim"]) > TIEBREAK_WINDOW:
        return candidates
    top_m = county_match(top.get("county_name"), probe_county)
    second_m = county_match(second.get("county_name"), probe_county)
    if second_m and not top_m:
        return [second, top] + candidates[2:]
    return candidates


def auto_match_site(candidates: list[dict], ccc_county: str | None) -> tuple[bool, str]:
    if not candidates:
        return False, "no candidates above min_sim"
    top = candidates[0]
    raw_ok = top["sim"] >= AUTO_MATCH_MIN_SIM
    norm_ok = top.get("sim_norm", 0.0) >= NORMALIZED_MIN_SIM
    if not (raw_ok or norm_ok):
        return False, (f"top sim {top['sim']:.2f} / norm {top.get('sim_norm', 0):.2f} "
                       f"below threshold ({AUTO_MATCH_MIN_SIM}/{NORMALIZED_MIN_SIM})")
    if ccc_county and not county_match(top.get("county_name"), ccc_county):
        return False, f"county mismatch (probe={ccc_county!r}, gold={top['county_name']!r})"
    if len(candidates) >= 2:
        top_score = max(top["sim"], top.get("sim_norm", 0.0))
        second_score = max(candidates[1]["sim"], candidates[1].get("sim_norm", 0.0))
        if (top_score - second_score) < AUTO_MATCH_MARGIN:
            return False, (f"top {top_score:.2f} not decisively above "
                           f"second {second_score:.2f} (<{AUTO_MATCH_MARGIN} margin)")
    return True, ("ok" if raw_ok else "ok (suffix-normalized)")


LLM_RESOLVE_PROMPT = """You are matching a site mentioned on a state accessible-coast directory to a row in our internal beach catalog. Pick the single best match from the candidates, or return null if none describe the same physical site.

Directory entry:
  site_name: %(site_name)s
  city:      %(city)s
  county:    %(county)s

Candidates (active, ordered by trigram similarity):
%(candidates_block)s

Output ONE JSON object with exactly this shape (no prose, no markdown):
{"chosen_fid": <integer fid> | null, "reason": "<one short sentence>"}

Rules:
- Choose ONLY when the candidate clearly refers to the SAME physical site. Adjacent areas / sub-units / different sites with similar names = null.
- Suffix-only differences ("Foo Beach" vs "Foo State Beach", "Foo Park" vs "Foo Park & Recreation Area") usually mean the SAME site.
- A "City of <X>" or "<X> Municipal" gold row IS the same as the directory "<X>" entry when the city is coastal and the directory implies the city's signature beach.
- "Yaquina Head Outstanding Natural Area" matches a gold "Yaquina Head ..." row by any suffix.
"""


def llm_resolve_entry(entry: dict) -> tuple[int | None, str, dict]:
    cands = entry.get("candidates") or []
    if not cands:
        return None, "no candidates", {}
    cand_lines = "\n".join(
        f"  fid={c['fid']:<6d} name={c['name']!r:50s} county={c.get('county_name')!r:18s} city={c.get('address_city')!r:18s} sim={c['sim']:.2f}"
        for c in cands
    )
    prompt = LLM_RESOLVE_PROMPT % {
        "site_name": entry["site_name"],
        "city": entry.get("city") or "<not stated>",
        "county": entry.get("county") or "<not stated>",
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
            valid_fids = {c["fid"] for c in cands}
            if chosen not in valid_fids:
                return None, f"llm chose fid {chosen} not in candidate set", resp.get("usage", {})
        return chosen, reason, resp.get("usage", {})
    except Exception as e:
        return None, f"llm_parse_fail: {str(e)[:120]} raw={out[:120]!r}", resp.get("usage", {})


# ───────────────────────── Payload + write ─────────────────────────

def claimed_values(entry: dict) -> dict:
    keys = ("accessible_parking", "accessible_restrooms", "path_to_sand",
            "beach_mat", "wheelchair_rental", "accessible_viewing", "notes")
    out = {}
    for k in keys:
        v = entry.get(k)
        if v is not None and v != "" and v != {}:
            out[k] = v
    return out


# ───────────────────────── Modes ─────────────────────────

def run_preview(state: str, manifest_path: Path, llm_resolve: bool) -> int:
    cfg = DIRECTORIES.get(state.upper())
    if not cfg:
        sys.exit(f"ERROR: no DIRECTORIES entry for state={state}. Add (state, url) row.")
    url = cfg["url"]

    print(f"  state={state}  url={url}", flush=True)
    print(f"  Fetching ...", flush=True)
    html = fetch_html(url)
    page = bs4_strip(html)
    print(f"  page content: {len(page)} chars (cap={MAX_INPUT_CHARS})", flush=True)

    print(f"  Calling {MODEL} ...", flush=True)
    t0 = time.time()
    parsed, usage = call_haiku(page, state)
    print(f"  LLM done in {time.time()-t0:.1f}s  in={usage.get('input_tokens')} out={usage.get('output_tokens')}", flush=True)

    entries_raw = parsed.get("entries") or []
    print(f"  entries extracted: {len(entries_raw)}", flush=True)

    site_entries = []
    city_entries = []
    auto_det = unmatch = 0

    with connect() as c, c.cursor() as cur:
        for idx, e in enumerate(entries_raw):
            cv = claimed_values(e)
            if not cv:
                continue
            kind = e.get("match_kind")
            if kind == "city":
                city = (e.get("city") or "").strip()
                if not city:
                    continue
                fids_rows = find_city_fids(cur, city, state.upper())
                city_entries.append({
                    "idx": idx,
                    "match_kind": "city",
                    "city": city,
                    "claimed_values": cv,
                    "fid_count": len(fids_rows),
                    "fids": [r["fid"] for r in fids_rows],
                    "fid_preview": [{"fid": r["fid"], "name": r["name"]} for r in fids_rows[:5]],
                })
            else:
                # site
                site_name = (e.get("site_name") or "").strip()
                probe_county = (e.get("county") or "").strip() or None
                if not site_name:
                    continue
                cands = find_site_candidates(cur, site_name, state.upper(), probe_county, k=3)
                cands = apply_county_tiebreak(cands, probe_county)
                auto_ok, reason = auto_match_site(cands, probe_county)
                site_entries.append({
                    "idx": idx,
                    "match_kind": "site",
                    "site_name": site_name,
                    "city": e.get("city"),
                    "county": probe_county,
                    "claimed_values": cv,
                    "candidates": cands,
                    "auto_match": auto_ok,
                    "match_fid": cands[0]["fid"] if (auto_ok and cands) else None,
                    "decision_reason": reason,
                    "resolved_by": "deterministic" if auto_ok else None,
                })
                if auto_ok:
                    auto_det += 1
                else:
                    unmatch += 1

    # LLM-resolve site entries that didn't auto-match
    llm_resolved = 0
    llm_in_tot = llm_out_tot = 0
    if llm_resolve:
        residue = [m for m in site_entries if not m["auto_match"] and m["candidates"]]
        print(f"\n  LLM-resolve pass over {len(residue)} unmatched site entries ...", flush=True)
        for i, me in enumerate(residue, 1):
            chosen, reason, llm_usage = llm_resolve_entry(me)
            llm_in_tot += llm_usage.get("input_tokens", 0) or 0
            llm_out_tot += llm_usage.get("output_tokens", 0) or 0
            if chosen is not None:
                me["auto_match"] = True
                me["match_fid"] = chosen
                me["decision_reason"] = f"llm_resolved: {reason}"
                me["resolved_by"] = "llm"
                me["candidates"] = sorted(me["candidates"], key=lambda c: 0 if c["fid"] == chosen else 1)
                llm_resolved += 1
            else:
                me["decision_reason"] = f"{me['decision_reason']} | llm:{reason}"
            if i % 10 == 0 or i == len(residue):
                print(f"    [{i}/{len(residue)}]  resolved_so_far={llm_resolved}", flush=True)
        unmatch = sum(1 for m in site_entries if not m["auto_match"])

    city_fid_total = sum(c["fid_count"] for c in city_entries)
    manifest = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "state": state.upper(),
        "source_url": url,
        "model": MODEL,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "llm_resolve_in_tokens": llm_in_tot,
        "llm_resolve_out_tokens": llm_out_tot,
        "site_entries_total": len(site_entries),
        "site_auto_deterministic": auto_det,
        "site_auto_llm": llm_resolved,
        "site_needs_review": unmatch,
        "city_entries_total": len(city_entries),
        "city_fid_total": city_fid_total,
        "site_entries": site_entries,
        "city_entries": city_entries,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n  manifest written: {manifest_path}")
    print(f"  SITE entries:  total={len(site_entries)}  det={auto_det}  llm={llm_resolved}  need_review={unmatch}")
    print(f"  CITY entries:  total={len(city_entries)}  total_fids_covered={city_fid_total}")
    if llm_resolve:
        cost = llm_in_tot * 1 / 1e6 + llm_out_tot * 5 / 1e6
        print(f"  llm_resolve tokens={llm_in_tot}/{llm_out_tot}  est_cost=${cost:.4f}")

    print(f"\n  SITE auto-matched (first 10):")
    for me in [m for m in site_entries if m["auto_match"]][:10]:
        top = me["candidates"][0]
        ascii_name = (me['site_name'] or '').encode('ascii', 'replace').decode('ascii')
        print(f"    fid={top['fid']:5d}  sim={top['sim']:.2f}  src={ascii_name!r:45s}  -> {top['name']!r}  ({me.get('resolved_by')})")
    print(f"\n  CITY entries (all):")
    for ce in city_entries:
        print(f"    city={ce['city']!r:20s}  {ce['fid_count']} fids  cv_keys={list(ce['claimed_values'].keys())}")
    print(f"\n  SITE needs review ({unmatch}):")
    for me in [m for m in site_entries if not m["auto_match"]]:
        ascii_name = (me['site_name'] or '').encode('ascii', 'replace').decode('ascii')
        top = me["candidates"][0] if me["candidates"] else None
        if top:
            print(f"    src={ascii_name!r:40s}  top={top['name']!r} sim={top['sim']:.2f}  ({me['decision_reason'][:100]})")
        else:
            print(f"    src={ascii_name!r:40s}  no candidates  ({me['decision_reason'][:100]})")
    return 0


def run_apply(state: str, manifest_path: Path) -> int:
    if not manifest_path.exists():
        sys.exit(f"ERROR: manifest not found at {manifest_path}. Run preview first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    state_code = manifest["state"]
    source = f"{state_code.lower()}_directory"
    source_url = manifest["source_url"]

    site_matched = [m for m in manifest["site_entries"] if m.get("auto_match") and m.get("match_fid")]
    city_entries = manifest.get("city_entries", [])

    fid_count = len(site_matched) + sum(c["fid_count"] for c in city_entries)
    print(f"  applying source={source!r}")
    print(f"  SITE rows to write: {len(site_matched)} (skipping {len(manifest['site_entries']) - len(site_matched)} unmatched)")
    print(f"  CITY entries: {len(city_entries)} covering {sum(c['fid_count'] for c in city_entries)} fids")
    print(f"  total BEP row writes: {fid_count}")

    inserted = updated = 0
    with connect() as c, c.cursor() as cur:
        # Site entries: one row per match_fid
        for me in site_matched:
            cur.execute("""
                INSERT INTO public.beach_enrichment_provenance
                  (gold_fid, field_group, source, source_url, claimed_values,
                   confidence, is_canonical, extraction_type, updated_at)
                VALUES (%s, 'accessibility', %s, %s, %s::jsonb,
                        0.82, false, 'derived_url_crawl', now())
                ON CONFLICT (gold_fid, field_group, source) DO UPDATE
                  SET claimed_values = excluded.claimed_values,
                      confidence     = excluded.confidence,
                      source_url     = excluded.source_url,
                      updated_at     = now()
                  WHERE beach_enrichment_provenance.claimed_values IS DISTINCT FROM excluded.claimed_values
                RETURNING (xmax = 0) AS inserted
            """, (me["match_fid"], source, source_url, json.dumps(me["claimed_values"])))
            r = cur.fetchone()
            if r is None: continue
            if r[0]: inserted += 1
            else: updated += 1
        # City entries: one row per fanned-out fid
        for ce in city_entries:
            for fid in ce["fids"]:
                cur.execute("""
                    INSERT INTO public.beach_enrichment_provenance
                      (gold_fid, field_group, source, source_url, claimed_values,
                       confidence, is_canonical, extraction_type, updated_at)
                    VALUES (%s, 'accessibility', %s, %s, %s::jsonb,
                            0.78, false, 'derived_url_crawl', now())
                    ON CONFLICT (gold_fid, field_group, source) DO UPDATE
                      SET claimed_values = excluded.claimed_values,
                          confidence     = excluded.confidence,
                          source_url     = excluded.source_url,
                          updated_at     = now()
                      WHERE beach_enrichment_provenance.claimed_values IS DISTINCT FROM excluded.claimed_values
                    RETURNING (xmax = 0) AS inserted
                """, (fid, source, source_url, json.dumps(ce["claimed_values"])))
                r = cur.fetchone()
                if r is None: continue
                if r[0]: inserted += 1
                else: updated += 1
        c.commit()
    print(f"  BEP done. inserted={inserted} updated={updated}")

    # Fire resolver + promote sweep (cascade gap: trigger doesn't auto-resolve
    # non-dogs field_groups). See [[non-dogs-bep-needs-manual-resolver]].
    print("  Firing resolver + promote sweep for accessibility ...")
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT public._resolve_field_group_gold('accessibility', NULL)")
        flipped = cur.fetchone()[0]
        cur.execute("SELECT rows_inserted, rows_updated FROM public.promote_canonical_accessibility_to_beach_amenities(NULL)")
        ins, upd = cur.fetchone()
        c.commit()
    print(f"  resolver: {flipped} canonical rows | promote: ins={ins} upd={upd}")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="2-letter state code; must be a key in DIRECTORIES")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--llm-resolve", action="store_true",
                    help="(preview only) Haiku second-pass on unmatched site entries")
    ap.add_argument("--manifest", type=Path, default=None,
                    help="manifest override (default: tmp/<state>_directory_matches.json)")
    args = ap.parse_args()

    state = args.state.upper()
    manifest_path = args.manifest or (TMP_DIR / f"{state.lower()}_directory_matches.json")

    if args.apply:
        return run_apply(state, manifest_path)
    return run_preview(state, manifest_path, args.llm_resolve)


if __name__ == "__main__":
    sys.exit(main())
