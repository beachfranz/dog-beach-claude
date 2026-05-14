"""extract_agency_master.py - Federal/state agency-wide dog policy extraction.

Replaces the 11 hand-seeded rows in pad_us_unit_dogs_policy (unit_id IS NULL)
with researched + cited equivalents. One row per PAD-US mng_name code,
populated by LLM extraction of the agency's official pet-policy page.

This is GLOBAL infrastructure - runs once, cached for ~90 days. Reuses the
Pass A pattern from extract_operator_dogs_policy.py.

Usage:
  python scripts/extract_agency_master.py               # extract all
  python scripts/extract_agency_master.py --codes NPS,USFWS,USFS
  python scripts/extract_agency_master.py --skip-recent 90  # default
  python scripts/extract_agency_master.py --skip-recent 0   # force re-extract
"""
from __future__ import annotations
import truststore; truststore.inject_into_ssl()
import argparse, json, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / "pipeline" / ".env")

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
TAVILY_KEY    = os.environ.get("TAVILY_API_KEY")
HAIKU = "claude-haiku-4-5-20251001"
CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


# Hand-curated agency seed list. URLs are best-known authoritative sources.
# If a URL 404s, Tavily fallback finds a substitute.
AGENCIES = [
    # ─── federal ─────────────────────────────────────────────────────
    ("NPS",   "National Park Service",
        "https://www.nps.gov/aboutus/pets-in-parks.htm",
        "36 CFR §2.15"),
    ("USFWS", "U.S. Fish and Wildlife Service",
        "https://www.fws.gov/library/collections/refuge-recreation",
        "50 CFR §26.21"),
    ("FWS",   "U.S. Fish and Wildlife Service",  # alias
        "https://www.fws.gov/library/collections/refuge-recreation",
        "50 CFR §26.21"),
    ("USFS",  "U.S. Forest Service",
        "https://www.fs.usda.gov/visit/know-before-you-go/pets",
        "36 CFR §261.16"),
    ("USACE", "U.S. Army Corps of Engineers",
        "https://www.usace.army.mil/Missions/Civil-Works/Recreation/",
        "ER 1130-2-550"),
    ("BLM",   "Bureau of Land Management",
        "https://www.blm.gov/programs/recreation",
        "43 CFR §8365.1-6"),
    ("USBR",  "Bureau of Reclamation",
        "https://www.usbr.gov/recreation/",
        "43 CFR §423"),
    ("BIA",   "Bureau of Indian Affairs",
        "https://www.bia.gov/bia/ots",
        "25 CFR §11"),
    ("DOD",   "Department of Defense",
        "https://www.defense.gov/",
        "AR 600-25 / DOD Instruction 6055.04"),
    ("NOAA",  "National Oceanic and Atmospheric Administration",
        "https://sanctuaries.noaa.gov/visit/recreation.html",
        "15 CFR §922"),
    ("BOEM",  "Bureau of Ocean Energy Management",
        "https://www.boem.gov/",
        "30 CFR §550"),
    ("NRCS",  "Natural Resources Conservation Service",
        "https://www.nrcs.usda.gov/",
        "7 CFR §622"),
    ("TVA",   "Tennessee Valley Authority",
        "https://www.tva.com/environment/recreation",
        "18 CFR §1304"),
    ("BPA",   "Bonneville Power Administration",
        "https://www.bpa.gov/",
        "18 CFR §10"),
    ("ARS",   "Agricultural Research Service",
        "https://www.ars.usda.gov/",
        "7 CFR §550"),
    # ─── state (PAD-US codes) ────────────────────────────────────────
    ("SPR",   "State Parks and Recreation (per-state agency)",
        None,  # state-specific URL; Tavily will find per-state
        "varies by state - see state_dogs_policy"),
    ("SFW",   "State Fish and Wildlife (per-state agency)",
        None,
        "varies by state"),
    ("SDNR",  "State Department of Natural Resources",
        None,
        "varies by state"),
    # ─── local ───────────────────────────────────────────────────────
    ("CITY",  "Municipal government",  None, "varies by city"),
    ("CNTY",  "County government",     None, "varies by county"),
    ("REG",   "Regional / special-district government",  None, "varies"),
    ("DIST",  "Special district",      None, "varies"),
    # ─── non-government ──────────────────────────────────────────────
    ("TRIB",  "Tribal lands",          None, "varies by tribe"),
    ("PVT",   "Private land",          None, "no public policy"),
    ("NGO",   "Non-governmental land trust",  None, "varies"),
    ("JNT",   "Joint management",      None, "varies"),
    # ─── catch-alls ──────────────────────────────────────────────────
    ("OTHF",  "Other federal",         None, "varies"),
    ("OTHS",  "Other state",           None, "varies"),
    ("UNK",   "Unknown manager",       None, "unknown"),
]


def tavily_search(query: str, max_results: int = 8) -> list[dict]:
    """Tavily search. Returns list of {url, title, content}."""
    if not TAVILY_KEY:
        return []
    try:
        r = httpx.post("https://api.tavily.com/search",
                       json={"api_key": TAVILY_KEY, "query": query,
                             "max_results": max_results,
                             "search_depth": "basic"},
                       timeout=20)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception:
        return []


def pick_agency_url(agency_name: str, default_cfr: str, candidates: list[dict]) -> str | None:
    """LLM picks the most authoritative agency-wide pet-policy URL from search hits."""
    if not candidates:
        return None
    listing = "\n".join(
        f"{i+1}. {c.get('url','')}\n   title: {c.get('title','')[:140]}\n   snip: {(c.get('content') or '')[:200]}"
        for i, c in enumerate(candidates[:8])
    )
    sys_p = "You pick the single most authoritative URL describing an agency's WHOLE-AGENCY pet/dog policy."
    user_p = (
        f"Agency: {agency_name}\n"
        f"Citation hint: {default_cfr}\n\n"
        "Candidate URLs:\n" + listing + "\n\n"
        "Return ONLY a JSON object: {\"pick_index\": <1-based int>, \"reason\": \"<short>\"}\n"
        "Pick the URL that most directly states the agency's default dog/pet rule. "
        "Prefer .gov over .org. Prefer pages titled 'pets', 'pet policy', 'pet rules', "
        "'regulations' over generic recreation portals. Return pick_index=0 if no candidate fits."
    )
    r = call_haiku(sys_p, user_p, max_tokens=256)
    if "error" in r:
        return None
    idx = r.get("json", {}).get("pick_index", 0)
    if not isinstance(idx, int) or idx < 1 or idx > len(candidates):
        return None
    return candidates[idx - 1].get("url")


def fetch_page(url: str, timeout: int = 30) -> str:
    """Fetch + plain-text-extract via Tavily extract endpoint.
    Bypasses WAFs / browser-only restrictions on gov sites. Falls back to
    direct httpx GET if Tavily fails."""
    if TAVILY_KEY:
        try:
            r = httpx.post("https://api.tavily.com/extract",
                           json={"api_key": TAVILY_KEY, "urls": [url]},
                           timeout=timeout)
            r.raise_for_status()
            body = r.json()
            results = body.get("results", [])
            if results:
                raw = results[0].get("raw_content", "") or ""
                if raw.strip():
                    return re.sub(r"\n{3,}", "\n\n", raw)[:12000]
        except Exception:
            pass
    # Direct fetch fallback
    try:
        r = httpx.get(url, headers={"User-Agent": CHROME_UA},
                      follow_redirects=True, timeout=timeout)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = re.sub(r"\n{3,}", "\n\n", soup.get_text(separator="\n"))
        return text[:12000]
    except Exception:
        return ""


def _extract_first_json_object(text: str) -> str | None:
    """Find first balanced {...} JSON object. Tolerates trailing commentary."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def call_haiku(system: str, user: str, max_tokens: int = 1024) -> dict:
    """One-shot Anthropic Haiku call. Returns {json, raw} or {error, raw}."""
    try:
        r = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY,
                     "anthropic-version": "2023-06-01",
                     "Content-Type": "application/json"},
            json={"model": HAIKU, "max_tokens": max_tokens,
                  "system": system,
                  "messages": [{"role": "user", "content": user}]},
            timeout=60,
        )
        r.raise_for_status()
        body = r.json()
        text = body["content"][0]["text"]
        json_slice = _extract_first_json_object(text)
        if json_slice is None:
            return {"error": "no json object found", "raw": text}
        try:
            return {"json": json.loads(json_slice), "raw": text}
        except json.JSONDecodeError as e:
            return {"error": str(e), "raw": text}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "raw": ""}


SYSTEM_PROMPT = (
    "You analyze official agency policy pages to extract the agency-wide "
    "default rule for pet/dog access on lands the agency manages."
)


def build_prompt(canonical_name: str, page_text: str, default_cfr: str) -> str:
    return f"""Agency: {canonical_name}
Default CFR citation (use this unless the page cites something more specific): {default_cfr}

Page text:
<page>
{page_text}
</page>

Extract the agency-WIDE default dog policy from the page text. Output ONLY:
{{
  "dogs_allowed": "yes" | "no" | "mixed" | "unknown",
  "default_rule": "yes" | "no" | "mixed" | "unknown",
  "leash_required": <bool|null>,
  "ordinance_reference": "<text>",
  "source_quote": "<verbatim text from page>",
  "confidence": <0.0-1.0>
}}

Rules:
- "yes" = dogs allowed without conditions on most agency lands
- "no" = dogs prohibited as a default (unconditional)
- "mixed" = allowed with conditions (leash, certain zones, etc.)
- "unknown" = page doesn't address dogs or page wasn't useful
- leash_required: true if the page says leash required when dogs are present
- ordinance_reference: preserve the default CFR unless the page provides a more specific one
- source_quote: the verbatim text snippet you based the answer on (one sentence)
"""


def upsert_row(mng_name: str, payload: dict, url: str, cfr: str):
    """Upsert pad_us_unit_dogs_policy where unit_id IS NULL AND mng_name=mng_name."""
    j = payload.get("json") or {}
    headers = {"apikey": SERVICE_KEY,
               "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json"}
    # PostgREST upsert pattern requires a unique constraint - fall back to
    # check-then-insert/update
    chk = httpx.get(
        f"{SUPABASE_URL}/rest/v1/pad_us_unit_dogs_policy",
        headers=headers,
        params={"unit_id": "is.null", "mng_name": f"eq.{mng_name}",
                "select": "id", "limit": "1"},
        timeout=15,
    )
    existing_id = (chk.json() or [None])[0]
    row = {
        "mng_name":              mng_name,
        "unit_id":               None,
        "dogs_allowed":          j.get("dogs_allowed"),
        "default_rule":          j.get("default_rule"),
        "leash_required":        j.get("leash_required"),
        "ordinance_ref":         j.get("ordinance_reference") or cfr,
        "source_quote":          j.get("source_quote"),
        "url_used":              url,
        "url_kind":              "agncy_web",
        "extraction_model":      HAIKU,
        "extraction_confidence": j.get("confidence"),
        "scraped_at":            datetime.now(timezone.utc).isoformat(),
        "notes":                 "agency-wide default - auto-extracted by extract_agency_master.py",
    }
    if existing_id and isinstance(existing_id, dict):
        rid = existing_id.get("id")
        httpx.patch(f"{SUPABASE_URL}/rest/v1/pad_us_unit_dogs_policy",
                    headers=headers, params={"id": f"eq.{rid}"},
                    json=row, timeout=15).raise_for_status()
        return "updated"
    else:
        httpx.post(f"{SUPABASE_URL}/rest/v1/pad_us_unit_dogs_policy",
                   headers=headers, json=row, timeout=15).raise_for_status()
        return "inserted"


def recently_extracted(mng_name: str, days: int) -> bool:
    if days <= 0:
        return False
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"}
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/pad_us_unit_dogs_policy",
        headers=headers,
        params={"mng_name": f"eq.{mng_name}", "unit_id": "is.null",
                "select": "scraped_at", "limit": "1"},
        timeout=15,
    )
    if not r.is_success:
        return False
    rows = r.json() or []
    if not rows:
        return False
    scraped = rows[0].get("scraped_at")
    if not scraped:
        return False
    try:
        from datetime import datetime as dt
        s = dt.fromisoformat(scraped.replace("Z","+00:00"))
        return (dt.now(timezone.utc) - s).total_seconds() / 86400 < days
    except Exception:
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--codes", type=str, default=None,
                   help="comma-separated mng_name codes; default = all known")
    p.add_argument("--skip-recent", type=int, default=90,
                   help="skip agencies extracted within last N days (default 90)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    requested = set([c.strip().upper() for c in args.codes.split(",")]) if args.codes else None
    counts = {"extracted": 0, "skipped_recent": 0, "skipped_no_url": 0,
              "skipped_unrequested": 0, "errors": 0}

    for code, name, url, cfr in AGENCIES:
        if requested and code not in requested:
            counts["skipped_unrequested"] += 1
            continue
        if recently_extracted(code, args.skip_recent):
            print(f"  {code}: cached (within {args.skip_recent}d), skip")
            counts["skipped_recent"] += 1
            continue
        if not url:
            print(f"  {code} ({name}): no fixed URL - skipped "
                  "(state/local/per-tribe defaults handled via state_dogs_policy or operator extraction)")
            counts["skipped_no_url"] += 1
            continue

        print(f"\n[{code}] {name}")
        # (1) Try hardcoded URL first
        page = fetch_page(url)
        if not page:
            # (2) Tavily search + LLM pick
            print(f"  hardcoded URL empty, searching Tavily")
            hits = tavily_search(f"{name} pet dog policy regulations CFR")
            picked = pick_agency_url(name, cfr, hits)
            if picked:
                print(f"  picked: {picked}")
                page = fetch_page(picked)
                if page:
                    url = picked   # use the discovered URL going forward
        if not page:
            print(f"  [!] no page content via direct or Tavily - skip ({code})")
            counts["errors"] += 1
            continue
        print(f"  using URL: {url}")
        prompt = build_prompt(name, page, cfr)
        if args.dry_run:
            print(f"  [dry-run] would extract {len(page)} chars of page text")
            continue
        result = call_haiku(SYSTEM_PROMPT, prompt)
        if "error" in result:
            print(f"  [X] LLM error: {result['error']}")
            counts["errors"] += 1
            continue
        j = result["json"]
        print(f"  dogs_allowed={j.get('dogs_allowed')}  default_rule={j.get('default_rule')}  "
              f"leash={j.get('leash_required')}  conf={j.get('confidence')}")
        print(f"  citation: {j.get('ordinance_reference') or cfr}")
        try:
            action = upsert_row(code, result, url, cfr)
            print(f"  [+] {action}")
            counts["extracted"] += 1
        except Exception as e:
            print(f"  [X] upsert: {e}")
            counts["errors"] += 1

    print(f"\nDone. {counts}")


if __name__ == "__main__":
    main()
