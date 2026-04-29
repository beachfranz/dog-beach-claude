"""scrape_cdpr_park_pages.py

Walk every CDPR-managed CPAD unit that touches a beach in beach_locations,
fetch its parks.ca.gov park page, and extract the structured
"Are dogs Allowed?" answer (Yes/No). Upsert into cpad_unit_dogs_policy
with default_rule mapped:
  Yes → 'restricted' (CDPR always requires leash in parks; "yes" on
         parks.ca.gov means leashed dogs allowed, not unrestricted)
  No  → 'no'
  ambiguous / not present → 'unknown'

Re-runnable. Idempotent via PK on cpad_unit_dogs_policy(cpad_unit_id).

Why this is the right level for CDPR:
  - parks.ca.gov is the canonical authority for CA state parks
  - Each park page has a structured "Are dogs Allowed?" Yes/No line
  - This bypasses LLM ambiguity (e.g., Pismo's "Pismo Dunes Natural
    Preserve" sub-area prohibition contaminating the parent beach)
  - Closes the truth-set false negatives clustered on state beaches

Usage:
  python scripts/scrape_cdpr_park_pages.py [--limit N] [--dry-run]
"""
from __future__ import annotations
import argparse, json, os, re, sys, subprocess, time
import sys as _sys
_sys.stdout.reconfigure(encoding="utf-8")
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def fetch_units():
    sql = """
      with cdpr as (
        select distinct cu.unit_id, cu.unit_name, cu.park_url
          from public.cpad_units cu
          join public.beach_locations bl on st_contains(cu.geom, bl.geom)
         where cu.agncy_name = 'California Department of Parks and Recreation'
      )
      select unit_id, unit_name, park_url
        from cdpr
       where park_url is not null and park_url <> ''
       order by unit_name;
    """
    r = subprocess.run(["supabase","db","query","--linked",sql],
                       capture_output=True, text=True, timeout=60,
                       cwd=str(Path(__file__).parent.parent))
    if r.returncode != 0:
        raise RuntimeError(f"db query failed: {r.stderr[:500]}")
    m = re.search(r'"rows"\s*:\s*(\[.*?\])\s*[},]', r.stdout, re.DOTALL)
    if not m:
        raise RuntimeError(f"could not parse rows:\n{r.stdout[:500]}")
    return json.loads(m.group(1))


PAGE_ID_RE = re.compile(r"page_id=(\d+)", re.I)


def normalize_url(park_url: str) -> str | None:
    """Convert any CDPR park_url variant to canonical https://www.parks.ca.gov/?page_id=N.
    Returns None if URL doesn't have an extractable page_id."""
    if not park_url:
        return None
    m = PAGE_ID_RE.search(park_url)
    if m:
        return f"https://www.parks.ca.gov/?page_id={m.group(1)}"
    # vanity URLs like /bigbasin — not normalizable to page_id without a lookup
    if "parks.ca.gov" in park_url and "?page_id" not in park_url:
        # Could try fetching and discovering, but skip for v1
        return None
    return None


# Look for the structured pair on the page. Format observed:
#   <some heading>Are dogs Allowed?
#   <answer>Yes  (or No)
# Spans line breaks and whitespace. Sometimes followed by extra text.
DOGS_ALLOWED_RE = re.compile(
    r"Are\s+dogs\s+Allowed\s*\?\s*\n+\s*(Yes|No)\b",
    re.I | re.S
)


def fetch_and_extract(url: str) -> tuple[str, str | None, int]:
    """Fetch with Playwright (accordion-aware), extract Yes/No.
    Returns (status, answer, page_chars). answer is "Yes"/"No"/None."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return ("playwright_unavailable", None, 0)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])
            ctx = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
            page = ctx.new_page()
            page.route("**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf,mp4,webm}",
                       lambda r: r.abort())
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try: page.wait_for_load_state("networkidle", timeout=20000)
            except Exception: pass
            page.wait_for_timeout(2000)
            page.evaluate("""() => {
                document.querySelectorAll('details').forEach(d => d.open = true);
                document.querySelectorAll('[aria-expanded="false"]').forEach(b =>
                    b.setAttribute('aria-expanded','true'));
                document.querySelectorAll('.accordion-block-panel,.accordion-panel,.accordion-content,.accordion-body,.collapse,.panel-collapse,[aria-hidden="true"]').forEach(el => {
                    el.style.display='block';el.style.visibility='visible';
                    el.style.height='auto';el.style.maxHeight='none';
                    el.removeAttribute('hidden');
                });
            }""")
            page.wait_for_timeout(500)
            text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            ctx.close(); browser.close()
        chars = len(text)
        if chars < 200:
            return ("playwright_thin", None, chars)
        m = DOGS_ALLOWED_RE.search(text)
        if not m:
            return ("ok_no_pattern", None, chars)
        return ("ok", m.group(1).strip().capitalize(), chars)
    except Exception as e:
        return (f"playwright_error:{type(e).__name__}", None, 0)


def upsert_unit_policy(unit_id: int, unit_name: str, url: str,
                       answer: str, source_quote: str):
    default_rule = "restricted" if answer.lower() == "yes" else "no"
    row = {
        "cpad_unit_id":          unit_id,
        "unit_name":             unit_name,
        "agency_name":           "California Department of Parks and Recreation",
        "url_used":              url,
        "url_kind":              "park_url",
        "default_rule":          default_rule,
        "dogs_allowed":          default_rule,
        "leash_required":        True if default_rule == "restricted" else None,
        "source_quote":          source_quote,
        "extraction_model":      "regex:are-dogs-allowed",
        "extraction_confidence": 0.95,
        # do NOT touch exceptions/time_windows/seasonal_rules/areas — leave whatever
        # the prior LLM extraction left. This scrape only refreshes the headline.
    }
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                   headers=headers, json=row, timeout=30,
                   params={"on_conflict": "cpad_unit_id"})
    r.raise_for_status()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--unit-ids", type=str, default=None,
                   help="comma-separated cpad unit_ids; run only these")
    args = p.parse_args()

    units = fetch_units()
    print(f"Loaded {len(units)} CDPR units with park_url")

    scoped = []
    for u in units:
        url = normalize_url(u.get("park_url"))
        if url is None:
            continue
        scoped.append({**u, "canonical_url": url})
    print(f"  {len(scoped)} have a canonical parks.ca.gov?page_id URL")
    if args.unit_ids:
        keep = set(int(x.strip()) for x in args.unit_ids.split(","))
        scoped = [u for u in scoped if u["unit_id"] in keep]
        print(f"  filtered to {len(scoped)} via --unit-ids")
    if args.limit:
        scoped = scoped[:args.limit]

    counts = {"yes": 0, "no": 0, "no_pattern": 0, "thin": 0, "error": 0}
    t0 = time.time()
    for i, u in enumerate(scoped, 1):
        url = u["canonical_url"]
        print(f"\n[{i}/{len(scoped)}] {u['unit_name']} (#{u['unit_id']})")
        print(f"  {url}")
        if args.dry_run:
            continue
        status, answer, chars = fetch_and_extract(url)
        print(f"  {chars} chars, status={status}, answer={answer}")
        if status == "ok" and answer in ("Yes","No"):
            # Capture a small chunk around the match for source_quote
            # (we already have the answer; quote is informational)
            quote = f"parks.ca.gov park page: 'Are dogs Allowed? {answer}'"
            upsert_unit_policy(u["unit_id"], u["unit_name"], url, answer, quote)
            counts["yes" if answer == "Yes" else "no"] += 1
        elif status == "ok_no_pattern":
            counts["no_pattern"] += 1
        elif status == "playwright_thin":
            counts["thin"] += 1
        else:
            counts["error"] += 1

    print(f"\nDone in {int(time.time()-t0)}s. {counts}")


if __name__ == "__main__":
    main()
