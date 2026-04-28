"""
cpad_dogs_playwright_retry.py
-----------------------------
Retry the geo_entity_response rows (entity_type='cpad') that have fetch_status='fetch_failed'
(JS-rendered SPAs, anti-bot pages) using a headless Chromium browser
via Playwright.

Skips PDFs — those need a PDF parser, not a browser.

Inserts a NEW row into geo_entity_response with a fresh scraped_at; the
unique constraint (entity_type, entity_id, source_url, response_scope, scraped_at)
allows the success row to co-exist with the prior failed one. The
geo_entity_response_current view picks the highest-confidence successful
row per (unit, scope), so the new success becomes canonical.

Usage:
  python scripts/one_off/cpad_dogs_playwright_retry.py [--limit N]
"""

from __future__ import annotations
import argparse, asyncio, json, os, re, subprocess, sys, tempfile
from pathlib import Path
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

MODEL          = "claude-haiku-4-5-20251001"
WORKERS        = 3   # Playwright is heavier; keep it low
USER_AGENT     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
FETCH_LIMIT    = 25_000
MIN_PAGE_CHARS = 300
RESPONSE_SCOPE = "dogs_allowed"

KEYWORDS = re.compile(r"(?i)\b(dog|dogs|pet|pets|leash|leashed|on[- ]leash|off[- ]leash|service\s+animal|canine)\b")
WIN_BEFORE, WIN_AFTER, SNIP_CAP = 200, 500, 2000

PROMPT = """\
You are reading a snippet from a beach/park webpage. Answer the single question:
ARE DOGS ALLOWED AT THIS BEACH?

Return ONLY this JSON object — no markdown, no preamble:

{
  "dogs_allowed": "yes" | "no" | "restricted" | "seasonal" | "unknown",
  "reason":      "one short clause quoting or paraphrasing the page basis",
  "confidence":  number 0.00-1.00
}
"""

def sb(): return {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}", "Content-Type": "application/json"}

def fetch_targets(limit):
    """fetch_failed rows for CPADs containing CCC sandy+named beach,
    that don't have a successful answer yet, and aren't PDFs."""
    sql = """
    with sandy_named as (
      select objectid, geom from public.ccc_access_points
      where (archived is null or archived <> 'Yes')
        and latitude is not null and longitude is not null
        and sandy_beach='Yes' and name ilike '%beach%'
    ),
    eligible as (
      select cu.unit_id, cu.unit_name
      from public.cpad_units cu
      join public.cpad_units_coastal cc using(unit_id)
      left join public.geo_entity_response_current d
        on d.entity_type='cpad' and d.entity_id=cu.unit_id and d.response_scope='dogs_allowed'
      where d.response_value is null
        and exists (select 1 from sandy_named s where st_contains(cu.geom, s.geom::geometry))
    )
    select distinct on (e.unit_id)
      e.unit_id, e.unit_name, r.source_url
    from eligible e
    join public.geo_entity_response r
      on r.entity_type = 'cpad'
     and r.entity_id = e.unit_id
     and r.response_scope = 'dogs_allowed'
     and r.fetch_status = 'fetch_failed'
    where r.source_url not ilike '%.pdf'
    order by e.unit_id, r.scraped_at desc
    """
    if limit: sql += f" limit {limit}"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(sql); tmp = f.name
    try:
        r = subprocess.run(['supabase','db','query','--linked','-f',tmp],
                           capture_output=True, text=True, timeout=120,
                           encoding='utf-8', errors='replace')
        if r.returncode != 0: print("SQL failed:", r.stderr); return []
        s, e = r.stdout.find('{'), r.stdout.rfind('}')
        return json.loads(r.stdout[s:e+1]).get('rows', [])
    finally:
        try: os.unlink(tmp)
        except: pass

def insert_row(row):
    # Strip null bytes that PostgREST can't ingest as JSON.
    if row.get("raw_text"):     row["raw_text"]     = row["raw_text"].replace("\x00", "")
    if row.get("snippet"):      row["snippet"]      = row["snippet"].replace("\x00", "")
    if row.get("response_reason"): row["response_reason"] = row["response_reason"].replace("\x00", "")
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url, headers={**sb(), "Prefer": "return=minimal"},
                   json={**row, "response_scope": RESPONSE_SCOPE, "extraction_model": MODEL}, timeout=20)
    if not r.is_success:
        print(f"  insert fail: {r.status_code} {r.text[:200]}", file=sys.stderr)

# Single shared browser for the run.
_browser = None
_browser_lock = asyncio.Lock()

async def get_browser():
    global _browser
    async with _browser_lock:
        if _browser is None:
            pw = await async_playwright().start()
            _browser = await pw.chromium.launch(headless=True)
    return _browser

async def fetch_with_playwright(url):
    browser = await get_browser()
    context = await browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 800})
    page = await context.new_page()
    try:
        resp = await page.goto(url, timeout=30_000, wait_until="domcontentloaded")
        status = resp.status if resp else 0
        if status >= 400: return None, status
        try:
            await page.wait_for_load_state("networkidle", timeout=8_000)
        except Exception:
            pass
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script","style","nav","footer","header"]): tag.decompose()
        text = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n")).strip()[:FETCH_LIMIT]
        return text, status
    except Exception as e:
        print(f"  playwright err ({url}): {type(e).__name__}", file=sys.stderr)
        return None, 0
    finally:
        await context.close()

def snippet(raw):
    hits = list(KEYWORDS.finditer(raw))
    if not hits: return "", 0
    wins = sorted([(max(0,m.start()-WIN_BEFORE), min(len(raw),m.end()+WIN_AFTER)) for m in hits])
    merged = []
    for s,e in wins:
        if merged and s <= merged[-1][1]+50: merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else: merged.append((s,e))
    parts, total = [], 0
    for s,e in merged:
        chunk = raw[s:e]
        if total + len(chunk) > SNIP_CAP: parts.append(chunk[:SNIP_CAP-total]); break
        parts.append(chunk); total += len(chunk)
    return "\n---\n".join(parts), len(hits)

async def llm(client, snip):
    payload = {"model": MODEL, "max_tokens": 200, "system": PROMPT,
               "messages": [{"role":"user","content":f"Snippet:\n{snip}"}]}
    try:
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json=payload, timeout=45.0)
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        text = re.sub(r"^```(?:json)?\s*","",text); text = re.sub(r"\s*```$","",text)
        m = re.search(r"\{.*\}", text, flags=re.S)
        return json.loads(m.group(0) if m else text)
    except Exception as e:
        print(f"  LLM err: {e}", file=sys.stderr); return None

async def process(sem, http_client, t):
    async with sem:
        cid, name, url = t["unit_id"], t["unit_name"], t["source_url"]
        raw, status = await fetch_with_playwright(url)
        base = {"entity_type": "cpad", "entity_id": cid, "source_url": url,
                "http_status": status, "raw_text": raw}
        if not raw or len(raw) < MIN_PAGE_CHARS:
            insert_row({**base, "fetch_status":"fetch_failed", "has_keywords": False})
            return f"  unit={cid:6d} {name[:35]:35s}  playwright also failed  {url}"
        snip, _ = snippet(raw)
        if not snip:
            insert_row({**base, "fetch_status":"no_keywords", "has_keywords": False})
            return f"  unit={cid:6d} {name[:35]:35s}  rendered but no_keywords"
        ans = await llm(http_client, snip)
        if not ans:
            insert_row({**base, "fetch_status":"llm_error", "has_keywords": True, "snippet": snip})
            return f"  unit={cid:6d} {name[:35]:35s}  llm_error"
        insert_row({**base, "fetch_status":"success", "has_keywords": True, "snippet": snip,
                    "response_value": ans.get("dogs_allowed"),
                    "response_reason": ans.get("reason"),
                    "response_confidence": ans.get("confidence"),
                    "extracted_at": "now()"})
        return f"  unit={cid:6d} {name[:35]:35s}  {ans.get('dogs_allowed'):11s} conf={ans.get('confidence')}"

async def run(args):
    targets = fetch_targets(args.limit)
    print(f"Loaded {len(targets)} fetch_failed rows to retry via Playwright (PDFs excluded)\n")
    if not targets: return
    sem = asyncio.Semaphore(WORKERS)
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=WORKERS)) as http:
        tasks = [process(sem, http, t) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            print(await t)
    if _browser:
        await _browser.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    asyncio.run(run(args))
