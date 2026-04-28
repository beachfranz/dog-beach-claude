"""Targeted dogs_allowed extraction: CPADs containing a CCC sandy +
named-beach point and lacking any current dogs_allowed answer.

Wraps the same fetch/snippet/LLM pipeline as cpad_dogs_question.py but
swaps the target query. Reads cached raw_text where available.
"""

from __future__ import annotations
import asyncio, json, os, re, subprocess, sys, tempfile
from pathlib import Path
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

MODEL          = "claude-haiku-4-5-20251001"
WORKERS        = 6
USER_AGENT     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
FETCH_LIMIT    = 25_000
MIN_PAGE_CHARS = 300
KEYWORDS = re.compile(r"(?i)\b(dog|dogs|pet|pets|leash|leashed|on[- ]leash|off[- ]leash|service\s+animal|canine)\b")
WIN_BEFORE, WIN_AFTER, SNIP_CAP = 200, 500, 2000
RESPONSE_SCOPE = "dogs_allowed"

PROMPT = """\
You are reading a snippet from a beach/park webpage. Answer the single question:
ARE DOGS ALLOWED AT THIS BEACH?

Return ONLY this JSON object — no markdown, no preamble:

{
  "dogs_allowed": "yes" | "no" | "restricted" | "seasonal" | "unknown",
  "reason":      "one short clause quoting or paraphrasing the page basis",
  "confidence":  number 0.00-1.00
}

Definitions:
- "yes":        Dogs allowed without significant restriction (a leash requirement alone is fine).
- "no":         Dogs prohibited entirely (or only service animals are allowed).
- "restricted": Dogs allowed but with notable restrictions — specific zones only, time-of-day windows,
                strict leash mandates beyond the standard.
- "seasonal":   Dogs allowed at some times of year and not others.
- "unknown":    The snippet does not give a clear answer.
"""

def sb(): return {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}", "Content-Type": "application/json"}

def fetch_targets():
    sql = """
    with sandy_named as (
      select geom from public.ccc_access_points
      where (archived is null or archived <> 'Yes')
        and latitude is not null and longitude is not null
        and sandy_beach='Yes' and name ilike '%beach%'
    ),
    targets as (
      select distinct cu.unit_id as cpad_unit_id, trim(cu.park_url) as park_url
      from public.cpad_units cu
      join public.cpad_units_coastal cc using(unit_id)
      left join public.geo_entity_response_current d
        on d.entity_type='cpad' and d.entity_id=cu.unit_id and d.response_scope='dogs_allowed'
      where d.response_value is null
        and cu.park_url is not null and trim(cu.park_url) <> ''
        and exists (select 1 from sandy_named s where st_contains(cu.geom, s.geom::geometry))
    ),
    cached as (
      select source_url, raw_text from (
        select source_url, raw_text,
          row_number() over (partition by source_url order by scraped_at desc) rn
        from public.park_url_extractions
        where extraction_status='success' and raw_text is not null
      ) x where rn=1
    )
    select t.cpad_unit_id, t.park_url, c.raw_text as cached_raw_text
    from targets t
    left join cached c on c.source_url = t.park_url
    order by t.cpad_unit_id;
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(sql); tmp = f.name
    try:
        r = subprocess.run(['supabase','db','query','--linked','-f',tmp],
                           capture_output=True, text=True, timeout=120,
                           encoding='utf-8', errors='replace')
        if r.returncode != 0:
            print("SQL failed:", r.stderr); return []
        s, e = r.stdout.find('{'), r.stdout.rfind('}')
        return json.loads(r.stdout[s:e+1]).get('rows', [])
    finally:
        try: os.unlink(tmp)
        except: pass

def insert_row(row):
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url, headers={**sb(), "Prefer": "return=minimal"},
                   json={**row, "response_scope": RESPONSE_SCOPE, "extraction_model": MODEL}, timeout=20)
    if not r.is_success:
        print(f"  insert fail: {r.status_code} {r.text[:200]}", file=sys.stderr)

async def fetch_url(client, url):
    try:
        r = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=25.0, follow_redirects=True)
        if not r.is_success: return None, r.status_code
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","footer","header"]): tag.decompose()
        text = re.sub(r"\n{3,}", "\n\n", soup.get_text("\n")).strip()[:FETCH_LIMIT]
        return text, r.status_code
    except Exception as e:
        print(f"  fetch err ({url}): {type(e).__name__}", file=sys.stderr)
        return None, 0

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

async def process_one(sem, client, t):
    async with sem:
        cid, url, cached = t["cpad_unit_id"], t["park_url"], t.get("cached_raw_text")
        raw, status = (cached, None) if cached else await fetch_url(client, url)
        base = {"entity_type": "cpad", "entity_id": cid, "source_url": url,
                "http_status": status, "raw_text": raw}
        if not raw or len(raw) < MIN_PAGE_CHARS:
            insert_row({**base, "fetch_status":"fetch_failed", "has_keywords": False})
            return f"  unit={cid}  fetch_failed  {url}"
        snip, n = snippet(raw)
        if not snip:
            insert_row({**base, "fetch_status":"no_keywords", "has_keywords": False})
            return f"  unit={cid}  no_keywords"
        ans = await llm(client, snip)
        if not ans:
            insert_row({**base, "fetch_status":"llm_error", "has_keywords": True, "snippet": snip})
            return f"  unit={cid}  llm_error"
        insert_row({**base, "fetch_status":"success", "has_keywords": True, "snippet": snip,
                    "response_value": ans.get("dogs_allowed"),
                    "response_reason": ans.get("reason"),
                    "response_confidence": ans.get("confidence"),
                    "extracted_at": "now()"})
        return f"  unit={cid}  {ans.get('dogs_allowed'):11s} conf={ans.get('confidence')}  {ans.get('reason','')[:80]}"

async def run():
    targets = fetch_targets()
    print(f"Loaded {len(targets)} CPAD units to extract dogs_allowed for "
          f"({sum(1 for t in targets if t.get('cached_raw_text'))} cached)\n")
    if not targets: return
    sem = asyncio.Semaphore(WORKERS)
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=WORKERS), follow_redirects=True) as client:
        tasks = [process_one(sem, client, t) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            print(await t)

if __name__ == "__main__":
    asyncio.run(run())
