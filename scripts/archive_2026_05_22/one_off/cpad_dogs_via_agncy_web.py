"""
cpad_dogs_via_agncy_web.py
--------------------------
For CPADs that lack a park_url but have an agncy_web (and contain a
CCC sandy + named-beach point and lack a dogs_allowed answer):

  1. Sitemap-grep the agency website. Score URLs against unit_name.
  2. If a URL scores >= MIN_MATCH, use it. Otherwise Tavily site:search
     as fallback (when TAVILY_API_KEY is set).
  3. Fetch the discovered URL.
  4. Section-target dog/pet/leash keyword windows.
  5. LLM extract dogs_allowed.
  6. Insert one row into geo_entity_response with the DISCOVERED url
     as source_url. No schema changes needed.

Sitemaps are cached per host in-memory for the run so 16 LA County
beaches share one fetch.

Usage:
  export TAVILY_API_KEY=...   # optional; enables fallback
  python scripts/one_off/cpad_dogs_via_agncy_web.py [--limit N]
"""

from __future__ import annotations
import argparse, asyncio, json, os, re, subprocess, sys, tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
TAVILY_KEY    = os.environ.get("TAVILY_API_KEY")

MODEL          = "claude-haiku-4-5-20251001"
WORKERS        = 4
USER_AGENT     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
FETCH_LIMIT    = 25_000
MIN_PAGE_CHARS = 300
MIN_MATCH      = 0.30
TAVILY_MIN     = 0.50
RESPONSE_SCOPE = "dogs_allowed"

KEYWORDS = re.compile(r"(?i)\b(dog|dogs|pet|pets|leash|leashed|on[- ]leash|off[- ]leash|service\s+animal|canine)\b")
WIN_BEFORE, WIN_AFTER, SNIP_CAP = 200, 500, 2000

STOPWORDS = {"the","a","an","of","and","at","in","on","to","for","park","beach","area","point","cove","bay","ca","california","state","city","county","national"}

SKIP_AGENCY_DOMAINS = {"parks.ca.gov","ca.gov","wikipedia.org","wikipedia.com"}
SKIP_PATH_TOKENS = {
    "blog","news","press","events","media","video","photo","gallery","calendar","contact","search","login","account","cart","tag","category","page","feed","rss","atom","sitemap","wp","admin","json","embed","pdf","jpg","jpeg","png","gif","css","js","svg",
}

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

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

def fetch_targets(limit: Optional[int]):
    sql = """
    with sandy_named as (
      select objectid, geom from public.ccc_access_points
      where (archived is null or archived <> 'Yes')
        and latitude is not null and longitude is not null
        and sandy_beach='Yes' and name ilike '%beach%'
    )
    select cu.unit_id, cu.unit_name, trim(cu.agncy_web) as agncy_web
    from public.cpad_units cu
    join public.cpad_units_coastal cc using(unit_id)
    left join public.geo_entity_response_current d on d.entity_type='cpad' and d.entity_id=cu.unit_id and d.response_scope='dogs_allowed'
    where d.response_value is null
      and (cu.park_url is null or trim(cu.park_url)='')
      and cu.agncy_web is not null and trim(cu.agncy_web) <> ''
      and exists (select 1 from sandy_named s where st_contains(cu.geom, s.geom::geometry))
    order by cu.agncy_web, cu.unit_name
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
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url, headers={**sb(), "Prefer": "return=minimal"},
                   json={**row, "response_scope": RESPONSE_SCOPE, "extraction_model": MODEL}, timeout=20)
    if not r.is_success:
        print(f"  insert fail: {r.status_code} {r.text[:200]}", file=sys.stderr)

def normalize_name(s):
    if not s: return set()
    return {p for p in re.findall(r"[a-z0-9]+", s.lower()) if len(p) >= 3 and p not in STOPWORDS}

def url_path_tokens(url):
    p = urlparse(url).path.lower()
    p = re.sub(r"\.(html?|aspx?|php|jsp)$", "", p)
    return set(re.findall(r"[a-z0-9]+", p))

def url_path_joined(url):
    p = urlparse(url).path.lower()
    p = re.sub(r"\.(html?|aspx?|php|jsp)$", "", p)
    return re.sub(r"[^a-z0-9]+", "", p)

def url_score(beach_name, candidate):
    name_t = normalize_name(beach_name)
    if not name_t: return 0.0
    path_t = url_path_tokens(candidate)
    if any(t in path_t for t in SKIP_PATH_TOKENS): return 0.0
    matched = name_t & path_t
    joined = url_path_joined(candidate)
    for t in name_t:
        if t not in matched and len(t) >= 5 and t in joined:
            matched.add(t)
    return round(len(matched) / len(name_t), 2) if matched else 0.0

async def fetch_sitemap(client, agency_url, cache):
    host = urlparse(agency_url).netloc.lower()
    if any(skip in host for skip in SKIP_AGENCY_DOMAINS): return cache.setdefault(host, [])
    if host in cache: return cache[host]

    base = agency_url.rstrip("/")
    parsed = urlparse(base)
    root = f"{parsed.scheme}://{parsed.netloc}"
    candidates = [f"{root}/sitemap.xml", f"{root}/sitemap_index.xml", f"{root}/sitemap-index.xml", f"{root}/wp-sitemap.xml"]

    all_urls = []
    for sm_url in candidates:
        try:
            r = await client.get(sm_url, headers={"User-Agent": USER_AGENT}, timeout=20)
            if not r.is_success: continue
            try: tree = ET.fromstring(r.text)
            except ET.ParseError: continue
            tag = tree.tag.split("}")[-1]
            if tag == "sitemapindex":
                for loc in tree.findall(".//sm:loc", NS):
                    if not loc.text or len(all_urls) >= 5000: break
                    try:
                        sub = await client.get(loc.text.strip(), headers={"User-Agent": USER_AGENT}, timeout=20)
                        if sub.is_success:
                            sub_tree = ET.fromstring(sub.text)
                            for u in sub_tree.findall(".//sm:loc", NS):
                                if u.text:
                                    all_urls.append(u.text.strip())
                                    if len(all_urls) >= 5000: break
                    except: continue
            else:
                for u in tree.findall(".//sm:loc", NS):
                    if u.text:
                        all_urls.append(u.text.strip())
                        if len(all_urls) >= 5000: break
            if all_urls: break
        except Exception: continue
    cache[host] = all_urls
    return all_urls

async def tavily_site(client, beach, agency_url):
    if not TAVILY_KEY: return []
    host = urlparse(agency_url).netloc
    try:
        r = await client.post("https://api.tavily.com/search", json={
            "api_key": TAVILY_KEY, "query": beach,
            "search_depth": "basic", "max_results": 3,
            "include_domains": [host],
        }, timeout=20)
        if not r.is_success: return []
        return r.json().get("results", []) or []
    except: return []

async def fetch_page(client, url):
    try:
        r = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=25.0, follow_redirects=True)
        if not r.is_success: return None, r.status_code
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","footer","header"]): tag.decompose()
        return re.sub(r"\n{3,}", "\n\n", soup.get_text("\n")).strip()[:FETCH_LIMIT], r.status_code
    except Exception as e:
        print(f"    fetch err ({url}): {type(e).__name__}", file=sys.stderr)
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
        print(f"    LLM err: {e}", file=sys.stderr); return None

async def process(sem, client, t, sm_cache):
    async with sem:
        cid, name, agency = t["unit_id"], t["unit_name"], t["agncy_web"]
        # 1. Discover URL
        urls = await fetch_sitemap(client, agency, sm_cache)
        chosen, source = None, None
        if urls:
            scored = sorted(((url_score(name, u), u) for u in urls), reverse=True)
            top_score, top_url = scored[0] if scored else (0.0, None)
            if top_score >= MIN_MATCH: chosen, source = top_url, f"sitemap (score {top_score})"
        if not chosen and TAVILY_KEY:
            tav = await tavily_site(client, name, agency)
            if tav and tav[0].get("score", 0) >= TAVILY_MIN:
                chosen, source = tav[0]["url"], f"tavily (score {tav[0]['score']:.2f})"
        if not chosen:
            insert_row({"entity_type": "cpad", "entity_id": cid, "source_url": agency,
                        "fetch_status":"no_keywords", "has_keywords": False})
            return f"  unit={cid:6d} {name[:40]:40s}  no discovery"
        # 2. Fetch + extract
        raw, status = await fetch_page(client, chosen)
        base = {"entity_type": "cpad", "entity_id": cid, "source_url": chosen,
                "http_status": status, "raw_text": raw}
        if not raw or len(raw) < MIN_PAGE_CHARS:
            insert_row({**base, "fetch_status":"fetch_failed", "has_keywords": False})
            return f"  unit={cid:6d} {name[:40]:40s}  found but fetch_failed  {chosen}"
        snip, _ = snippet(raw)
        if not snip:
            insert_row({**base, "fetch_status":"no_keywords", "has_keywords": False})
            return f"  unit={cid:6d} {name[:40]:40s}  no_keywords  {chosen}"
        ans = await llm(client, snip)
        if not ans:
            insert_row({**base, "fetch_status":"llm_error", "has_keywords": True, "snippet": snip})
            return f"  unit={cid:6d} {name[:40]:40s}  llm_error  {chosen}"
        insert_row({**base, "fetch_status":"success", "has_keywords": True, "snippet": snip,
                    "response_value": ans.get("dogs_allowed"),
                    "response_reason": ans.get("reason"),
                    "response_confidence": ans.get("confidence"),
                    "extracted_at": "now()"})
        return f"  unit={cid:6d} {name[:40]:40s}  {ans.get('dogs_allowed'):11s} via {source}"

async def run(args):
    targets = fetch_targets(args.limit)
    print(f"Loaded {len(targets)} CPAD units to discover URLs for "
          f"({len(set(t['agncy_web'] for t in targets))} distinct agency sites)\n")
    if not targets: return
    sem = asyncio.Semaphore(WORKERS)
    sm_cache = {}
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=WORKERS), follow_redirects=True) as client:
        tasks = [process(sem, client, t, sm_cache) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            print(await t)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    asyncio.run(run(args))
