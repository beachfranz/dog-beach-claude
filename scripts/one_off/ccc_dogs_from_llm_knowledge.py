"""
ccc_dogs_from_llm_knowledge.py
------------------------------
LLM-knowledge dogs-allowed pass for CCC access points (sandy + named
"beach" subset, matching the active map filter).

Same shape as cpad_dogs_from_llm_knowledge.py: no fetch, model answers
from training data only, strict prompt forbids confabulation, confidence
capped at 0.80.

Usage:
  python scripts/one_off/ccc_dogs_from_llm_knowledge.py --dry-run
"""

from __future__ import annotations
import argparse, asyncio, json, os, re, sys
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL  = os.environ["SUPABASE_URL"]
ANON_KEY      = os.environ.get("SUPABASE_ANON_KEY") or os.environ["SUPABASE_SERVICE_KEY"]
SERVICE_KEY   = os.environ.get("SUPABASE_SERVICE_KEY")
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

MODEL          = "claude-haiku-4-5-20251001"
WORKERS        = 8
CONFIDENCE_CAP = 0.80
RESPONSE_SCOPE = "dogs_allowed"

PROMPT = """\
You answer ONE question about a specific California beach access point.
You are NOT given a webpage to read — you must answer from your
training-data knowledge of this specific named place.

Question: ARE DOGS ALLOWED AT THIS BEACH?

INPUT FORMAT:
  name:        the beach access point's name
  county:      California county
  district:    CCC's regional grouping (e.g., "5_South Coast")
  location:    short text describing the location (city/cross-streets)
  description: short text describing what's there

DECISION RULE — be strict:
  - If you have specific knowledge of THIS named beach's dog policy
    (e.g., "Carmel Beach is famous for off-leash dogs"), answer with
    that policy and confidence 0.6-0.8.
  - If you only have GENERIC knowledge of the agency or region (e.g.,
    "California State Parks usually prohibit dogs on sand"), but
    nothing specific about this exact beach, answer "unknown" with
    confidence < 0.5. Do NOT extrapolate from category priors.
  - If you've never heard of this specific beach, answer "unknown"
    with confidence ≤ 0.3.

Return ONLY this JSON — no markdown, no preamble:

{
  "dogs_allowed": "yes" | "no" | "restricted" | "seasonal" | "unknown",
  "reason":      "short clause citing specific facts about THIS beach OR what you don't know",
  "confidence":  number 0.00-0.80
}
"""

def restGet(path):
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/{path}",
                  headers={"apikey": ANON_KEY, "Authorization": f"Bearer {ANON_KEY}"},
                  timeout=60)
    r.raise_for_status()
    return r.json()

def restGetAll(path, page=1000):
    all_rows = []
    for offset in range(0, 100_000, page):
        sep = '&' if '?' in path else '?'
        batch = restGet(f"{path}{sep}offset={offset}&limit={page}")
        all_rows.extend(batch)
        if len(batch) < page: break
    return all_rows

def fetch_targets():
    return restGetAll(
        'ccc_access_points'
        + '?select=objectid,name,county,district,location,description'
        + '&latitude=not.is.null&longitude=not.is.null'
        + '&archived=not.eq.Yes&sandy_beach=eq.Yes&name=ilike.*beach*'
        + '&order=name'
    )

async def llm(client, t):
    user = (f"name:        {t.get('name') or ''}\n"
            f"county:      {t.get('county') or ''}\n"
            f"district:    {t.get('district') or ''}\n"
            f"location:    {(t.get('location') or '')[:200]}\n"
            f"description: {(t.get('description') or '')[:300]}")
    payload = {"model": MODEL, "max_tokens": 250, "system": PROMPT,
               "messages": [{"role":"user","content":user}]}
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
        print(f"  LLM err for {t.get('name')}: {e}", file=sys.stderr); return None

def insert_row(row):
    if not SERVICE_KEY:
        raise RuntimeError("SUPABASE_SERVICE_KEY required for --write")
    url = f"{SUPABASE_URL}/rest/v1/geo_entity_response"
    r = httpx.post(url,
        headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
                 "Content-Type": "application/json", "Prefer": "return=minimal"},
        json={**row, "response_scope": RESPONSE_SCOPE,
              "extraction_model": MODEL,
              "source_url": "llm-prior://claude-haiku-4-5",
              "fetch_status": "llm_knowledge",
              "has_keywords": False}, timeout=20)
    if not r.is_success:
        print(f"  insert fail obj={row.get('entity_id')}: {r.status_code} {r.text[:200]}", file=sys.stderr)

async def process(sem, client, t, args):
    async with sem:
        ans = await llm(client, t)
        if not ans:
            return f"  obj={t['objectid']:5d}  llm_error  {t.get('name','')[:50]}"
        conf = min(float(ans.get('confidence', 0.0)), CONFIDENCE_CAP)
        ans['confidence'] = conf
        ans_val = ans.get('dogs_allowed')
        wrote = ""
        if args.write and conf >= args.min_conf and ans_val and ans_val != 'unknown':
            insert_row({"entity_type":  "ccc",
                        "entity_id":    t['objectid'],
                        "response_value":      ans_val,
                        "response_reason":     ans.get('reason'),
                        "response_confidence": conf,
                        "extracted_at":  "now()"})
            wrote = "[wrote] "
        elif args.write:
            wrote = "[skip ] "
        else:
            wrote = "[dry  ] "
        return (f"  {wrote}obj={t['objectid']:5d}  "
                f"{(ans_val or '?'):11s} conf={conf}  "
                f"{(t.get('name') or '')[:45]:45s}  ({(t.get('county') or '')[:18]})\n"
                f"     {(ans.get('reason') or '')[:140]}")

async def run(args):
    targets = fetch_targets()
    print(f"Loaded {len(targets)} CCC sandy+named-beach points "
          f"(write={args.write}, min_conf={args.min_conf})\n")
    if not targets: return
    sem = asyncio.Semaphore(WORKERS)
    by_answer = {}
    by_conf_band = {'>=0.7': 0, '0.5-0.7': 0, '<0.5': 0}
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=WORKERS)) as client:
        tasks = [process(sem, client, t, args) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            line = await t
            print(line)
            # try to parse for stats
            m = re.search(r'(yes|no|restricted|seasonal|unknown)\s+conf=([\d.]+)', line)
            if m:
                ans, conf = m.group(1), float(m.group(2))
                by_answer[ans] = by_answer.get(ans, 0) + 1
                if conf >= 0.7: by_conf_band['>=0.7'] += 1
                elif conf >= 0.5: by_conf_band['0.5-0.7'] += 1
                else: by_conf_band['<0.5'] += 1
            if i % 50 == 0:
                print(f"  --- {i}/{len(targets)} done ---")
    print("\n========== SUMMARY ==========")
    for k, v in sorted(by_answer.items(), key=lambda x:-x[1]):
        print(f"  {k:12s} {v}")
    print()
    for k in ['>=0.7','0.5-0.7','<0.5']:
        print(f"  conf {k:8s} {by_conf_band[k]}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="(default) Don't write — just print + summarize.")
    p.add_argument("--write", action="store_true",
                   help="Write answers to geo_entity_response (entity_type='ccc'). "
                        "Skips 'unknown' and answers below --min-conf.")
    p.add_argument("--min-conf", type=float, default=0.6,
                   help="Confidence floor for writes (default 0.6).")
    args = p.parse_args()
    asyncio.run(run(args))
