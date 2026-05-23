"""
cpad_dogs_from_llm_knowledge.py
-------------------------------
Ask the LLM "are dogs allowed?" for CPADs lacking a current dogs_allowed
answer, providing only {unit_name, mng_agncy, county} — no URL, no fetch.

This is a knowledge-only fallback: works for well-known beaches in
training data, returns "unknown" for obscure CPAD units. Strict prompt
forbids confabulation; confidence is capped at 0.80 to reflect the
inherent staleness vs verified web extracts.

Usage:
  python scripts/one_off/cpad_dogs_from_llm_knowledge.py --dry-run
  python scripts/one_off/cpad_dogs_from_llm_knowledge.py        # writes
"""

from __future__ import annotations
import argparse, asyncio, json, os, re, subprocess, sys, tempfile
from pathlib import Path
from typing import Optional
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "pipeline" / ".env")
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SERVICE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

MODEL          = "claude-haiku-4-5-20251001"
WORKERS        = 6
RESPONSE_SCOPE = "dogs_allowed"
CONFIDENCE_CAP = 0.80   # knowledge-source ceiling

PROMPT = """\
You answer ONE question about a specific California beach or coastal
park polygon. You are NOT given a webpage to read — you must answer
from your training-data knowledge of this specific named place.

Question: ARE DOGS ALLOWED AT THIS BEACH/PARK?

INPUT FORMAT (you'll receive these three facts):
  unit_name: the CPAD polygon's official name
  mng_agncy: the agency that manages it
  county:    the California county it sits in

DECISION RULE — be strict:
  - If you have specific knowledge of THIS named beach's dog policy
    (e.g., "Carmel Beach is famous for off-leash dogs"), answer with
    that policy and confidence 0.6-0.8.
  - If you only have GENERIC knowledge of the agency type (e.g.,
    "California State Parks usually prohibit dogs on sand"), but
    nothing specific about this exact beach, answer "unknown" with
    confidence < 0.5. Do NOT extrapolate from category priors.
  - If you've never heard of this specific beach, answer "unknown"
    with confidence ≤ 0.3.

Return ONLY this JSON — no markdown, no preamble:

{
  "dogs_allowed": "yes" | "no" | "restricted" | "seasonal" | "unknown",
  "reason":      "short clause explaining the basis (cite specific facts you know about THIS beach, OR say what you don't know)",
  "confidence":  number 0.00-0.80
}
"""

def sb(): return {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}", "Content-Type": "application/json"}

def fetch_targets():
    sql = """
    with sandy_named as (
      select objectid, geom from public.ccc_access_points
      where (archived is null or archived <> 'Yes')
        and latitude is not null and longitude is not null
        and sandy_beach='Yes' and name ilike '%beach%'
    )
    select cu.unit_id, cu.unit_name, cu.mng_agncy, cu.county
    from public.cpad_units cu
    join public.cpad_units_coastal cc using(unit_id)
    left join public.geo_entity_response_current d on d.entity_type='cpad' and d.entity_id=cu.unit_id and d.response_scope='dogs_allowed'
    where d.response_value is null
      and exists (select 1 from sandy_named s where st_contains(cu.geom, s.geom::geometry))
    order by cu.mng_agncy, cu.unit_name;
    """
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
                   json={**row, "response_scope": RESPONSE_SCOPE,
                         "extraction_model": MODEL,
                         "source_url": "llm-prior://claude-haiku-4-5",
                         "fetch_status": "llm_knowledge",
                         "has_keywords": False}, timeout=20)
    if not r.is_success:
        print(f"  insert fail: {r.status_code} {r.text[:200]}", file=sys.stderr)

async def llm(client, target):
    user = (f"unit_name: {target['unit_name']}\n"
            f"mng_agncy: {target['mng_agncy']}\n"
            f"county:    {target.get('county') or '(unknown)'}")
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
        print(f"  LLM err: {e}", file=sys.stderr); return None

async def process(sem, client, t, dry_run):
    async with sem:
        ans = await llm(client, t)
        if not ans:
            return f"  unit={t['unit_id']:6d} {t['unit_name'][:38]:38s}  llm_error"
        ans['confidence'] = min(float(ans.get('confidence', 0.0)), CONFIDENCE_CAP)

        if not dry_run:
            insert_row({"entity_type": "cpad",
                        "entity_id":   t["unit_id"],
                        "response_value":      ans.get("dogs_allowed"),
                        "response_reason":     ans.get("reason"),
                        "response_confidence": ans.get("confidence"),
                        "extracted_at":        "now()"})

        prefix = "[dry] " if dry_run else ""
        return (f"  {prefix}unit={t['unit_id']:6d} {t['unit_name'][:38]:38s}  "
                f"{ans.get('dogs_allowed'):11s} conf={ans.get('confidence')}\n"
                f"     {t['mng_agncy'][:60]}\n"
                f"     reason: {(ans.get('reason') or '')[:120]}")

async def run(args):
    targets = fetch_targets()
    print(f"Loaded {len(targets)} CPAD gap units to ask via LLM knowledge "
          f"(dry_run={args.dry_run})\n")
    if not targets: return
    sem = asyncio.Semaphore(WORKERS)
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=WORKERS)) as client:
        tasks = [process(sem, client, t, args.dry_run) for t in targets]
        for i, t in enumerate(asyncio.as_completed(tasks), 1):
            print(await t)
            print()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    asyncio.run(run(args))
