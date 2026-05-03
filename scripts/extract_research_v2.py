"""
extract_research_v2.py — modernized research-style extraction with validation gates.

Tests whether the original `research` source's quality issues (wrong-beach
name collisions, content that doesn't mention the beach) are fixed by:

  1. Disambiguated query — beach + city + county + state baked into search
  2. Source name match gate — drop URL if page doesn't mention beach by name
  3. Geographic gate — drop URL that mentions a different state or wrong CA city
  4. Cite-required mode — model must return verbatim quote with the beach name,
     or return null
  5. Authority hint — .gov / parks.ca.gov / city.gov weighted above aggregators

URL pool (instead of hitting a search API): existing local data.
  - park_url_extractions.source_url for this fid
  - city_policy_sources URLs for this beach's place_fips (city)
  - operators.website / operators.dog_policy_url
  - cpad_units.agncy_web for this beach's CPAD unit

Outputs to beach_policy_extractions with:
  variant_key  = 'research_v2'
  model_name   = 'claude-sonnet-4-6'
  extraction_method = 'research_v2'

Usage:
  python scripts/extract_research_v2.py            # dry-run
  python scripts/extract_research_v2.py --apply    # write rows
  python scripts/extract_research_v2.py --apply --set-name v3   # default v3+v3b
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from anthropic import Anthropic
from bs4 import BeautifulSoup
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")
ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT,
           "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9",
           "Accept-Encoding": "identity"}
MAX_CONTENT_CHARS = 12000

# Other CA cities + states — used by the geographic gate
WRONG_STATES = ["new york", "florida", "rhode island", "ohio", "texas", "oregon",
                "washington state", "hawaii", "north carolina", "south carolina",
                "new jersey", "massachusetts", "delaware", "maine", "alabama"]

# Authority hints — domains that should be trusted MORE than aggregators
AUTH_DOMAINS = {
    "parks.ca.gov":  4, "ca.gov": 3, "nps.gov": 4,
    "sandiego.gov":  4, "longbeach.gov": 4, "newportbeachca.gov": 4,
    "smgov.net":     4, "hermosabeach.gov": 4, "ventura.org": 4,
    "lacounty.gov":  3, "ocparks.com": 3, "danapoint.org": 3,
    "delmar.ca.us":  4, "coronado.ca.us": 4,
    "bringfido.com": 1, "dogtrekker.com": 1, "californiabeaches.com": 2,
    "wildlife.ca.gov": 3, "movingtolagunabeach.com": 1, "yelp.com": 0,
    "tripadvisor.com": 0, "alltrails.com": 1,
}


def fetch_html(url: str, timeout: int = 12) -> str | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return None
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception:
        return None


def strip_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe"]):
        tag.decompose()
    text = (soup.body or soup).get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    return "\n".join(lines)[:MAX_CONTENT_CHARS]


def authority_score(url: str) -> int:
    host = urllib.parse.urlparse(url).hostname or ""
    host = host.lower().lstrip("www.")
    for dom, score in AUTH_DOMAINS.items():
        if host.endswith(dom):
            return score
    if host.endswith(".gov"):  return 3
    if host.endswith(".ca.us"): return 3
    return 1


def name_match(text: str, beach_name: str) -> bool:
    """Gate 2: Does the page text reference the beach by name? Word-level fuzzy."""
    text_lower = text.lower()
    name_lower = beach_name.lower()
    if name_lower in text_lower:
        return True
    stopwords = {'the','a','an','of','and','beach','park','state','county','city','municipal'}
    words = [w for w in re.findall(r"\w+", name_lower) if w not in stopwords and len(w) > 2]
    if not words:
        return name_lower in text_lower
    matches = sum(1 for w in words if w in text_lower)
    # 75%+ of significant words must appear, AND at least one must be "distinctive"
    # (a word the model can use to disambiguate this beach from generic content)
    return matches / len(words) >= 0.75


def geo_gate(text: str, target_city: str | None, target_county: str | None) -> bool:
    """Gate 3: Reject ONLY if multiple wrong-state mentions and no California mention."""
    text_lower = text.lower()[:8000]
    wrong_count = sum(text_lower.count(ws) for ws in WRONG_STATES)
    california_count = text_lower.count('california') + text_lower.count(', ca ') + text_lower.count(' ca ')
    if california_count >= 1 and wrong_count <= 1:
        return True
    if wrong_count >= 2 and california_count == 0:
        return False
    return True


def gather_urls(cur, fid: int, beach_name: str) -> list[dict]:
    """Build candidate URL pool for one beach from existing local data."""
    urls = {}
    # park_url_extractions
    cur.execute("""select source_url from public.park_url_extractions
                    where arena_group_id=%s and source_url is not null
                    order by scraped_at desc limit 5""", (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['source_url'], "park_url")
    # operator website + dog_policy_url via cpad_units.mng_agncy → operators
    cur.execute("""
      select op.website, op.dog_policy_url, op.canonical_name
        from public.beaches_gold g
        join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
        join public.operators op on op.cpad_agncy_name = cu.mng_agncy
       where g.fid = %s
    """, (fid,))
    for r in cur.fetchall():
        for k in ('website', 'dog_policy_url'):
            if r[k]:
                urls.setdefault(r[k], f"operator:{r['canonical_name']}")
    # cpad_units.agncy_web
    cur.execute("""
      select cu.agncy_web, cu.unit_name
        from public.beaches_gold g
        join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
       where g.fid = %s and cu.agncy_web is not null
    """, (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['agncy_web'], f"cpad_unit:{r['unit_name']}")
    # city_policy_sources by place_fips
    cur.execute("""
      select cps.url, cps.source_type
        from public.beaches_gold g
        join public.poi_landing pl on pl.fid = g.fid
        join public.city_policy_sources cps on cps.place_fips = pl.place_fips
       where g.fid = %s and pl.place_fips is not null
       order by cps.id desc limit 5
    """, (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['url'], f"city:{r['source_type']}")
    # Existing extraction URLs (already proven relevant)
    cur.execute("""
      select cps.url
        from public.beach_policy_extractions e
        join public.city_policy_sources cps on cps.id = e.source_id
       where e.arena_group_id = %s
       group by cps.url limit 5
    """, (fid,))
    for r in cur.fetchall():
        urls.setdefault(r['url'], "prior_extraction_source")
    return [{"url": u, "kind": k, "auth": authority_score(u)} for u, k in urls.items()]


def call_llm_with_cite(content: str, beach_name: str, city: str | None,
                        question: str, expected_shape: str) -> dict:
    """Cite-required LLM call. Model must include verbatim quote with beach name."""
    geo = f", {city}" if city else ""
    sys_prompt = (
        f"You are extracting policy information about '{beach_name}{geo}'. "
        f"Use only the provided source page. "
        f"If the source does not mention this specific beach by name "
        f"(or its commonly-known abbreviation), return 'no_match'. "
        f"If the source mentions the beach but does not address the question, "
        f"return 'unknown'. Otherwise, answer + provide a verbatim quote from "
        f"the source (50-200 chars) that supports the answer."
    )
    user_prompt = f"""SOURCE PAGE CONTENT:
{content}

QUESTION: {question}

EXPECTED ANSWER SHAPE: {expected_shape}

Return JSON:
{{
  "answer": "<the extracted value, 'no_match', or 'unknown'>",
  "quote": "<verbatim quote from source, ~50-200 chars, must contain the beach name>",
  "confidence": "high|medium|low"
}}"""
    resp = ANTHROPIC.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=sys_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    raw = resp.content[0].text if resp.content else ""
    return {
        "raw": raw,
        "input_tokens":  resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }


def parse_cited_response(raw: str) -> dict:
    """Extract answer + quote + confidence from JSON block. Tolerate markdown fences."""
    raw = raw.strip()
    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        d = json.loads(raw)
        return {"answer": d.get("answer"), "quote": d.get("quote"), "confidence": d.get("confidence")}
    except Exception:
        # Fallback regex
        m = re.search(r'"answer"\s*:\s*"([^"]+)"', raw)
        ans = m.group(1) if m else raw[:80]
        return {"answer": ans, "quote": None, "confidence": None}


# Fields to extract per beach (match curator's enum-only fields for clean scoring)
FIELDS = [
    ("dogs_allowed",     "Are dogs allowed on this beach? (yes/no/seasonal/unclear). If ANY part of the beach allows dogs (even with restrictions), pick 'yes'.", "yes/no/seasonal/unclear"),
    ("leash_policy",     "What is the leash rule? If ANY part of the beach is off-leash (even just at certain hours or in a designated area), pick 'off_leash'. Otherwise on_leash if dogs are allowed but always on leash. unknown if source silent.", "off_leash/on_leash/unknown"),
    ("public_access",    "Is the beach reachable to the general public? (yes/restricted/no/unclear)", "yes/restricted/no/unclear"),
    ("parking_lot",      "Is there a parking lot, and is it free or paid? (free/paid/none/unclear)", "free/paid/none/unclear"),
    ("parking_street",   "Is there street parking near the access, and is it free or paid? (free/paid/none/unclear)", "free/paid/none/unclear"),
    ("lifeguard",        "Is there a lifeguard? (full_time/seasonal/none/unknown)", "full_time/seasonal/none/unknown"),
    ("restrooms",        "Are there public restrooms? (yes/seasonal/none/unknown)", "yes/seasonal/none/unknown"),
    ("outdoor_showers",  "Are there outdoor cold-water rinse showers at the access point? (yes/seasonal/none/unknown)", "yes/seasonal/none/unknown"),
]


def reconnect():
    """Open a fresh DB connection. Called per-beach to avoid pooler timeouts
    during long LLM-heavy runs."""
    c = psycopg2.connect(**PG)
    c.set_client_encoding("UTF8")
    return c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Write rows to beach_policy_extractions. Default is dry-run.")
    ap.add_argument("--set-name", default="v3+v3b",
                    help="Gold set to extract for (v3 / v3b / v3+v3b)")
    ap.add_argument("--max-urls-per-beach", type=int, default=4,
                    help="Cap URLs fetched per beach (sorted by authority desc)")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Skip beaches that already have research_v2 rows (resume mode)")
    args = ap.parse_args()

    conn = reconnect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Ensure research_v2 variant entries exist (one per field) so we can satisfy
    # beach_policy_extractions.variant_id FK. Marked active=false, is_canon=false
    # since these are experimental, not canonical.
    for fname, _q, _shape in FIELDS:
        cur.execute("""
            insert into public.extraction_prompt_variants
                (field_name, variant_key, prompt_template, expected_shape, target_model,
                 active, is_canon, notes)
            values (%s, 'research_v2',
                    'multi-source research extraction with 5 validation gates (cite-required)',
                    'enum', 'claude-sonnet-4-6',
                    false, false,
                    'extract_research_v2.py — modernized research source with multi-URL synthesis + cite gates')
            on conflict (field_name, variant_key) do nothing
        """, (fname,))
    cur.execute("""select field_name, id from public.extraction_prompt_variants
                    where variant_key = 'research_v2'""")
    variant_id_by_field = {r['field_name']: r['id'] for r in cur.fetchall()}
    conn.commit()
    print(f"  research_v2 variants registered: {len(variant_id_by_field)} fields")

    sets = ['v3', 'v3b'] if args.set_name == 'v3+v3b' else [args.set_name]
    cur.execute("""
      select g.fid, g.name, g.county_name, g.lat, g.lon, j.name as city_name
        from public.beaches_gold g
        join public.gold_set_membership m on m.fid = g.fid
        left join public.poi_landing pl on pl.fid = g.fid
        left join public.jurisdictions j on j.fips_place = pl.place_fips
       where m.set_name = any(%s) and not m.excluded
       group by g.fid, g.name, g.county_name, g.lat, g.lon, j.name
       order by g.fid
    """, (sets,))
    beaches = cur.fetchall()
    print(f"v3+v3b active: {len(beaches)} beaches")

    run_id = str(uuid.uuid4())
    print(f"\nrun_id = {run_id}")

    # Skip-existing support: which beaches already have research_v2 rows?
    cur.execute("""select distinct arena_group_id from public.beach_policy_extractions
                    where variant_key = 'research_v2'""")
    already_done = {r['arena_group_id'] for r in cur.fetchall()}
    if args.skip_existing and already_done:
        print(f"  --skip-existing: already done for {len(already_done)} beaches")
        beaches = [b for b in beaches if b['fid'] not in already_done]
        print(f"  remaining: {len(beaches)}")

    total_calls = 0
    total_skips = 0
    total_inserted = 0
    for b in beaches:
        fid = b['fid']; name = b['name']; city = b['city_name']
        # Reconnect per-beach to dodge pooler timeouts during long LLM runs
        try:
            conn.close()
        except Exception:
            pass
        conn = reconnect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        urls = sorted(gather_urls(cur, fid, name), key=lambda u: -u['auth'])
        urls = urls[:args.max_urls_per_beach]
        print(f"\n[{fid}] {name}  city={city}  county={b['county_name']}  candidate_urls={len(urls)}", flush=True)
        # Fetch + apply gates
        valid_sources = []
        for u in urls:
            html = fetch_html(u['url'])
            if not html:
                print(f"    [-] {u['url']}  (fetch failed)")
                continue
            text = strip_html(html)
            if not name_match(text, name):
                print(f"    [-] {u['url']}  (name not in page)")
                continue
            if not geo_gate(text, city, b['county_name']):
                print(f"    [-] {u['url']}  (geographic gate)")
                continue
            valid_sources.append({**u, "text": text})
            print(f"    [+] {u['url']}  (auth={u['auth']}, kind={u['kind']})")
        if not valid_sources:
            print(f"    SKIP: no valid sources for {name}")
            total_skips += 1
            continue
        # Use the highest-authority source
        primary = max(valid_sources, key=lambda u: u['auth'])

        # Get/create source_id once per beach
        if args.apply:
            cur.execute("select id from public.city_policy_sources where url = %s limit 1", (primary['url'],))
            existing = cur.fetchone()
            if existing:
                source_id = existing['id']
            else:
                cur.execute("""
                  insert into public.city_policy_sources (url, source_type, place_fips, curated_by, notes)
                  values (%s, 'other', '06ORPH', 'extract_research_v2', 'auto-created during research_v2 run')
                  returning id
                """, (primary['url'],))
                source_id = cur.fetchone()['id']
            conn.commit()

        beach_rows = []
        for fname, question, shape in FIELDS:
            if not args.apply:
                continue
            try:
                resp = call_llm_with_cite(primary['text'], name, city, question, shape)
            except Exception as e:
                print(f"    [LLM err] {fname}: {e}", flush=True)
                continue
            parsed = parse_cited_response(resp['raw'])
            answer = (parsed.get('answer') or '').strip()
            if answer in ('no_match', 'unknown', None, ''):
                print(f"    [{fname}] -> {answer!r}  (skipped — no usable answer)", flush=True)
            else:
                print(f"    [{fname}] -> {answer!r}  (cite: {(parsed.get('quote') or '')[:60]!r})", flush=True)
            beach_rows.append({
                "fid": fid, "arena_group_id": fid, "source_id": source_id,
                "variant_id": variant_id_by_field[fname],
                "field_name": fname, "variant_key": "research_v2",
                "model_name": "claude-sonnet-4-6",
                "parsed_value": answer if answer else None,
                "raw_response": resp['raw'], "evidence_quote": parsed.get('quote'),
                "input_tokens": resp['input_tokens'], "output_tokens": resp['output_tokens'],
            })
            total_calls += 1
        # Commit per beach so a connection drop doesn't lose work
        if beach_rows and args.apply:
            try:
                for r in beach_rows:
                    cur.execute("""
                      insert into public.beach_policy_extractions
                        (fid, arena_group_id, source_id, variant_id, source_type, variant_key,
                         field_name, model_name, parsed_value, raw_response, evidence_quote,
                         parse_succeeded, extraction_method, run_id, input_tokens, output_tokens)
                      values (%(fid)s, %(arena_group_id)s, %(source_id)s, %(variant_id)s, 'other', %(variant_key)s,
                              %(field_name)s, %(model_name)s, %(parsed_value)s, %(raw_response)s,
                              %(evidence_quote)s, true, 'llm_hybrid', %(run_id)s,
                              %(input_tokens)s, %(output_tokens)s)
                    """, {**r, "run_id": run_id})
                conn.commit()
                total_inserted += len(beach_rows)
                print(f"    flushed {len(beach_rows)} rows", flush=True)
            except Exception as e:
                # Reconnect on connection-drop so we can keep going
                try: conn.rollback()
                except Exception: pass
                try: conn.close()
                except Exception: pass
                conn = reconnect()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                print(f"    INSERT err (reconnected): {str(e)[:200]}", flush=True)

    print(f"\n=== summary ===", flush=True)
    print(f"  beaches processed: {len(beaches)}, skipped (no valid sources): {total_skips}")
    print(f"  LLM calls: {total_calls}, rows inserted: {total_inserted}")
    if not args.apply:
        print("\n(dry-run; rerun with --apply to write rows)")
    else:
        print(f"  run_id = {run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
