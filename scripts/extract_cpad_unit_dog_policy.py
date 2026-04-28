"""
extract_cpad_unit_dog_policy.py
-------------------------------
Pass B of the CCC-less dog-policy pipeline.

For each CPAD unit that intersects beach_locations AND has a park_url,
Tavily-extract the page and classify the dog-access rule. Single
Sonnet pass (per-park context is narrower than per-agency).

Writes to public.cpad_unit_dogs_policy keyed on cpad_unit_id.

Idempotent — skips units already in cpad_unit_dogs_policy unless --refresh.
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / "pipeline" / ".env")
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SERVICE_KEY       = os.environ["SUPABASE_SERVICE_KEY"]
TAVILY_API_KEY    = os.environ["TAVILY_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SONNET            = "claude-sonnet-4-6"
SLEEP_S           = 1.5


def tavily_extract(url: str) -> tuple[str, str]:
    try:
        r = httpx.post("https://api.tavily.com/extract",
            json={"api_key": TAVILY_API_KEY, "urls":[url]}, timeout=90)
        r.raise_for_status()
        res = r.json().get("results", [])
        if not res:
            failed = r.json().get("failed_results", [])
            why = (failed[0].get("error","") if failed else "no_results")[:80]
            return ("tavily_failed", why)
        return ("ok", res[0].get("raw_content","") or "")
    except httpx.TimeoutException:
        return ("tavily_timeout", "")
    except Exception as e:
        return (f"tavily_error:{type(e).__name__}", "")


def playwright_fetch(url: str, timeout_ms: int = 20000) -> tuple[str, str]:
    """Render a page with headless Chromium. Strips images/fonts/media to
    speed up loads. Returns (status, plain_text). Plain text is body.innerText
    after the page's network is mostly idle."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return ("playwright_unavailable", "")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            # Block heavy resources for speed
            page.route("**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf,mp4,webm}",
                       lambda r: r.abort())
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                # Best-effort wait for network to quiet
                try:
                    page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass
                text = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
            finally:
                ctx.close(); browser.close()
            text = re.sub(r"\n{3,}", "\n\n", text)[:15000]
            if len(text) < 100:
                return ("playwright_thin", text)
            return ("ok_playwright", text)
    except Exception as e:
        return (f"playwright_error:{type(e).__name__}", "")


SYSTEM = """You read a single park or beach detail page from a public agency's website
and extract dog-access policy for THAT park/unit only.

CONTEXT: The page comes from a CPAD park_url — a per-unit page maintained by the
managing agency. It describes a specific park, beach, or open-space unit.

Return ONLY a JSON object with these fields:
{
  "default_rule":      "yes" | "no" | "restricted" | "unknown",
  "leash_required":    true | false | null,
  "source_quote":      "<verbatim quote from the page that supports default_rule, ≤200 chars>",
  "exceptions":        [{"beach_name": "...", "rule": "yes"|"no"|"restricted", "source_quote": "..."}, ...] OR null,
  "time_windows":      [{"description": "...", "start_hour": 0-23, "end_hour": 0-23}, ...] OR null,
  "seasonal_rules":    [{"description": "...", "start_date": "MM-DD", "end_date": "MM-DD"}, ...] OR null,
  "ordinance_ref":     "<city/county code citation if mentioned, e.g. 'Mun. Code 8.32'>" OR null,
  "confidence":        0.0-1.0,

  "areas": {
    "sand":         "off_leash" | "on_leash" | "forbidden" | "unknown",
    "water":        "off_leash" | "on_leash" | "forbidden" | "unknown",
    "picnic_area":  "off_leash" | "on_leash" | "forbidden" | "unknown",
    "parking_lot":  "off_leash" | "on_leash" | "forbidden" | "unknown",
    "trails":       "off_leash" | "on_leash" | "forbidden" | "unknown",
    "campground":   "off_leash" | "on_leash" | "forbidden" | "unknown"
  },
  "designated_dog_zones": "<verbatim text describing any named dog-permitted zones>" OR null,
  "prohibited_areas":     "<verbatim text describing any named off-limits zones>" OR null
}

RULE SEMANTICS:
- "yes" = dogs allowed. If only one constraint is "must be on leash", that's still "yes" with leash_required=true.
- "no" = dogs prohibited.
- "restricted" = dogs allowed only sometimes (time windows, seasonal, designated zones, etc.).
- "unknown" = page does not actually state a dog policy.

IMPORTANT: A SINGLE MENTION of leash, dog rules, or pet policy is enough to determine the rule.
- "Keep dogs on leash" / "Dogs must be leashed" / "Dogs welcome on leash" → default_rule="yes", leash_required=true
- "No dogs allowed" / "Pets prohibited" → default_rule="no"
- "Dogs allowed in designated areas only" / "before 9am" → default_rule="restricted"
- "Service animals only" → default_rule="no" (with note in source_quote)
- ONLY return "unknown" if there is genuinely zero dog/leash/pet content. A header like "Are dogs Allowed?" with no answer text below it counts as unknown.

AREA SEMANTICS (independent decomposition — fill these IN ADDITION to default_rule):
- The 6 fixed areas: sand, water, picnic_area, parking_lot, trails, campground.
- A beach can be default_rule="yes" even if 5 of 6 areas are forbidden — as long as ONE area allows dogs.
- "off_leash" — page explicitly says dogs may be off-leash in this area.
- "on_leash" — dogs allowed but leash required. CA-default rules of thumb: "Dogs allowed" without further qualifier usually means on_leash on city/state agency pages.
- "forbidden" — page explicitly says dogs not allowed in this area (e.g., "Dogs not allowed on sand").
- "unknown" — page does not address this area. DEFAULT for any area not explicitly mentioned.
- "Day-use areas" → picnic_area = on_leash. Day-use does NOT cover sand/water/trails.
- "Multiuse trail / bike path" → trails = on_leash.
- "Not allowed on the beach" → sand = forbidden, water = forbidden.
- "Allowed only in parking lot" → parking_lot = on_leash; ALL other named-but-excluded = forbidden; unmentioned = unknown.
- NEVER infer off_leash unless the quote literally says "off-leash" or "no leash required".

DESIGNATED_DOG_ZONES / PROHIBITED_AREAS: verbatim text from the page. E.g., "Dog Beach (north end)" or "south of lifeguard tower 3". Null if no such named zones.

EXCEPTIONS: only populate when the page mentions specific NAMED sub-beaches/zones with rules different from the default. Don't manufacture exceptions from generic mentions.

TIME_WINDOWS / SEASONAL_RULES / ORDINANCE_REF: only if specific values are stated.

If the page text is empty, error, or unrelated, return default_rule="unknown" with confidence ≤0.2 and all areas="unknown".

NO PROSE OUTSIDE THE JSON. NO MARKDOWN FENCES."""


def call_llm(page_text: str, unit_name: str) -> dict:
    # Pre-filter: if page mentions dogs/leash/pet, anchor the snippet around
    # those mentions so the LLM doesn't miss them when content is buried.
    text = page_text
    keyword_positions = []
    for kw in ('dog', 'leash', 'pet'):
        i = 0
        while True:
            i = text.lower().find(kw, i)
            if i < 0: break
            keyword_positions.append(i); i += 1
    if keyword_positions:
        first = max(0, min(keyword_positions) - 500)
        last  = min(len(text), max(keyword_positions) + 1500)
        snippet = text[first:last]
        if len(snippet) > 12000:
            snippet = snippet[:12000]
    else:
        snippet = text[:12000]
    user = f"Park/unit: {unit_name}\n\nPage text:\n{snippet}"
    for attempt in range(3):
        try:
            r = httpx.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model": SONNET, "max_tokens": 1500, "system": SYSTEM,
                      "messages":[{"role":"user","content":user}]},
                timeout=120)
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(2 ** attempt * 3); continue
            r.raise_for_status()
            text = r.json()["content"][0]["text"].strip()
            text = re.sub(r"^```(?:json)?\s*|\s*```\s*$", "", text)
            # Find first balanced JSON object
            return json.loads(text)
        except json.JSONDecodeError as e:
            print(f"    json parse error: {e}", file=sys.stderr)
            return {"default_rule":"unknown","confidence":0.0,"_parse_error":True}
        except Exception as e:
            print(f"    llm error: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt * 2)
    return {"default_rule":"unknown","confidence":0.0,"_max_retries":True}


def fetch_target_units(counties: list[str] | None = None) -> list[dict]:
    """CPAD units that intersect beach_locations and have a park_url.
    If counties supplied, scope to beach_locations within those counties."""
    import subprocess
    if counties:
        county_list = ",".join(f"'{c}'" for c in counties)
        sql = f"""
          with bl_sc as (
            select bl.geom from public.beach_locations bl
            join public.counties c on st_intersects(c.geom, bl.geom)
            where c.name in ({county_list})
          ),
          hits as (
            select distinct cu.unit_id, cu.unit_name, cu.agncy_name, cu.park_url
            from bl_sc bl
            join public.cpad_units cu on st_intersects(cu.geom, bl.geom)
            where cu.park_url is not null and cu.park_url <> ''
          )
          select unit_id, unit_name, agncy_name, park_url
          from hits
          order by unit_id;
        """
    else:
        sql = """
          with hits as (
            select distinct cu.unit_id, cu.unit_name, cu.agncy_name, cu.park_url
            from beach_locations bl
            join cpad_units cu on st_intersects(cu.geom, bl.geom)
            where cu.park_url is not null and cu.park_url <> ''
          )
          select unit_id, unit_name, agncy_name, park_url
          from hits
          order by unit_id;
        """
    r = subprocess.run(
        ["supabase","db","query","--linked",sql],
        capture_output=True, text=True, timeout=60,
        cwd=str(Path(__file__).parent.parent)
    )
    if r.returncode != 0:
        raise RuntimeError(f"db query failed: {r.stderr[:500]}")
    out = r.stdout
    i = out.find('"rows"')
    j = out.find('[', i)
    depth = 0
    for k in range(j, len(out)):
        if out[k] == '[': depth += 1
        elif out[k] == ']':
            depth -= 1
            if depth == 0:
                return json.loads(out[j:k+1])
    raise RuntimeError("no rows")


def db_existing_unit_ids() -> set[int]:
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                  headers=headers, params={"select":"cpad_unit_id"}, timeout=30)
    r.raise_for_status()
    return {row["cpad_unit_id"] for row in r.json()}


def db_upsert(rows: list[dict]) -> None:
    if not rows: return
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    r = httpx.post(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                   headers=headers, json=rows, timeout=60,
                   params={"on_conflict": "cpad_unit_id"})
    if r.status_code >= 400:
        # Fall back to per-row upserts so one bad row doesn't kill batch
        print(f"    batch upsert {r.status_code} — falling back per-row", file=sys.stderr)
        for row in rows:
            rr = httpx.post(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                           headers=headers, json=[row], timeout=30,
                           params={"on_conflict": "cpad_unit_id"})
            if rr.status_code >= 400:
                print(f"    SKIP unit {row.get('cpad_unit_id')}: {rr.status_code} {rr.text[:200]}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--counties", type=str, default=None,
                    help="comma-separated, e.g. 'Los Angeles,Orange,San Diego'")
    ap.add_argument("--retry-failed-only", action="store_true",
                    help="only re-process rows where prior pass returned fetch_failed or dogs_allowed='unknown'; uses Playwright fallback")
    args = ap.parse_args()

    counties = [c.strip() for c in args.counties.split(",")] if args.counties else None
    units = fetch_target_units(counties=counties)
    print(f"Loaded {len(units)} CPAD units (805 ∩ has park_url)")

    # Always exclude rows already populated by the CDPR master-table parse —
    # their data is richer (per-area + nuanced source quotes) than what a
    # per-park park_url scrape would produce.
    headers = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
               "Range": "0-9999"}
    r = httpx.get(f"{SUPABASE_URL}/rest/v1/cpad_unit_dogs_policy",
                  headers=headers,
                  params={"select":"cpad_unit_id,source_quote,dogs_allowed,extraction_model"},
                  timeout=30)
    r.raise_for_status()
    existing_rows = r.json()
    table_parse_ids = {row["cpad_unit_id"] for row in existing_rows
                       if row.get("extraction_model") == "table_parse"}
    if table_parse_ids:
        before = len(units)
        units = [u for u in units if u["unit_id"] not in table_parse_ids]
        print(f"Excluded {before - len(units)} rows already from CDPR master-table parse")

    if args.retry_failed_only:
        retry_ids = {row["cpad_unit_id"] for row in existing_rows
                     if (row.get("source_quote") or "").startswith("fetch_failed:")
                     or row.get("dogs_allowed") == "unknown"}
        before = len(units)
        units = [u for u in units if u["unit_id"] in retry_ids]
        print(f"--retry-failed-only: kept {len(units)} of {before} (rows that previously failed or returned unknown)")
    elif not args.refresh:
        existing = db_existing_unit_ids()
        before = len(units)
        units = [u for u in units if u["unit_id"] not in existing]
        print(f"Skipping {before - len(units)} already-extracted")

    if args.limit:
        units = units[:args.limit]

    print(f"\n=== EXTRACTING {len(units)} ===")
    batch: list[dict] = []
    for n, u in enumerate(units, 1):
        url = u["park_url"]
        status, text = tavily_extract(url)
        # If Tavily failed OR returned a page without any dog/leash/pet keywords,
        # fall back to Playwright (real Chromium can render JS + bypass Cloudflare).
        if status != "ok" or len(text) < 200 or not re.search(r"\b(dog|leash|pet)s?\b", (text or "")[:8000], re.I):
            print(f"  [{n}/{len(units)}] unit {u['unit_id']} {u['unit_name']!r}: tavily {status} (chars={len(text)}) -> trying Playwright")
            pw_status, pw_text = playwright_fetch(url)
            if pw_status.startswith("ok") and len(pw_text) >= 200:
                status, text = pw_status, pw_text
                print(f"      Playwright OK ({len(text)} chars)")
            else:
                print(f"      Playwright {pw_status}")
        if status not in ("ok","ok_playwright") or len(text) < 200:
            print(f"  [{n}/{len(units)}] unit {u['unit_id']} {u['unit_name']!r}: fetch_{status}")
            # still record a row so we don't re-try
            batch.append({
                "cpad_unit_id": u["unit_id"],
                "unit_name": u["unit_name"],
                "agency_name": u["agncy_name"],
                "url_used": url,
                "url_kind": "park_url",
                "default_rule": "unknown",
                "extraction_model": SONNET,
                "extraction_confidence": 0.0,
                "source_quote": f"fetch_failed: {status}",
            })
            if len(batch) >= 5:
                db_upsert(batch); batch.clear()
            continue

        cls = call_llm(text, u["unit_name"] or "")
        rule = (cls.get("default_rule") or "unknown").lower()
        if rule not in ("yes","no","restricted","unknown"):
            rule = "unknown"
        conf = float(cls.get("confidence") or 0)
        leash = cls.get("leash_required")
        if leash not in (True, False, None):
            leash = None

        # Area decomposition
        valid_areas = {"off_leash","on_leash","forbidden","unknown"}
        areas = cls.get("areas") or {}
        def san_area(name: str) -> str | None:
            v = (areas.get(name) or "").strip().lower().replace("-","_")
            return v if v in valid_areas else None

        print(f"  [{n}/{len(units)}] unit {u['unit_id']} {u['unit_name']!r}: {rule} (conf={conf:.2f}, leash={leash})")
        batch.append({
            "cpad_unit_id": u["unit_id"],
            "unit_name": u["unit_name"],
            "agency_name": u["agncy_name"],
            "url_used": url,
            "url_kind": "park_url",
            "dogs_allowed": rule,
            "default_rule": rule,
            "leash_required": leash,
            "exceptions": cls.get("exceptions") or None,
            "time_windows": cls.get("time_windows") or None,
            "seasonal_rules": cls.get("seasonal_rules") or None,
            "source_quote": (cls.get("source_quote") or "")[:1000],
            "ordinance_ref": cls.get("ordinance_ref") or None,
            "extraction_model": SONNET,
            "extraction_confidence": conf,
            "area_sand":        san_area("sand"),
            "area_water":       san_area("water"),
            "area_picnic_area": san_area("picnic_area"),
            "area_parking_lot": san_area("parking_lot"),
            "area_trails":      san_area("trails"),
            "area_campground":  san_area("campground"),
            "designated_dog_zones": (cls.get("designated_dog_zones") or "").strip() or None,
            "prohibited_areas":     (cls.get("prohibited_areas") or "").strip() or None,
        })
        if len(batch) >= 5:
            db_upsert(batch); batch.clear()
        time.sleep(SLEEP_S)

    if batch:
        db_upsert(batch)

    print("\nDone.")


if __name__ == "__main__":
    main()
