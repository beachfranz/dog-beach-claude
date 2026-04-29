"""Inspect Oxnard FAQ page to identify the accordion pattern."""
import re, sys
sys.stdout.reconfigure(encoding="utf-8")
from playwright.sync_api import sync_playwright

URL = "https://www.oxnard.org/city-department/public-works/parks/parks-faq/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def probe():
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
        page = ctx.new_page()
        page.route("**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,otf,mp4,webm}",
                   lambda r: r.abort())
        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        try: page.wait_for_load_state("networkidle", timeout=20000)
        except: pass
        page.wait_for_timeout(2000)

        # Pre-expand snapshot
        text_before = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
        print(f"BEFORE EXPAND — {len(text_before)} chars")
        m = re.search(r"dogs?.{0,200}", text_before, re.I)
        if m: print(f"  dog mention: {m.group(0)[:200]}")

        # Look for the FAQ structure around 'dogs allowed on the beach'
        struct = page.evaluate("""() => {
            const html = document.body.innerHTML;
            // Find context around 'dogs allowed on the beach'
            const idx = html.toLowerCase().indexOf('dogs allowed');
            if (idx === -1) return null;
            return html.substring(Math.max(0, idx - 500), idx + 1500);
        }""")
        print("\nDOM around 'dogs allowed':")
        print((struct or "[not found]")[:2500])

        # Try clicking common accordion patterns
        # 1. <details> tags
        details = page.locator("details").count()
        print(f"\n<details> tags: {details}")
        # 2. Bootstrap collapse
        collapse = page.locator("[data-bs-toggle='collapse'], [data-toggle='collapse']").count()
        print(f"data-toggle=collapse: {collapse}")
        # 3. role=button with aria-expanded
        expandable = page.locator("[aria-expanded='false']").count()
        print(f"aria-expanded=false: {expandable}")
        # 4. .accordion class
        accordion = page.locator(".accordion, .accordion-toggle, .accordion-button").count()
        print(f".accordion*: {accordion}")
        # 5. Generic h*+button patterns
        h_btn = page.locator("button[type='button']").count()
        print(f"button[type=button]: {h_btn}")

        # Try expanding all details + clicking any aria-expanded=false
        page.evaluate("""() => {
            document.querySelectorAll('details').forEach(d => d.open = true);
            document.querySelectorAll('[aria-expanded="false"]').forEach(b => {
                try { b.click(); } catch(e) {}
            });
        }""")
        page.wait_for_timeout(2000)
        text_after = page.evaluate("() => document.body ? document.body.innerText : ''") or ""
        print(f"\nAFTER EXPAND — {len(text_after)} chars (delta: {len(text_after) - len(text_before):+d})")
        m = re.search(r".{200}dogs?.{500}", text_after, re.I)
        if m: print(f"\n  dog context:\n{m.group(0)[:800]}")

        browser.close()

if __name__ == "__main__":
    probe()
