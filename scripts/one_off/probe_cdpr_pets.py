"""Inspect CDPR's pets-in-parks page (parks.ca.gov page_id=541)."""
import re, sys
sys.stdout.reconfigure(encoding="utf-8")
from playwright.sync_api import sync_playwright

URL = "https://www.parks.ca.gov/?page_id=541"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

with sync_playwright() as p:
    b = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])
    ctx = b.new_context(user_agent=UA, viewport={"width":1280,"height":1600})
    page = ctx.new_page()
    page.goto(URL, wait_until="domcontentloaded", timeout=30000)
    try: page.wait_for_load_state("networkidle", timeout=20000)
    except: pass
    page.wait_for_timeout(2000)
    # Try to expand any collapsed content (we improved fetch_playwright for this)
    page.evaluate("""() => {
        document.querySelectorAll('details').forEach(d => d.open = true);
        document.querySelectorAll('[aria-expanded="false"]').forEach(b => b.setAttribute('aria-expanded','true'));
        document.querySelectorAll('.accordion-block-panel,.accordion-panel,.accordion-content,.accordion-body,.collapse,.panel-collapse,[aria-hidden="true"]').forEach(el => {
            el.style.display='block';el.style.visibility='visible';el.style.height='auto';el.style.maxHeight='none';el.removeAttribute('hidden');
        });
    }""")
    page.wait_for_timeout(1000)
    text = page.evaluate("() => document.body.innerText") or ""
    title = page.title()
    print(f"TITLE: {title}")
    print(f"CHARS: {len(text)}")
    # Look for keyword density
    print(f"keyword counts: dog={len(re.findall(r'\\bdog', text, re.I))}, leash={len(re.findall(r'\\bleash', text, re.I))}, beach={len(re.findall(r'\\bbeach', text, re.I))}")
    print("=" * 70)
    # Print first 8000 chars
    print(text[:8000])
    b.close()
