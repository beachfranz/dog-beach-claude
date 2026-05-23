import sys
sys.stdout.reconfigure(encoding="utf-8")
from playwright.sync_api import sync_playwright
URL = "https://slocountyparks.com/park-ordinances/"
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
    text = page.evaluate("() => document.body.innerText") or ""
    print(f"CHARS: {len(text)}")
    print(text[:5000])
    print("---LINKS---")
    links = page.evaluate("""() => Array.from(document.querySelectorAll('a[href]')).map(a => ({
        text: a.innerText.trim(), href: a.href})).filter(l => l.text)""")
    for l in links[:50]:
        if l['text']: print(f"  [{l['text'][:60]}] -> {l['href']}")
    b.close()
