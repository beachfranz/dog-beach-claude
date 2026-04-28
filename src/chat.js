// ── Shared Scout Chat UI ──────────────────────────────────────────
(function () {
  const cfg = window.CHAT_CONFIG || {};
  const placeholder = cfg.placeholder || 'Ask Scout...';

  // Product mentions in Scout's reply get auto-linked to Amazon search
  // (not affiliate URLs — switch to ASIN + tag= when you join the program).
  // Generous regexes to catch Scout's natural rephrasings.
  const PRODUCT_LINKS = [
    { regex: /\b(dog\s+)?(sunscreen|sunblock)\b/i,                        href: 'https://www.amazon.com/s?k=dog+sunscreen' },
    { regex: /\b(dog\s+)?(booties|boots)\b/i,                              href: 'https://www.amazon.com/s?k=dog+boots+for+hot+sand' },
    { regex: /\b(fetch\s+ball|tennis\s+ball|chuck(?:it|er))\b/i,           href: 'https://www.amazon.com/s?k=chuckit+ball+launcher' },
    { regex: /\b((long|leash)\s+leash|long\s+lead|training\s+leash|30(?:\s|-)?ft\s+leash)\b/i, href: 'https://www.amazon.com/s?k=long+dog+training+leash' },
    { regex: /\b(collapsible\s+)?(water\s+bowl|dog\s+bowl|travel\s+bowl)\b/i, href: 'https://www.amazon.com/s?k=collapsible+dog+water+bowl' },
    { regex: /\b(microfiber\s+)?(dog\s+towel|beach\s+towel)\b/i,           href: 'https://www.amazon.com/s?k=microfiber+dog+towel' },
  ];

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function withProductLinks(text) {
    // Escape FIRST so any markup in Scout's text is rendered as plain
    // text. Then link product mentions — first hit per product only,
    // to avoid spammy repeat-linking when Scout uses the same phrase
    // twice in one reply.
    let html = escapeHtml(text);
    for (const { regex, href } of PRODUCT_LINKS) {
      let used = false;
      html = html.replace(regex, (match) => {
        if (used) return match;
        used = true;
        return `<a class="chat-product-link" href="${href}" target="_blank" rel="noopener noreferrer">${match}</a>`;
      });
    }
    return html;
  }

  // Exposed so the narrative blurb on index.html / detail.html can run
  // Scout's reply through the same product-link injector.
  window.withProductLinks = withProductLinks;

  document.body.insertAdjacentHTML('beforeend', `
    <div class="chat-overlay" id="chat-overlay" onclick="toggleChat()"></div>
    <div class="chat-panel" id="chat-panel">
      <div class="chat-header">
        <div class="chat-header-left">
          <img src="src/avatar.png" class="chat-avatar" alt="">
          <span class="chat-header-title">Ask Scout</span>
        </div>
        <button class="chat-close" onclick="toggleChat()">&#x2715;</button>
      </div>
      <div class="chat-messages" id="chat-messages"></div>
      <div class="chat-input-row">
        <input class="chat-input" id="chat-input" type="text" placeholder="${placeholder}"
               onkeydown="if(event.key==='Enter')sendChatMessage()" />
        <button class="chat-send" onclick="sendChatMessage()">Ask Scout</button>
      </div>
    </div>
  `);

  const chatHistory = [];

  // Surfer-toned openers, picked at random when the panel first opens.
  // Not pushed into chatHistory — the LLM shouldn't think it greeted
  // the user; its first reply should answer the user's actual question.
  const GREETINGS = [
    "Hey! What's the call — questions about conditions, timing, or what to bring?",
    "Hey, stoked you stopped by. What do you wanna know?",
    "Hey! Anything I can help you scope out?",
  ];

  window.toggleChat = function () {
    const panel   = document.getElementById('chat-panel');
    const overlay = document.getElementById('chat-overlay');
    const isOpen  = panel.classList.contains('open');
    panel.classList.toggle('open', !isOpen);
    overlay.classList.toggle('open', !isOpen);
    if (!isOpen) {
      const msgs = document.getElementById('chat-messages');
      if (msgs && !msgs.children.length) {
        const greeting = GREETINGS[Math.floor(Math.random() * GREETINGS.length)];
        appendChatMessage('assistant', greeting);
      }
      document.getElementById('chat-input').focus();
    }
  };

  window.sendChatMessage = async function () {
    const input    = document.getElementById('chat-input');
    const question = input.value.trim();
    if (!question) return;

    input.value = '';
    appendChatMessage('user', question);
    chatHistory.push({ role: 'user', content: question });

    const typingId = appendTyping();
    const locationId = cfg.getLocationId ? cfg.getLocationId() : null;
    const localDate  = cfg.getLocalDate  ? cfg.getLocalDate()  : null;

    try {
      const body = { location_id: locationId, question, conversation_history: chatHistory.slice(0, -1) };
      if (localDate) body.local_date = localDate;
      const res = await fetch(`${SUPABASE_URL}/functions/v1/beach-chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${ANON_KEY}` },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      removeTyping(typingId);
      const answer = data.answer ?? data.error ?? 'Something went wrong.';
      appendChatMessage('assistant', answer);
      chatHistory.push({ role: 'assistant', content: answer });
    } catch {
      removeTyping(typingId);
      appendChatMessage('assistant', 'Could not reach the server. Try again.');
    }
  };

  function appendChatMessage(role, text) {
    const msgs = document.getElementById('chat-messages');
    const div  = document.createElement('div');
    div.className = `chat-msg chat-msg-${role}`;
    if (role === 'assistant') {
      div.innerHTML = withProductLinks(text);
    } else {
      div.textContent = text;
    }
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
    return div;
  }

  function appendTyping() {
    const msgs = document.getElementById('chat-messages');
    const div  = document.createElement('div');
    div.id = 'typing-' + Date.now();
    div.className = 'chat-msg chat-msg-assistant chat-typing';
    div.textContent = '...';
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
    return div.id;
  }

  function removeTyping(id) {
    document.getElementById(id)?.remove();
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      document.getElementById('chat-panel').classList.remove('open');
      document.getElementById('chat-overlay').classList.remove('open');
    }
  });

  // Close the chat panel when a product link is clicked. The user has
  // their answer and the link itself; on return from Amazon, no point
  // keeping the dialog open over the page.
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.chat-product-link')) return;
    const panel = document.getElementById('chat-panel');
    if (panel?.classList.contains('open')) {
      panel.classList.remove('open');
      document.getElementById('chat-overlay')?.classList.remove('open');
    }
  });
})();
