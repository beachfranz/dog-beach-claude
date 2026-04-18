// ── Shared Scout Chat UI ──────────────────────────────────────────
(function () {
  const cfg = window.CHAT_CONFIG || {};
  const placeholder = cfg.placeholder || 'Ask Scout...';

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

  window.toggleChat = function () {
    const panel   = document.getElementById('chat-panel');
    const overlay = document.getElementById('chat-overlay');
    const isOpen  = panel.classList.contains('open');
    panel.classList.toggle('open', !isOpen);
    overlay.classList.toggle('open', !isOpen);
    if (!isOpen) document.getElementById('chat-input').focus();
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

    try {
      const res = await fetch(`${SUPABASE_URL}/functions/v1/beach-chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${ANON_KEY}` },
        body: JSON.stringify({ location_id: locationId, question, conversation_history: chatHistory.slice(0, -1) }),
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
    div.textContent = text;
    if (role === 'assistant') {
      msgs.appendChild(div);
    } else {
      msgs.appendChild(div);
    }
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
})();
