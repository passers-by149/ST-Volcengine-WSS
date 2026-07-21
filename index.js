/**
 * SillyTavern 火山引擎实时语音 TTS 插件 (前端)
 * 连接本地服务端插件 ws://127.0.0.1:8700，由服务端代理转发到火山引擎
 */
const EXTENSION_NAME = 'volcengine-tts';
const PROXY_URL = 'ws://127.0.0.1:8700';

// ---------- 事件常量 ----------
const EVENT_START_CONNECTION = 1, EVENT_FINISH_CONNECTION = 2;
const EVENT_START_SESSION = 100, EVENT_FINISH_SESSION = 102;
const EVENT_CHAT_TTS_TEXT = 500;
const EVENT_SESSION_STARTED = 150, EVENT_SESSION_FAILED = 153;
const EVENT_TTS_RESPONSE = 352, EVENT_TTS_ENDED = 359;
const EVENT_CONNECTION_STARTED = 50, EVENT_CONNECTION_FAILED = 51;
const EVENT_DIALOG_COMMON_ERROR = 599;

// ---------- 设置 ----------
const defaultSettings = {
    app_id: '', access_token: '', speaker: 'saturn_zh_female_aojiaonvyou_tob',
    sample_rate: 24000, host: 'openspeech.bytedance.com',
    path: '/api/v3/realtime/dialogue', resource_id: 'volc.speech.dialog',
    enabled: false, auto_read: false, streaming: true, read_user: false,
    sentence_split_chars: '。！？.!?\n',
};
let settings = { ...defaultSettings };

// ---------- 状态 ----------
let ws = null, sessionId = null, audioContext = null;
let isConnected = false, isSessionActive = false;
let pendingText = '', playedSentences = '';
const activeAudioSources = [];

// ---------- 音频 ----------
function getAudioContext() {
    if (!audioContext) audioContext = new (window.AudioContext || window.webkitAudioContext)();
    if (audioContext.state === 'suspended') audioContext.resume();
    return audioContext;
}
function stopAllAudio() {
    for (const s of activeAudioSources) { try { s.stop(); } catch (e) {} }
    activeAudioSources.length = 0;
}
function playPCM(pcmData, sampleRate) {
    const ctx = getAudioContext();
    const numSamples = pcmData.byteLength / 2;
    if (!numSamples) return;
    const buffer = ctx.createBuffer(1, numSamples, sampleRate);
    const channel = buffer.getChannelData(0);
    const view = new DataView(pcmData.buffer, pcmData.byteOffset, pcmData.byteLength);
    for (let i = 0; i < numSamples; i++) channel[i] = view.getInt16(i * 2, true) / 32768;
    const source = ctx.createBufferSource();
    source.buffer = buffer; source.connect(ctx.destination);
    source.onended = () => { const i = activeAudioSources.indexOf(source); if (i >= 0) activeAudioSources.splice(i, 1); };
    activeAudioSources.push(source); source.start();
}

// ---------- 设置持久化 ----------
function loadSettings() {
    try { const s = window.SillyTavern.getContext().extensionSettings[EXTENSION_NAME]; if (s) settings = { ...defaultSettings, ...s }; } catch (e) {}
}
function saveSettings() {
    try { const ctx = window.SillyTavern.getContext(); ctx.extensionSettings[EXTENSION_NAME] = settings; ctx.saveSettingsDebounced(); } catch (e) {}
}

// ---------- 二进制协议 ----------
function makeHeader(msgType, flag, serialization, compression) {
    return new Uint8Array([0x11, (msgType << 4) | flag, (serialization << 4) | compression, 0x00]);
}
function packJsonEvent(eventId, payload, sessionIdStr) {
    payload = payload || {};
    const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
    const header = makeHeader(0x1, 0x4, 0x1, 0x0);
    let totalLen = header.length + 4;
    let sidBytes = null;
    if (sessionIdStr !== undefined && sessionIdStr !== null) {
        sidBytes = new TextEncoder().encode(sessionIdStr);
        totalLen += 4 + sidBytes.length;
    }
    totalLen += 4 + payloadBytes.length;
    const buf = new ArrayBuffer(totalLen);
    const view = new DataView(buf);
    const arr = new Uint8Array(buf);
    let offset = 0;
    arr.set(header, offset); offset += header.length;
    view.setUint32(offset, eventId, false); offset += 4;
    if (sidBytes) { view.setUint32(offset, sidBytes.length, false); offset += 4; arr.set(sidBytes, offset); offset += sidBytes.length; }
    view.setUint32(offset, payloadBytes.length, false); offset += 4;
    arr.set(payloadBytes, offset);
    return buf;
}
function parseServerFrame(buffer) {
    const data = new Uint8Array(buffer);
    const view = new DataView(buffer);
    if (data.length < 4) return { msg_type: null, event: null, session_id: null, payload: data, error_code: null };
    const headerSize = (data[0] & 0x0F) * 4;
    const msgType = data[1] >> 4;
    const flag = data[1] & 0x0F;
    const serialization = data[2] >> 4;
    let pos = headerSize, event = null, sessionIdStr = null, errorCode = null;
    if (msgType === 0xF) {
        if (data.length >= pos + 4) { errorCode = view.getUint32(pos, false); pos += 4; }
        let payload = null;
        if (data.length >= pos + 4) { const size = view.getUint32(pos, false); pos += 4; payload = decodePayload(serialization, data.slice(pos, pos + size)); }
        return { msg_type: msgType, event, session_id: sessionIdStr, payload, error_code: errorCode };
    }
    if (flag === 0x4 && data.length >= pos + 4) {
        event = view.getUint32(pos, false); pos += 4;
        const CONNECTION_EVENTS = new Set([50, 51, 52]);
        if (!CONNECTION_EVENTS.has(event) && data.length >= pos + 4) {
            const possibleLen = view.getUint32(pos, false);
            if (possibleLen > 0 && possibleLen < 200 && data.length >= pos + 4 + possibleLen + 4) {
                try { sessionIdStr = new TextDecoder().decode(data.slice(pos + 4, pos + 4 + possibleLen)); pos += 4 + possibleLen; } catch (e) {}
            }
        }
    }
    let payload = null;
    if (data.length >= pos + 4) { const size = view.getUint32(pos, false); pos += 4; payload = decodePayload(serialization, data.slice(pos, pos + size)); }
    return { msg_type: msgType, event, session_id: sessionIdStr, payload, error_code: errorCode };
}
function decodePayload(serialization, bytes) {
    if (!bytes || bytes.length === 0) return null;
    if (serialization === 0x1) { try { return JSON.parse(new TextDecoder().decode(bytes)); } catch (e) { return new TextDecoder().decode(bytes); } }
    return bytes;
}

// ---------- 通过代理连接 ----------
async function connectWss() {
    if (ws && ws.readyState === WebSocket.OPEN) return ws;
    const appId = settings.app_id.trim();
    const accessToken = settings.access_token.trim();
    if (!appId || !accessToken) throw new Error('请填写 APP_ID 和 ACCESS_TOKEN');

    return new Promise((resolve, reject) => {
        const socket = new WebSocket(PROXY_URL);
        socket.binaryType = 'arraybuffer';
        const timeout = setTimeout(() => { socket.close(); reject(new Error('代理连接超时，请确认插件服务已启动')); }, 15000);
        let authSent = false;

        socket.onopen = () => {
            socket.send(JSON.stringify({
                appId, accessToken,
                host: settings.host, path: settings.path, resourceId: settings.resource_id,
                connectId: crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => { const r = Math.random() * 16 | 0; return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16); }),
            }));
            authSent = true;
        };

        socket.onmessage = (evt) => {
            if (typeof evt.data === 'string') {
                try {
                    const msg = JSON.parse(evt.data);
                    if (msg.type === 'ready') { clearTimeout(timeout); ws = socket; isConnected = true; resolve(socket); return; }
                    if (msg.type === 'error') { clearTimeout(timeout); socket.close(); reject(new Error(msg.message)); return; }
                } catch (e) {}
            }
        };

        socket.onerror = () => { clearTimeout(timeout); reject(new Error('无法连接代理 ws://127.0.0.1:8700')); };
        socket.onclose = (evt) => {
            if (!authSent) { clearTimeout(timeout); reject(new Error('代理连接被拒绝')); }
            ws = null; isConnected = false; isSessionActive = false;
        };
    });
}

// ---------- 会话管理 ----------
async function startSession() {
    if (isSessionActive) return;
    const socket = await connectWss();
    sessionId = crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => { const r = Math.random() * 16 | 0; return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16); });
    socket.send(packJsonEvent(EVENT_START_CONNECTION, {}));
    const r1 = await waitForEvent(socket, [EVENT_CONNECTION_STARTED, EVENT_CONNECTION_FAILED]);
    if (r1.event === EVENT_CONNECTION_FAILED) throw new Error('StartConnection 失败');
    socket.send(packJsonEvent(EVENT_START_SESSION, {
        dialog: { bot_name: '语音助手', extra: { input_mod: 'text', model: '2.2.0.0', strict_audit: true } },
        tts: { speaker: settings.speaker, audio_config: { channel: 1, format: 'pcm_s16le', sample_rate: settings.sample_rate } },
    }, sessionId));
    const r2 = await waitForEvent(socket, [EVENT_SESSION_STARTED, EVENT_SESSION_FAILED, EVENT_DIALOG_COMMON_ERROR]);
    if (r2.event !== EVENT_SESSION_STARTED) throw new Error('StartSession 失败');
    isSessionActive = true;
}
async function finishSession() {
    if (!isSessionActive || !ws) return;
    try { ws.send(packJsonEvent(EVENT_FINISH_SESSION, {}, sessionId)); ws.send(packJsonEvent(EVENT_FINISH_CONNECTION, {})); } catch (e) {}
    isSessionActive = false;
}
async function disconnect() {
    stopAllAudio();
    if (ws) {
        try { if (isSessionActive) { ws.send(packJsonEvent(EVENT_FINISH_SESSION, {}, sessionId)); ws.send(packJsonEvent(EVENT_FINISH_CONNECTION, {})); } ws.close(); } catch (e) {}
    }
    ws = null; isConnected = false; isSessionActive = false; sessionId = null;
    pendingText = ''; playedSentences = '';
}
function waitForEvent(socket, expectedEvents, timeoutMs = 20000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { socket.onmessage = orig; reject(new Error('等待服务端事件超时')); }, timeoutMs);
        const orig = socket.onmessage;
        socket.onmessage = (evt) => {
            const parsed = parseServerFrame(evt.data);
            if (expectedEvents.includes(parsed.event)) { clearTimeout(timer); socket.onmessage = orig; resolve(parsed); }
            else if (parsed.event === EVENT_DIALOG_COMMON_ERROR) { clearTimeout(timer); socket.onmessage = orig; resolve(parsed); }
        };
    });
}

// ---------- TTS 合成 ----------
async function synthesizeText(text) {
    if (!text || !text.trim()) return new ArrayBuffer(0);
    if (!isSessionActive) await startSession();
    const socket = ws;
    const chunks = [];
    let finished = false;
    const orig = socket.onmessage;
    socket.onmessage = (evt) => {
        const parsed = parseServerFrame(evt.data);
        console.log('[VolcTTS] TTS阶段收到事件:', parsed.event, 'payload类型:', parsed.payload ? (parsed.payload instanceof Uint8Array ? 'binary(' + parsed.payload.length + ')' : typeof parsed.payload) : 'null');
        if (parsed.event === EVENT_TTS_RESPONSE && parsed.payload instanceof Uint8Array) {
            chunks.push(parsed.payload);
            console.log('[VolcTTS] 音频块: ' + parsed.payload.length + ' bytes');
        } else if (parsed.event === EVENT_TTS_ENDED) {
            console.log('[VolcTTS] TTS 结束');
            finished = true;
        } else if (parsed.event === 351) {
            // EVENT_TTS_SENTENCE_END
            console.log('[VolcTTS] 句子结束');
        } else if (parsed.event === EVENT_DIALOG_COMMON_ERROR || parsed.event === EVENT_SESSION_FAILED) {
            console.log('[VolcTTS] 错误或失败');
            finished = true;
        }
    };
    try {
        socket.send(packJsonEvent(EVENT_CHAT_TTS_TEXT, { start: true, content: text, end: false }, sessionId));
        await sleep(50);
        socket.send(packJsonEvent(EVENT_CHAT_TTS_TEXT, { start: false, content: '', end: true }, sessionId));
        const start = Date.now();
        while (!finished && Date.now() - start < 30000) await sleep(100);
        console.log('[VolcTTS] TTS完成, 收集了 ' + chunks.length + ' 个音频块');
    } finally { socket.onmessage = orig; }
    if (chunks.length === 0) return new ArrayBuffer(0);
    const total = chunks.reduce((s, c) => s + c.length, 0);
    const result = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { result.set(c, off); off += c.length; }
    return result.buffer;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---------- 句子分割 ----------
function splitSentences(text) {
    const chars = settings.sentence_split_chars || '。！？.!?\n';
    const esc = chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return text.match(new RegExp(`[^${esc}]+[${esc}]?`, 'g')) || [text];
}
function getNewSentences(fullText) {
    if (!fullText) return [];
    const newText = fullText.substring(playedSentences.length);
    if (!newText.trim()) return [];
    const sentences = splitSentences(newText);
    const complete = [];
    let lastEnd = 0;
    for (const s of sentences) {
        if (settings.sentence_split_chars.includes(s.charAt(s.length - 1)) || s.endsWith('\n')) {
            complete.push(s); lastEnd += s.length;
        } else { break; }
    }
    if (complete.length) playedSentences += newText.substring(0, lastEnd);
    return complete;
}

// ---------- 朗读 ----------
let speakQueue = [];
let speakRunning = false;

async function processSpeakQueue() {
    if (speakRunning) return;
    speakRunning = true;
    while (speakQueue.length > 0) {
        const text = speakQueue.shift();
        if (!text || !text.trim()) continue;
        try {
            const pcm = await synthesizeText(text);
            if (pcm && pcm.byteLength) playPCM(pcm, settings.sample_rate);
        } catch (err) {
            console.error('[VolcTTS] 逐句失败:', err);
        }
    }
    speakRunning = false;
}

/** 非阻塞：加入队列，不等待 */
function enqueueSpeak(text) {
    if (!text || !text.trim()) return;
    speakQueue.push(text);
    processSpeakQueue(); // 不 await，不阻塞调用方
}

async function speakText(text) {
    if (!text || !text.trim() || !settings.enabled) return;
    try {
        stopAllAudio();
        const pcm = await synthesizeText(text);
        if (pcm && pcm.byteLength) playPCM(pcm, settings.sample_rate);
    } catch (err) { console.error('[VolcTTS] 合成失败:', err); await disconnect(); }
}

// ---------- 事件监听 ----------
async function setupEventListeners() {
    let eventSource, event_types;
    try {
        const m = await import('../../../events.js');
        eventSource = m.eventSource; event_types = m.event_types;
    } catch (e) {
        try {
            const m = await import('../../../../script.js');
            eventSource = m.eventSource; event_types = m.event_types;
        } catch (e2) { console.warn('[VolcTTS] 事件系统不可用'); return; }
    }
    if (!eventSource || !event_types) return;

    eventSource.on(event_types.GENERATION_STARTED, async () => {
        if (!settings.enabled || !settings.auto_read) return;
        await disconnect();
        speakQueue = []; speakRunning = false;
        playedSentences = ''; pendingText = '';
        if (settings.streaming) { try { await startSession(); } catch (err) {} }
    });
    eventSource.on(event_types.STREAM_TOKEN_RECEIVED, (token) => {
        if (!settings.enabled || !settings.auto_read || !settings.streaming || !isSessionActive) return;
        pendingText += (typeof token === 'string' ? token : '');
        const sentences = getNewSentences(pendingText);
        for (const s of sentences) enqueueSpeak(s);
    });
    eventSource.on(event_types.GENERATION_ENDED, async () => {
        if (!settings.enabled || !settings.auto_read) return;
        if (settings.streaming) {
            const remaining = pendingText.substring(playedSentences.length);
            if (remaining.trim()) enqueueSpeak(remaining);
            // 等待队列播放完再断开会话
            while (speakQueue.length > 0 || speakRunning) await sleep(200);
            await finishSession(); await disconnect();
        } else {
            try {
                const ctx = window.SillyTavern.getContext();
                const last = ctx.chat?.[ctx.chat.length - 1];
                if (last && !last.is_user && !last.is_system) await speakText(last.mes);
            } catch (err) {}
        }
    });
    eventSource.on(event_types.MESSAGE_SENT, async (id) => {
        if (!settings.enabled || !settings.auto_read || !settings.read_user) return;
        try { const ctx = window.SillyTavern.getContext(); const msg = ctx.chat?.[id]; if (msg && msg.is_user) await speakText(msg.mes); } catch (err) {}
    });
    eventSource.on(event_types.CHAT_CHANGED, () => disconnect());
    console.log('[VolcTTS] 事件监听已注册');
}

// ---------- 设置面板 ----------
function renderSettingsUI() {
    const container = document.getElementById('extensions_settings');
    if (!container) { setTimeout(renderSettingsUI, 500); return; }
    if (document.getElementById('volcengine_tts_settings')) { updateSettingsUI(); return; }

    container.insertAdjacentHTML('beforeend', `
<div id="volcengine_tts_settings" class="volcengine-tts-panel">
  <div class="inline-drawer">
    <div class="inline-drawer-toggle inline-drawer-header">
      <b>火山引擎实时语音 TTS</b>
      <span class="inline-drawer-icon fa-solid fa-circle-chevron-down"></span>
    </div>
    <div class="inline-drawer-content">
      <p class="volc-tts-desc">服务端插件自动运行代理，无需手动开终端。支持流式逐句朗读。</p>
      <div class="volc-tts-section"><h4>连接设置</h4>
        <div class="volc-tts-row"><label>APP_ID</label><input type="text" id="volc_tts_app_id" placeholder="火山引擎 App ID"></div>
        <div class="volc-tts-row"><label>ACCESS_TOKEN</label><input type="password" id="volc_tts_access_token" placeholder="火山引擎 Access Token"></div>
        <div class="volc-tts-row"><label>服务地址</label><input type="text" id="volc_tts_host" placeholder="openspeech.bytedance.com"></div>
        <div class="volc-tts-row"><label>接口路径</label><input type="text" id="volc_tts_path" placeholder="/api/v3/realtime/dialogue"></div>
        <div class="volc-tts-row"><label>Resource ID</label><input type="text" id="volc_tts_resource_id" placeholder="volc.speech.dialog"></div>
      </div>
      <div class="volc-tts-section"><h4>语音设置</h4>
        <div class="volc-tts-row"><label>说话人</label><input type="text" id="volc_tts_speaker" placeholder="saturn_zh_female_aojiaonvyou_tob"></div>
        <div class="volc-tts-row"><label>采样率</label><input type="number" id="volc_tts_sample_rate" min="8000" max="48000" step="1000"></div>
        <div class="volc-tts-row"><label>句子分隔符</label><input type="text" id="volc_tts_split_chars" placeholder="。！？.!?"></div>
      </div>
      <div class="volc-tts-section"><h4>功能开关</h4>
        <div class="volc-tts-row"><label class="volc-tts-checkbox"><input type="checkbox" id="volc_tts_enabled"><span>启用 TTS</span></label></div>
        <div class="volc-tts-row"><label class="volc-tts-checkbox"><input type="checkbox" id="volc_tts_auto_read"><span>AI 回复时自动朗读</span></label></div>
        <div class="volc-tts-row"><label class="volc-tts-checkbox"><input type="checkbox" id="volc_tts_streaming"><span>流式逐句朗读</span></label></div>
        <div class="volc-tts-row"><label class="volc-tts-checkbox"><input type="checkbox" id="volc_tts_read_user"><span>也朗读用户消息</span></label></div>
      </div>
      <div class="volc-tts-section">
        <button id="volc_tts_test_btn" class="volc-tts-btn">测试连接 & 朗读</button>
        <button id="volc_tts_stop_btn" class="volc-tts-btn volc-tts-btn-danger">停止</button>
        <span id="volc_tts_status" class="volc-tts-status"></span>
      </div>
      <div class="volc-tts-section">
        <button id="volc_tts_narrate_btn" class="volc-tts-btn">朗读最后一条 AI 消息</button>
      </div>
    </div>
  </div>
</div>`);
    bindEvents();
    updateSettingsUI();
}

function bindEvents() {
    for (const [id, key] of Object.entries({ volc_tts_app_id: 'app_id', volc_tts_access_token: 'access_token', volc_tts_host: 'host', volc_tts_path: 'path', volc_tts_resource_id: 'resource_id', volc_tts_speaker: 'speaker', volc_tts_split_chars: 'sentence_split_chars' })) {
        const el = document.getElementById(id); if (el) el.addEventListener('input', () => { settings[key] = el.value; saveSettings(); });
    }
    const sr = document.getElementById('volc_tts_sample_rate'); if (sr) sr.addEventListener('input', () => { settings.sample_rate = parseInt(sr.value) || 24000; saveSettings(); });
    for (const [id, key] of Object.entries({ volc_tts_enabled: 'enabled', volc_tts_auto_read: 'auto_read', volc_tts_streaming: 'streaming', volc_tts_read_user: 'read_user' })) {
        const el = document.getElementById(id); if (el) el.addEventListener('change', () => { settings[key] = el.checked; saveSettings(); });
    }
    document.getElementById('volc_tts_test_btn')?.addEventListener('click', async () => {
        setStatus('正在连接...');
        try { await disconnect(); await startSession(); setStatus('已连接，合成测试...'); const pcm = await synthesizeText('你好，这是语音合成测试。'); if (pcm && pcm.byteLength) { playPCM(pcm, settings.sample_rate); setStatus('测试成功！'); } else setStatus('未收到音频'); } catch (err) { setStatus('失败: ' + err.message); }
    });
    document.getElementById('volc_tts_stop_btn')?.addEventListener('click', async () => { stopAllAudio(); await disconnect(); setStatus('已停止'); });
    document.getElementById('volc_tts_narrate_btn')?.addEventListener('click', async () => {
        try {
            const ctx = window.SillyTavern.getContext(); const chat = ctx.chat || [];
            let last = null; for (let i = chat.length - 1; i >= 0; i--) { if (!chat[i].is_user && !chat[i].is_system) { last = chat[i]; break; } }
            if (last) { setStatus('朗读中...'); stopAllAudio(); await startSession(); const pcm = await synthesizeText(last.mes); if (pcm && pcm.byteLength) { playPCM(pcm, settings.sample_rate); setStatus('播放中'); } } else setStatus('无AI消息');
        } catch (err) { setStatus('失败: ' + err.message); }
    });
}
function updateSettingsUI() {
    for (const [id, key] of Object.entries({ volc_tts_app_id: 'app_id', volc_tts_access_token: 'access_token', volc_tts_host: 'host', volc_tts_path: 'path', volc_tts_resource_id: 'resource_id', volc_tts_speaker: 'speaker', volc_tts_split_chars: 'sentence_split_chars' })) { const el = document.getElementById(id); if (el) el.value = settings[key] || ''; }
    const sr = document.getElementById('volc_tts_sample_rate'); if (sr) sr.value = settings.sample_rate;
    for (const [id, key] of Object.entries({ volc_tts_enabled: 'enabled', volc_tts_auto_read: 'auto_read', volc_tts_streaming: 'streaming', volc_tts_read_user: 'read_user' })) { const el = document.getElementById(id); if (el) el.checked = settings[key]; }
}
function setStatus(msg) { const el = document.getElementById('volc_tts_status'); if (el) el.textContent = msg; }

// ---------- 入口 ----------
(async function init() {
    if (!window.SillyTavern) { setTimeout(init, 1000); return; }
    loadSettings();
    renderSettingsUI();
    await setupEventListeners();
    console.log('[VolcTTS] 插件初始化完成');
})();