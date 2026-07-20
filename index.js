/**
 * SillyTavern 火山引擎实时语音 TTS 插件
 * 
 * 纯浏览器端实现，通过 WebSocket + 查询参数鉴权直接连接火山引擎语音服务。
 * 不需要 Python/Pydroid 后台进程。
 * 
 * 鉴权方式：浏览器 WebSocket 不支持自定义 Header，
 * 因此通过 STS 端点获取 JWT Token，再以 URL 查询参数方式传递鉴权信息。
 */

// ============================================================
// 事件常量（与火山引擎协议对齐）
// ============================================================
const EVENT_START_CONNECTION = 1;
const EVENT_FINISH_CONNECTION = 2;
const EVENT_START_SESSION = 100;
const EVENT_FINISH_SESSION = 102;
const EVENT_CHAT_TTS_TEXT = 500;
const EVENT_TTS_SENTENCE_START = 350;
const EVENT_TTS_SENTENCE_END = 351;
const EVENT_TTS_RESPONSE = 352;
const EVENT_TTS_ENDED = 359;
const EVENT_SESSION_STARTED = 150;
const EVENT_SESSION_FAILED = 153;
const EVENT_CONNECTION_STARTED = 50;
const EVENT_CONNECTION_FAILED = 51;
const EVENT_CONNECTION_FINISHED = 52;
const EVENT_DIALOG_COMMON_ERROR = 599;

// ============================================================
// 默认设置
// ============================================================
const EXTENSION_NAME = 'volcengine-tts';

const defaultSettings = {
    app_id: '',
    access_token: '',
    speaker: 'saturn_zh_female_aojiaonvyou_tob',
    sample_rate: 24000,
    host: 'openspeech.bytedance.com',
    path: '/api/v3/realtime/dialogue',
    resource_id: 'volc.speech.dialog',
    // 功能开关
    enabled: false,
    auto_read: false,       // AI 回复时自动朗读
    read_user: false,       // 也朗读用户消息
    streaming: true,        // 流式逐句朗读（关闭则等完整回复再读）
    sentence_split_chars: '。！？.!?\n',
};

let settings = { ...defaultSettings };

// ============================================================
// 运行时状态
// ============================================================
let ws = null;
let sessionId = null;
let jwtToken = null;
let jwtTokenExpiry = 0;
let audioContext = null;
let isConnecting = false;
let isConnected = false;
let isSessionActive = false;
let pendingText = '';          // 流式模式下累积的文本
let playedSentences = '';      // 已播放的文本（用于增量检测）
let activeAudioSources = [];   // 当前正在播放的音频源
let connectionPromise = null;
let sessionPromise = null;

// ============================================================
// 工具函数
// ============================================================

/** 获取或创建 AudioContext */
function getAudioContext() {
    if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
    if (audioContext.state === 'suspended') {
        audioContext.resume();
    }
    return audioContext;
}

/** 停止所有正在播放的音频 */
function stopAllAudio() {
    for (const src of activeAudioSources) {
        try { src.stop(); } catch (e) { /* 忽略 */ }
    }
    activeAudioSources = [];
}

/** 播放 PCM 16-bit signed little-endian 音频数据 */
function playPCM(pcmData, sampleRate) {
    const ctx = getAudioContext();
    const numSamples = pcmData.byteLength / 2;
    if (numSamples === 0) return;

    const buffer = ctx.createBuffer(1, numSamples, sampleRate);
    const channel = buffer.getChannelData(0);
    const view = new DataView(
        pcmData.buffer,
        pcmData.byteOffset,
        pcmData.byteLength
    );

    for (let i = 0; i < numSamples; i++) {
        channel[i] = view.getInt16(i * 2, true) / 32768.0;
    }

    const source = ctx.createBufferSource();
    source.buffer = buffer;
    source.connect(ctx.destination);

    source.onended = () => {
        const idx = activeAudioSources.indexOf(source);
        if (idx >= 0) activeAudioSources.splice(idx, 1);
    };

    activeAudioSources.push(source);
    source.start();
    return source;
}

/** 获取 UI 容器元素 */
function getContainer() {
    return document.getElementById('volcengine_tts_settings');
}

// ============================================================
// 设置持久化
// ============================================================

function loadSettings() {
    try {
        const context = window.SillyTavern.getContext();
        const saved = context.extensionSettings[EXTENSION_NAME];
        if (saved) {
            settings = { ...defaultSettings, ...saved };
        }
    } catch (e) {
        console.warn('[VolcTTS] 加载设置失败:', e);
    }
}

function saveSettings() {
    try {
        const context = window.SillyTavern.getContext();
        context.extensionSettings[EXTENSION_NAME] = settings;
        context.saveSettingsDebounced();
    } catch (e) {
        console.warn('[VolcTTS] 保存设置失败:', e);
    }
}

// ============================================================
// JWT Token 获取（STS 端点）
// ============================================================

async function fetchJwtToken() {
    const now = Date.now();
    if (jwtToken && now < jwtTokenExpiry - 60000) {
        return jwtToken; // 缓存有效（提前1分钟刷新）
    }

    const appId = settings.app_id.trim();
    const accessToken = settings.access_token.trim();

    if (!appId || !accessToken) {
        throw new Error('请先在设置面板填写 APP_ID 和 ACCESS_TOKEN');
    }

    console.log('[VolcTTS] 正在获取 JWT Token...');

    const resp = await fetch('https://openspeech.bytedance.com/api/v1/sts/token', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer; ${accessToken}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            appid: appId,
            duration: 1800, // 30分钟
        }),
    });

    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`STS 端点返回错误 ${resp.status}: ${text}`);
    }

    const data = await resp.json();
    jwtToken = data.jwt_token;
    jwtTokenExpiry = now + 1800 * 1000;

    if (!jwtToken) {
        throw new Error('STS 端点未返回 jwt_token');
    }

    console.log('[VolcTTS] JWT Token 获取成功');
    return jwtToken;
}

// ============================================================
// WebSocket 连接
// ============================================================

function buildWssUrl() {
    const appId = settings.app_id.trim();
    const host = settings.host.trim();
    const path = settings.path.trim();
    const resourceId = settings.resource_id.trim();

    const params = new URLSearchParams();
    params.set('api_resource_id', resourceId);
    params.set('api_app_key', appId);
    params.set('api_access_key', `Jwt; ${jwtToken}`);

    return `wss://${host}${path}?${params.toString()}`;
}

async function connectWss() {
    if (ws && ws.readyState === WebSocket.OPEN) {
        return ws;
    }

    await fetchJwtToken();

    const url = buildWssUrl();
    console.log('[VolcTTS] 正在连接 WebSocket...');

    return new Promise((resolve, reject) => {
        const socket = new WebSocket(url);
        socket.binaryType = 'arraybuffer';

        const timeout = setTimeout(() => {
            socket.close();
            reject(new Error('WebSocket 连接超时（15秒）'));
        }, 15000);

        socket.onopen = () => {
            clearTimeout(timeout);
            console.log('[VolcTTS] WebSocket 已连接');
            ws = socket;
            isConnected = true;
            resolve(socket);
        };

        socket.onerror = (err) => {
            clearTimeout(timeout);
            console.error('[VolcTTS] WebSocket 错误:', err);
            reject(new Error('WebSocket 连接失败，请检查网络和设置'));
        };

        socket.onclose = (evt) => {
            console.log('[VolcTTS] WebSocket 已关闭, code:', evt.code);
            ws = null;
            isConnected = false;
            isSessionActive = false;
        };

        socket.onmessage = (evt) => {
            // 由上层处理
        };
    });
}

// ============================================================
// 二进制协议（与 Python 代码完全对齐）
// ============================================================

function makeHeader(messageType, flag, serialization, compression) {
    return new Uint8Array([
        0x11,
        (messageType << 4) | flag,
        (serialization << 4) | compression,
        0x00,
    ]);
}

function packJsonEvent(eventId, payload, sessionIdStr) {
    payload = payload || {};

    const payloadBytes = new TextEncoder().encode(
        JSON.stringify(payload)
    );

    const header = makeHeader(0x1, 0x4, 0x1, 0x0);

    // 计算总长度
    let totalLen = header.length;       // 4
    totalLen += 4;                       // event_id (uint32)
    let sessionIdBytes = null;
    if (sessionIdStr !== undefined && sessionIdStr !== null) {
        sessionIdBytes = new TextEncoder().encode(sessionIdStr);
        totalLen += 4 + sessionIdBytes.length; // length + session_id
    }
    totalLen += 4 + payloadBytes.length; // payload length + payload

    const buf = new ArrayBuffer(totalLen);
    const view = new DataView(buf);
    const arr = new Uint8Array(buf);

    let offset = 0;
    arr.set(header, offset); offset += header.length;

    // event_id
    view.setUint32(offset, eventId, false); offset += 4;

    // session_id
    if (sessionIdBytes) {
        view.setUint32(offset, sessionIdBytes.length, false); offset += 4;
        arr.set(sessionIdBytes, offset); offset += sessionIdBytes.length;
    }

    // payload
    view.setUint32(offset, payloadBytes.length, false); offset += 4;
    arr.set(payloadBytes, offset);

    return buf;
}

function parseServerFrame(buffer) {
    const data = new Uint8Array(buffer);
    const view = new DataView(buffer);

    if (data.length < 4) {
        return {
            msg_type: null, event: null, session_id: null,
            payload: data, error_code: null,
        };
    }

    const headerSize = (data[0] & 0x0F) * 4;
    const msgType = data[1] >> 4;
    const flag = data[1] & 0x0F;
    const serialization = data[2] >> 4;

    let pos = headerSize;
    let event = null;
    let sessionIdStr = null;
    let errorCode = null;
    // 错误消息（msg_type = 0xF）
    if (msgType === 0xF) {
        if (data.length >= pos + 4) {
            errorCode = view.getUint32(pos, false);
            pos += 4;
        }
        let payload = null;
        if (data.length >= pos + 4) {
            const size = view.getUint32(pos, false);
            pos += 4;
            payload = decodePayload(serialization, data.slice(pos, pos + size));
        }
        return { msg_type: msgType, event, session_id: sessionIdStr, payload, error_code: errorCode };
    }

    if (flag === 0x4 && data.length >= pos + 4) {
        event = view.getUint32(pos, false);
        pos += 4;

        // 尝试解析 session_id
        const CONNECTION_EVENTS = new Set([50, 51, 52]);
        if (!CONNECTION_EVENTS.has(event) && data.length >= pos + 4) {
            const possibleLen = view.getUint32(pos, false);
            if (possibleLen > 0 && possibleLen < 200 && data.length >= pos + 4 + possibleLen + 4) {
                const sidBytes = data.slice(pos + 4, pos + 4 + possibleLen);
                try {
                    sessionIdStr = new TextDecoder().decode(sidBytes);
                    pos += 4 + possibleLen;
                } catch (e) {
                    // 不是 session_id
                }
            }
        }
    }

    let payload = null;
    if (data.length >= pos + 4) {
        const size = view.getUint32(pos, false);
        pos += 4;
        payload = decodePayload(serialization, data.slice(pos, pos + size));
    }

    return { msg_type: msgType, event, session_id: sessionIdStr, payload, error_code: errorCode };
}

function decodePayload(serialization, bytes) {
    if (!bytes || bytes.length === 0) return null;
    if (serialization === 0x1) {
        try {
            return JSON.parse(new TextDecoder().decode(bytes));
        } catch (e) {
            return new TextDecoder().decode(bytes);
        }
    }
    return bytes;
}

// ============================================================
// 会话管理
// ============================================================

async function startSession() {
    if (isSessionActive) return;

    const socket = await connectWss();
    sessionId = crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });

    // 1. StartConnection
    console.log('[VolcTTS] 发送 StartConnection...');
    socket.send(packJsonEvent(EVENT_START_CONNECTION, {}));

    const resp1 = await waitForEvent(socket, [EVENT_CONNECTION_STARTED, EVENT_CONNECTION_FAILED]);
    if (resp1.event === EVENT_CONNECTION_FAILED) {
        throw new Error('StartConnection 失败: ' + JSON.stringify(resp1.payload));
    }
    console.log('[VolcTTS] StartConnection 成功');

    // 2. StartSession
    const startSessionPayload = {
        dialog: {
            bot_name: '语音助手',
            extra: {
                input_mod: 'text',
                model: '2.2.0.0',
                strict_audit: true,
            },
        },
        tts: {
            speaker: settings.speaker,
            audio_config: {
                channel: 1,
                format: 'pcm_s16le',
                sample_rate: settings.sample_rate,
            },
        },
    };

    console.log('[VolcTTS] 发送 StartSession...');
    socket.send(packJsonEvent(EVENT_START_SESSION, startSessionPayload, sessionId));

    const resp2 = await waitForEvent(socket, [
        EVENT_SESSION_STARTED, EVENT_SESSION_FAILED, EVENT_DIALOG_COMMON_ERROR,
    ]);
    if (resp2.event !== EVENT_SESSION_STARTED) {
        throw new Error('StartSession 失败: ' + JSON.stringify(resp2.payload));
    }

    isSessionActive = true;
    console.log('[VolcTTS] 会话已启动, session_id:', sessionId);
}

async function finishSession() {
    if (!isSessionActive || !ws) return;
    try {
        ws.send(packJsonEvent(EVENT_FINISH_SESSION, {}, sessionId));
        ws.send(packJsonEvent(EVENT_FINISH_CONNECTION, {}));
    } catch (e) { /* 忽略 */ }
    isSessionActive = false;
}

async function disconnect() {
    stopAllAudio();
    if (ws) {
        try {
            if (isSessionActive) {
                ws.send(packJsonEvent(EVENT_FINISH_SESSION, {}, sessionId));
                ws.send(packJsonEvent(EVENT_FINISH_CONNECTION, {}));
            }
            ws.close();
        } catch (e) { /* 忽略 */ }
    }
    ws = null;
    isConnected = false;
    isSessionActive = false;
    sessionId = null;
    pendingText = '';
    playedSentences = '';
    activeAudioSources = [];
}

function waitForEvent(socket, expectedEvents, timeoutMs = 20000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            socket.onmessage = originalHandler;
            reject(new Error('等待服务端事件超时'));
        }, timeoutMs);

        const originalHandler = socket.onmessage;
        socket.onmessage = (evt) => {
            const parsed = parseServerFrame(evt.data);
            if (expectedEvents.includes(parsed.event)) {
                clearTimeout(timer);
                socket.onmessage = originalHandler;
                resolve(parsed);
            } else if (parsed.event === EVENT_DIALOG_COMMON_ERROR) {
                clearTimeout(timer);
                socket.onmessage = originalHandler;
                resolve(parsed);
            } else {
                // 打印其他事件用于调试
                console.log('[VolcTTS] 收到事件:', parsed.event, parsed.payload);
            }
        };
    });
}

// ============================================================
// TTS 合成（发送文本，接收音频）
// ============================================================

/**
 * 发送一段文本进行 TTS 合成，返回收集到的 PCM 音频数据
 */
async function synthesizeText(text) {
    if (!text || !text.trim()) return new ArrayBuffer(0);

    if (!isSessionActive) {
        await startSession();
    }

    const socket = ws;
    const audioChunks = [];
    let finished = false;

    // 设置消息处理器
    const originalHandler = socket.onmessage;
    socket.onmessage = (evt) => {
        const parsed = parseServerFrame(evt.data);

        if (parsed.event === EVENT_TTS_RESPONSE && parsed.payload instanceof Uint8Array) {
            audioChunks.push(parsed.payload);
        } else if (parsed.event === EVENT_TTS_ENDED || parsed.event === EVENT_TTS_SENTENCE_END) {
            finished = true;
        } else if (parsed.event === EVENT_DIALOG_COMMON_ERROR) {
            console.error('[VolcTTS] 服务端错误:', parsed.payload);
            finished = true;
        } else if (parsed.event === EVENT_SESSION_FAILED) {
            console.error('[VolcTTS] 会话失败:', parsed.payload);
            finished = true;
        }
    };

    try {
        // 发送 ChatTTSText
        socket.send(packJsonEvent(EVENT_CHAT_TTS_TEXT, {
            start: true,
            content: text,
            end: false,
        }, sessionId));

        await sleep(50);

        socket.send(packJsonEvent(EVENT_CHAT_TTS_TEXT, {
            start: false,
            content: '',
            end: true,
        }, sessionId));

        // 等待音频数据
        const startTime = Date.now();
        while (!finished && Date.now() - startTime < 30000) {
            await sleep(100);
        }
    } finally {
        socket.onmessage = originalHandler;
    }

    // 合并所有音频块
    if (audioChunks.length === 0) return new ArrayBuffer(0);

    const totalLen = audioChunks.reduce((sum, c) => sum + c.length, 0);
    const result = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of audioChunks) {
        result.set(chunk, offset);
        offset += chunk.length;
    }

    return result.buffer;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================
// 句子分割
// ============================================================

function splitSentences(text) {
    const chars = settings.sentence_split_chars || '。！？.!?\n';
    const regex = new RegExp(`[^${chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}]+[${chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}]?`, 'g');
    return text.match(regex) || [text];
}

function getNewSentences(fullText) {
    if (!fullText) return [];
    const newText = fullText.substring(playedSentences.length);
    if (!newText.trim()) return [];

    const sentences = splitSentences(newText);
    const complete = [];
    let lastCompleteEnd = 0;

    for (const s of sentences) {
        const lastChar = s.charAt(s.length - 1);
        if (settings.sentence_split_chars.includes(lastChar) || lastChar === '\n') {
            complete.push(s);
            lastCompleteEnd += s.length;
        }
    }

    if (complete.length > 0) {
        playedSentences += newText.substring(0, lastCompleteEnd);
    }

    return complete;
}

// ============================================================
// 朗读接口
// ============================================================

async function speakText(text) {
    if (!text || !text.trim()) return;
    if (!settings.enabled) return;

    try {
        console.log('[VolcTTS] 开始合成:', text.substring(0, 50) + '...');
        stopAllAudio();

        const pcmData = await synthesizeText(text);
        if (pcmData && pcmData.byteLength > 0) {
            playPCM(pcmData, settings.sample_rate);
        } else {
            console.warn('[VolcTTS] 未收到音频数据');
        }
    } catch (err) {
        console.error('[VolcTTS] 合成失败:', err);
        showToast('火山引擎 TTS 失败: ' + err.message);
        await disconnect();
    }
                                 }
async function speakSentences(sentences) {
    for (const sentence of sentences) {
        if (!sentence.trim()) continue;
        try {
            const pcmData = await synthesizeText(sentence);
            if (pcmData && pcmData.byteLength > 0) {
                playPCM(pcmData, settings.sample_rate);
            }
        } catch (err) {
            console.error('[VolcTTS] 逐句合成失败:', err);
        }
    }
}

// ============================================================
// SillyTavern 事件监听
// ============================================================

async function setupEventListeners() {
    // 动态导入 ST 模块（兼容第三方扩展加载方式）
    let eventSource, event_types;
    try {
        const scriptModule = await import('../../../script.js');
        eventSource = scriptModule.eventSource;
        event_types = scriptModule.event_types;
    } catch (e) {
        console.warn('[VolcTTS] 无法导入 ST 事件模块，使用轮询模式:', e.message);
        setupPollingFallback();
        return;
    }

    if (!eventSource || !event_types) {
        console.warn('[VolcTTS] 事件系统不可用，使用轮询模式');
        setupPollingFallback();
        return;
    }

    // AI 回复开始
    eventSource.on(event_types.GENERATION_STARTED, async () => {
        if (!settings.enabled || !settings.auto_read) return;
        if (isSessionActive) {
            await disconnect();
        }
        playedSentences = '';
        pendingText = '';

        if (settings.streaming) {
            try {
                await startSession();
            } catch (err) {
                console.error('[VolcTTS] 流式模式启动会话失败:', err);
            }
        }
    });

    // AI 回复流式输出中（每个 token 触发一次）
    eventSource.on(event_types.STREAM_TOKEN_RECEIVED, async (token) => {
        if (!settings.enabled || !settings.auto_read || !settings.streaming) return;
        if (!isSessionActive) return;

        pendingText += (typeof token === 'string' ? token : '');

        const sentences = getNewSentences(pendingText);
        if (sentences.length > 0) {
            await speakSentences(sentences);
        }
    });

    // AI 回复结束
    eventSource.on(event_types.GENERATION_ENDED, async () => {
        if (!settings.enabled || !settings.auto_read) return;

        if (settings.streaming && isSessionActive) {
            // 播放剩余文本
            const remaining = pendingText.substring(playedSentences.length);
            if (remaining.trim()) {
                await speakSentences([remaining]);
            }
            await finishSession();
            await disconnect();
        } else {
            // 非流式模式：播放完整回复
            try {
                const context = window.SillyTavern.getContext();
                const lastMsg = context.chat?.[context.chat.length - 1];
                if (lastMsg && !lastMsg.is_user && !lastMsg.is_system) {
                    await speakText(lastMsg.mes);
                }
            } catch (err) {
                console.error('[VolcTTS] 播放完整回复失败:', err);
            }
        }
    });

    // 用户消息发送
    eventSource.on(event_types.MESSAGE_SENT, async (msgId) => {
        if (!settings.enabled || !settings.auto_read || !settings.read_user) return;
        try {
            const context = window.SillyTavern.getContext();
            const msg = context.chat?.[msgId];
            if (msg && msg.is_user) {
                await speakText(msg.mes);
            }
        } catch (err) {
            console.error('[VolcTTS] 播放用户消息失败:', err);
        }
    });

    // 聊天切换时断开
    eventSource.on(event_types.CHAT_CHANGED, async () => {
        await disconnect();
    });

    console.log('[VolcTTS] 事件监听已注册');
}

/** 轮询降级方案：当事件系统不可用时，定时检查聊天消息变化 */
function setupPollingFallback() {
    let lastMsgText = '';
    let pollingInterval = null;

    // 简化：仅在用户点击 Narrate 按钮时可用
    console.log('[VolcTTS] 事件系统不可用，自动朗读功能将受限。请使用手动 Narrate 按钮。');
    showToast('火山引擎 TTS: 事件系统不可用，自动朗读功能受限。请使用手动按钮。');
}

// ============================================================
// Toast 提示
// ============================================================

function showToast(message) {
    try {
        const context = window.SillyTavern.getContext();
        if (context.toast) {
            context.toast(message, 'warning');
            return;
        }
    } catch (e) { /* 忽略 */ }

    // 降级：使用 alert
    console.warn('[VolcTTS]', message);
}

// ============================================================
// 设置面板 UI
// ============================================================

function renderSettingsUI() {
    const container = document.getElementById('extensions_settings');
    if (!container) {
        setTimeout(renderSettingsUI, 500);
        return;
    }

    // 检查是否已存在
    if (document.getElementById('volcengine_tts_settings')) {
        updateSettingsUI();
        return;
    }

    const html = `
    <div id="volcengine_tts_settings" class="volcengine-tts-panel">
        <h3>火山引擎实时语音 TTS</h3>
        <p class="volc-tts-desc">纯浏览器端 WebSocket 连接，无需 Python 后台。支持流式逐句朗读。</p>

        <div class="volc-tts-section">
            <h4>连接设置</h4>
            <div class="volc-tts-row">
                <label>APP_ID</label>
                <input type="text" id="volc_tts_app_id" placeholder="火山引擎控制台获取的 App ID">
            </div>
            <div class="volc-tts-row">
                <label>ACCESS_TOKEN</label>
                <input type="password" id="volc_tts_access_token" placeholder="火山引擎控制台获取的 Access Token">
            </div>
            <div class="volc-tts-row">
                <label>服务地址</label>
                <input type="text" id="volc_tts_host" placeholder="openspeech.bytedance.com">
            </div>
            <div class="volc-tts-row">
                <label>接口路径</label>
                <input type="text" id="volc_tts_path" placeholder="/api/v3/realtime/dialogue">
            </div>
            <div class="volc-tts-row">
                <label>Resource ID</label>
                <input type="text" id="volc_tts_resource_id" placeholder="volc.speech.dialog">
            </div>
        </div>

        <div class="volc-tts-section">
            <h4>语音设置</h4>
            <div class="volc-tts-row">
                <label>说话人 (Speaker)</label>
                <input type="text" id="volc_tts_speaker" placeholder="saturn_zh_female_aojiaonvyou_tob">
            </div>
            <div class="volc-tts-row">
                <label>采样率 (Hz)</label>
                <input type="number" id="volc_tts_sample_rate" min="8000" max="48000" step="1000">
            </div>
            <div class="volc-tts-row">
                <label>句子分隔符</label>
                <input type="text" id="volc_tts_split_chars" placeholder="。！？.!?">
            </div>
        </div>

        <div class="volc-tts-section">
            <h4>功能开关</h4>
            <div class="volc-tts-row">
                <label class="volc-tts-checkbox">
                    <input type="checkbox" id="volc_tts_enabled">
                    <span>启用 TTS</span>
                </label>
            </div>
            <div class="volc-tts-row">
                <label class="volc-tts-checkbox">
                    <input type="checkbox" id="volc_tts_auto_read">
                    <span>AI 回复时自动朗读</span>
                </label>
            </div>
            <div class="volc-tts-row">
                <label class="volc-tts-checkbox">
                    <input type="checkbox" id="volc_tts_streaming">
                    <span>流式逐句朗读（AI 边生成边读）</span>
                </label>
            </div>
            <div class="volc-tts-row">
                <label class="volc-tts-checkbox">
                    <input type="checkbox" id="volc_tts_read_user">
                    <span>也朗读用户消息</span>
                </label>
            </div>
        </div>

        <div class="volc-tts-section">
            <button id="volc_tts_test_btn" class="volc-tts-btn">测试连接 & 朗读</button>
            <button id="volc_tts_stop_btn" class="volc-tts-btn volc-tts-btn-danger">停止播放</button>
            <span id="volc_tts_status" class="volc-tts-status"></span>
        </div>

        <div class="volc-tts-section">
            <h4>Narrate（朗读当前消息）</h4>
            <button id="volc_tts_narrate_btn" class="volc-tts-btn">朗读最后一条 AI 消息</button>
        </div>
    </div>`;

    container.insertAdjacentHTML('beforeend', html);

    // 绑定事件
    bindSettingsEvents();
    updateSettingsUI();
}

function bindSettingsEvents() {
    const fields = [
        { id: 'volc_tts_app_id', key: 'app_id' },
        { id: 'volc_tts_access_token', key: 'access_token' },
        { id: 'volc_tts_host', key: 'host' },
        { id: 'volc_tts_path', key: 'path' },
        { id: 'volc_tts_resource_id', key: 'resource_id' },
        { id: 'volc_tts_speaker', key: 'speaker' },
        { id: 'volc_tts_split_chars', key: 'sentence_split_chars' },
    ];

    for (const f of fields) {
        const el = document.getElementById(f.id);
        if (!el) continue;
        el.addEventListener('input', () => {
            settings[f.key] = el.value;
            saveSettings();
        });
    }

    // 数字字段
    const sampleRateEl = document.getElementById('volc_tts_sample_rate');
    if (sampleRateEl) {
        sampleRateEl.addEventListener('input', () => {
            settings.sample_rate = parseInt(sampleRateEl.value) || 24000;
            saveSettings();
        });
    }

    // 复选框
    const checkboxes = [
        { id: 'volc_tts_enabled', key: 'enabled' },
        { id: 'volc_tts_auto_read', key: 'auto_read' },
        { id: 'volc_tts_streaming', key: 'streaming' },
        { id: 'volc_tts_read_user', key: 'read_user' },
    ];

    for (const cb of checkboxes) {
        const el = document.getElementById(cb.id);
        if (!el) continue;
        el.addEventListener('change', () => {
            settings[cb.key] = el.checked;
            saveSettings();
        });
    }

    // 测试按钮
    const testBtn = document.getElementById('volc_tts_test_btn');
    if (testBtn) {
        testBtn.addEventListener('click', async () => {
            setStatus('正在连接...');
            try {
                await disconnect();
                await startSession();
                setStatus('已连接，正在合成测试音频...');
                const pcmData = await synthesizeText('你好，这是火山引擎语音合成测试。');
                if (pcmData && pcmData.byteLength > 0) {
                    playPCM(pcmData, settings.sample_rate);
                    setStatus('测试成功！音频正在播放。');
                } else {
                    setStatus('测试失败：未收到音频数据');
                }
            } catch (err) {
                setStatus('测试失败: ' + err.message);
                console.error('[VolcTTS] 测试失败:', err);
            }
        });
    }

    // 停止按钮
    const stopBtn = document.getElementById('volc_tts_stop_btn');
    if (stopBtn) {
        stopBtn.addEventListener('click', async () => {
            stopAllAudio();
            await disconnect();
            setStatus('已停止');
        });
    }

    // Narrate 按钮
    const narrateBtn = document.getElementById('volc_tts_narrate_btn');
    if (narrateBtn) {
        narrateBtn.addEventListener('click', async () => {
            try {
                const context = window.SillyTavern.getContext();
                const chat = context.chat || [];
                let lastMsg = null;
                for (let i = chat.length - 1; i >= 0; i--) {
                    if (!chat[i].is_user && !chat[i].is_system) {
                        lastMsg = chat[i];
                        break;
                    }
                }
                if (lastMsg) {
                    setStatus('正在朗读...');
                    stopAllAudio();
                    await startSession();
                    const pcmData = await synthesizeText(lastMsg.mes);
                    if (pcmData && pcmData.byteLength > 0) {
                        playPCM(pcmData, settings.sample_rate);
                        setStatus('正在播放');
                    }
                } else {
                    setStatus('没有找到 AI 消息');
                }
            } catch (err) {
                setStatus('朗读失败: ' + err.message);
                console.error('[VolcTTS] Narrate 失败:', err);
            }
        });
    }
}

function updateSettingsUI() {
    const fieldMap = {
        'volc_tts_app_id': 'app_id',
        'volc_tts_access_token': 'access_token',
        'volc_tts_host': 'host',
        'volc_tts_path': 'path',
        'volc_tts_resource_id': 'resource_id',
        'volc_tts_speaker': 'speaker',
        'volc_tts_split_chars': 'sentence_split_chars',
    };

    for (const [id, key] of Object.entries(fieldMap)) {
        const el = document.getElementById(id);
        if (el) el.value = settings[key] || '';
    }

    const sampleRateEl = document.getElementById('volc_tts_sample_rate');
    if (sampleRateEl) sampleRateEl.value = settings.sample_rate;

    const checkboxMap = {
        'volc_tts_enabled': 'enabled',
        'volc_tts_auto_read': 'auto_read',
        'volc_tts_streaming': 'streaming',
        'volc_tts_read_user': 'read_user',
    };

    for (const [id, key] of Object.entries(checkboxMap)) {
        const el = document.getElementById(id);
        if (el) el.checked = settings[key];
    }
}

function setStatus(msg) {
    const el = document.getElementById('volc_tts_status');
    if (el) el.textContent = msg;
}

// ============================================================
// 插件入口
// ============================================================

(async function init() {
    // 等待 SillyTavern 就绪
    if (!window.SillyTavern) {
        console.log('[VolcTTS] 等待 SillyTavern 加载...');
        setTimeout(init, 1000);
        return;
    }

    console.log('[VolcTTS] 火山引擎 TTS 插件初始化中...');

    // 加载设置
    loadSettings();

    // 渲染设置面板
    renderSettingsUI();

    // 注册事件监听
    await setupEventListeners();

    console.log('[VolcTTS] 插件初始化完成');
    console.log('[VolcTTS] 设置:', {
        enabled: settings.enabled,
        auto_read: settings.auto_read,
        streaming: settings.streaming,
        speaker: settings.speaker,
    });
})();
