/**
 * 火山引擎 WebSocket 代理
 * 
 * 运行方式：
 *   cd /data/user/0/com.jm.sillydroid/files/android-tavern/bootstrap/server
 *   node public/scripts/extensions/third-party/volcengine-tts/volcengine-proxy.js
 * 
 * 作用：浏览器 WebSocket 不支持自定义 Header，此代理用 Node.js ws 库
 * 连接火山引擎（带 Header 鉴权），并转发浏览器与火山引擎之间的所有二进制帧。
 */

import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'http';

const PROXY_PORT = 8700;
const PROXY_HOST = '127.0.0.1';

let server = null;
let wss = null;

function start() {
    server = createServer();
    wss = new WebSocketServer({ server });

    wss.on('connection', (browserWs) => {
        console.log('[VolcProxy] 浏览器已连接');
        let volcWs = null;
        let authInfo = null;

        browserWs.on('message', async (data, isBinary) => {
            // 第一条消息：JSON 鉴权信息
            if (!authInfo && !isBinary) {
                try {
                    authInfo = JSON.parse(data.toString());
                    console.log('[VolcProxy] 收到鉴权信息, appId:', authInfo.appId);
                } catch (e) {
                    browserWs.send(JSON.stringify({ type: 'error', message: 'Invalid auth JSON' }));
                    browserWs.close();
                    return;
                }

                // 连接火山引擎
                try {
                    volcWs = await connectVolcengine(authInfo);
                    browserWs.send(JSON.stringify({ type: 'ready' }));
                    console.log('[VolcProxy] 火山引擎连接成功，开始转发');
                } catch (err) {
                    console.error('[VolcProxy] 连接火山引擎失败:', err.message);
                    browserWs.send(JSON.stringify({ type: 'error', message: err.message }));
                    browserWs.close();
                    return;
                }

                // 火山引擎 → 浏览器
                volcWs.on('message', (volcData) => {
                    if (browserWs.readyState === WebSocket.OPEN) {
                        browserWs.send(volcData);
                    }
                });

                volcWs.on('close', (code, reason) => {
                    console.log('[VolcProxy] 火山引擎连接关闭, code:', code);
                    if (browserWs.readyState === WebSocket.OPEN) {
                        browserWs.close(1000, 'Volcengine closed');
                    }
                });

                volcWs.on('error', (err) => {
                    console.error('[VolcProxy] 火山引擎错误:', err.message);
                    if (browserWs.readyState === WebSocket.OPEN) {
                        browserWs.send(JSON.stringify({ type: 'error', message: err.message }));
                    }
                });

                return;
            }

            // 后续消息：二进制帧，直接转发到火山引擎
            if (volcWs && volcWs.readyState === WebSocket.OPEN) {
                volcWs.send(data, { binary: isBinary });
            }
        });

        browserWs.on('close', () => {
            console.log('[VolcProxy] 浏览器断开');
            if (volcWs) {
                try { volcWs.close(); } catch (e) { /* ignore */ }
            }
        });

        browserWs.on('error', (err) => {
            console.error('[VolcProxy] 浏览器连接错误:', err.message);
        });
    });

    server.listen(PROXY_PORT, PROXY_HOST, () => {
        console.log('[VolcProxy] 代理已启动: ws://' + PROXY_HOST + ':' + PROXY_PORT);
        console.log('[VolcProxy] 请勿关闭此终端，保持代理运行');
    });

    server.on('error', (err) => {
        console.error('[VolcProxy] 服务器错误:', err.message);
        if (err.code === 'EADDRINUSE') {
            console.error('[VolcProxy] 端口 ' + PROXY_PORT + ' 已被占用，可能已有代理在运行');
        }
    });
}

function connectVolcengine(auth) {
    return new Promise((resolve, reject) => {
        const url = 'wss://' + (auth.host || 'openspeech.bytedance.com') + (auth.path || '/api/v3/realtime/dialogue');

        const ws = new WebSocket(url, {
            headers: {
                'X-Api-App-ID': String(auth.appId || ''),
                'X-Api-Access-Key': String(auth.accessToken || ''),
                'X-Api-Resource-Id': String(auth.resourceId || 'volc.speech.dialog'),
                'X-Api-App-Key': 'PlgvMymc7f3tQnJ6',
                'X-Api-Connect-Id': String(auth.connectId || randomUUID()),
            },
            rejectUnauthorized: false, // 允许自签名证书
        });

        const timeout = setTimeout(() => {
            ws.close();
            reject(new Error('连接火山引擎超时（15秒）'));
        }, 15000);

        ws.on('open', () => {
            clearTimeout(timeout);
            resolve(ws);
        });

        ws.on('error', (err) => {
            clearTimeout(timeout);
            reject(new Error('无法连接火山引擎: ' + err.message));
        });

        ws.on('unexpected-response', (req, res) => {
            clearTimeout(timeout);
            let body = '';
            res.on('data', (chunk) => { body += chunk; });
            res.on('end', () => {
                reject(new Error('火山引擎拒绝连接 (HTTP ' + res.statusCode + '): ' + body.substring(0, 200)));
            });
        });
    });
}

function randomUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
}

// 启动
start();

// 优雅退出
process.on('SIGINT', () => {
    console.log('\n[VolcProxy] 正在关闭...');
    if (wss) wss.close();
    if (server) server.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    if (wss) wss.close();
    if (server) server.close();
    process.exit(0);
});