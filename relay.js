
const http      = require('http');
const WebSocket  = require('ws');
const crypto     = require('crypto');

const SECRET = process.env.RELAY_SECRET || 'vera-relay-secret';
const PORT   = Number(process.env.PORT)  || 80;

const server = http.createServer();
const wss    = new WebSocket.Server({ server });

let agentWs        = null;
const pendingHttp  = {};
const pendingHttpTimeout = {};
const pendingWs    = {};
let agentPingTimer = null;

wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://x');

    if (url.pathname === '/agent' && url.searchParams.get('token') === SECRET) {
        if (agentWs) { try { agentWs.terminate(); } catch (_) {} }
        agentWs = ws;
        console.log('[relay] PC agent conectado');
        agentPingTimer = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.ping();
        }, 25000);
        ws.on('message', handleAgentMessage);
        ws.on('pong', () => {});
        ws.on('close', () => {
            agentWs = null;
            clearInterval(agentPingTimer);
            console.log('[relay] PC agent desconectado');
            Object.entries(pendingHttp).forEach(([id, res]) => {
                clearTimeout(pendingHttpTimeout[id]);
                delete pendingHttpTimeout[id];
                try { res.end(); } catch (_) {}
                delete pendingHttp[id];
            });
            Object.values(pendingWs).forEach(mws => {
                try { mws.close(1001, 'Agent disconnected'); } catch (_) {}
            });
        });
        return;
    }

    if (!agentWs || agentWs.readyState !== WebSocket.OPEN) {
        ws.close(1001, 'PC not connected');
        return;
    }
    const id = crypto.randomBytes(4).toString('hex');
    pendingWs[id] = ws;
    agentWs.send(JSON.stringify({ type: 'ws_open', id, url: req.url, headers: req.headers }));
    ws.on('message', (data, isBinary) => {
        if (agentWs?.readyState !== WebSocket.OPEN) return;
        agentWs.send(JSON.stringify({ type: 'ws_data', id, data: Buffer.from(data).toString('base64'), binary: isBinary }));
    });
    ws.on('close', (code, reason) => {
        if (agentWs?.readyState === WebSocket.OPEN)
            agentWs.send(JSON.stringify({ type: 'ws_close', id, code, reason: reason.toString() }));
        delete pendingWs[id];
    });
});

server.on('request', (req, res) => {
    if (req.url === '/health' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(agentWs ? 'connected' : 'no-agent');
        return;
    }
    if (!agentWs || agentWs.readyState !== WebSocket.OPEN) {
        res.writeHead(503, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<h2>PC agent no conectado — ¿está encendido el PC?</h2>');
        return;
    }
    const id = crypto.randomBytes(4).toString('hex');
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
        pendingHttp[id] = res;
        agentWs.send(JSON.stringify({ type: 'http_req', id, method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString('base64') }));
        const timeoutHandle = setTimeout(() => {
            if (pendingHttp[id]) {
                try { res.writeHead(504, { 'Content-Type': 'text/plain' }); res.end('Gateway Timeout'); } catch (_) {}
                delete pendingHttp[id];
            }
        }, 60000);
        pendingHttpTimeout[id] = timeoutHandle;
    });
});

function handleAgentMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'http_resp') {
        const res = pendingHttp[msg.id]; if (!res) return;
        clearTimeout(pendingHttpTimeout[msg.id]); delete pendingHttpTimeout[msg.id];
        const headers = { ...msg.headers };
        delete headers['transfer-encoding']; delete headers['connection'];
        try { res.writeHead(msg.status, headers); res.end(Buffer.from(msg.body || '', 'base64')); } catch (_) {}
        delete pendingHttp[msg.id];
    } else if (msg.type === 'http_resp_start') {
        const res = pendingHttp[msg.id]; if (!res) return;
        clearTimeout(pendingHttpTimeout[msg.id]); delete pendingHttpTimeout[msg.id];
        const headers = { ...msg.headers };
        delete headers['transfer-encoding']; delete headers['connection'];
        try { res.writeHead(msg.status, headers); } catch (_) {}
    } else if (msg.type === 'http_resp_chunk') {
        const res = pendingHttp[msg.id]; if (!res) return;
        try { res.write(Buffer.from(msg.data, 'base64')); } catch (_) {}
    } else if (msg.type === 'http_resp_end') {
        const res = pendingHttp[msg.id]; if (!res) return;
        try { res.end(); } catch (_) {}
        delete pendingHttp[msg.id];
    } else if (msg.type === 'ws_data') {
        const mws = pendingWs[msg.id];
        if (mws?.readyState === WebSocket.OPEN)
            mws.send(Buffer.from(msg.data, 'base64'), { binary: msg.binary });
    } else if (msg.type === 'ws_close') {
        const mws = pendingWs[msg.id];
        if (mws) { try { mws.close(msg.code || 1000, msg.reason || ''); } catch (_) {} delete pendingWs[msg.id]; }
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`[relay] escuchando en :${PORT}  (secret: ${SECRET.slice(0, 4)}****)`);
});
