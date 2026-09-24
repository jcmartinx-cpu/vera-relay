'use strict';
const http     = require('http');
const WebSocket = require('ws');
const crypto   = require('crypto');
const { Server: SocketIO } = require('socket.io');

const SECRET = process.env.RELAY_SECRET || 'vera-relay-secret';
const PORT   = Number(process.env.PORT)  || 80;

const server = http.createServer();

// socket.io — agente PC usa HTTP polling (pasa ZScaler sin WebSocket)
const io = new SocketIO(server, {
    path: '/socket.io',
    cors: { origin: '*' },
    transports: ['polling', 'websocket']
});

// WebSocket nativo — móvil (no pasa por ZScaler)
const wss = new WebSocket.Server({ noServer: true });

let agentSocket          = null;
const pendingHttp        = {};
const pendingHttpTimeout = {};
const pendingWs          = {};

// ── Agente PC via socket.io ───────────────────────────────────────────────────
io.on('connection', (socket) => {
    const token = socket.handshake.auth?.token || socket.handshake.query?.token;
    if (token !== SECRET) { socket.disconnect(true); return; }

    if (agentSocket) { try { agentSocket.disconnect(true); } catch (_) {} }
    agentSocket = socket;
    console.log('[relay] ✅ PC agent conectado (socket.io)');

    socket.on('http_resp', (msg) => {
        const res = pendingHttp[msg.id]; if (!res) return;
        clearTimeout(pendingHttpTimeout[msg.id]); delete pendingHttpTimeout[msg.id];
        const hdrs = { ...msg.headers };
        delete hdrs['transfer-encoding']; delete hdrs['connection'];
        try { res.writeHead(msg.status, hdrs); res.end(Buffer.from(msg.body || '', 'base64')); } catch (_) {}
        delete pendingHttp[msg.id];
    });

    socket.on('http_resp_start', (msg) => {
        const res = pendingHttp[msg.id]; if (!res) return;
        clearTimeout(pendingHttpTimeout[msg.id]); delete pendingHttpTimeout[msg.id];
        const hdrs = { ...msg.headers };
        delete hdrs['transfer-encoding']; delete hdrs['connection'];
        try { res.writeHead(msg.status, hdrs); } catch (_) {}
    });

    socket.on('http_resp_chunk', (msg) => {
        const res = pendingHttp[msg.id]; if (!res) return;
        try { res.write(Buffer.from(msg.data, 'base64')); } catch (_) {}
    });

    socket.on('http_resp_end', (msg) => {
        const res = pendingHttp[msg.id]; if (!res) return;
        try { res.end(); } catch (_) {}
        delete pendingHttp[msg.id];
    });

    socket.on('ws_data', (msg) => {
        const mws = pendingWs[msg.id];
        if (mws?.readyState === WebSocket.OPEN)
            mws.send(Buffer.from(msg.data, 'base64'), { binary: msg.binary });
    });

    socket.on('ws_close', (msg) => {
        const mws = pendingWs[msg.id];
        if (mws) {
            try { mws.close(msg.code || 1000, msg.reason || ''); } catch (_) {}
            delete pendingWs[msg.id];
        }
    });

    socket.on('disconnect', (reason) => {
        if (agentSocket === socket) agentSocket = null;
        console.log(`[relay] PC agent desconectado: ${reason}`);
        Object.entries(pendingHttp).forEach(([id, res]) => {
            clearTimeout(pendingHttpTimeout[id]); delete pendingHttpTimeout[id];
            try { res.writeHead(503); res.end('Agent disconnected'); } catch (_) {}
            delete pendingHttp[id];
        });
        Object.values(pendingWs).forEach(mws => {
            try { mws.close(1001, 'Agent disconnected'); } catch (_) {}
        });
    });
});

// ── WebSocket del móvil (upgrade nativo, no socket.io) ───────────────────────
server.on('upgrade', (req, socket, head) => {
    if (req.url.startsWith('/socket.io')) return; // socket.io lo gestiona solo
    if (!agentSocket) { socket.destroy(); return; }

    wss.handleUpgrade(req, socket, head, (ws) => {
        const id = crypto.randomBytes(4).toString('hex');
        pendingWs[id] = ws;
        agentSocket.emit('ws_open', { id, url: req.url, headers: req.headers });

        ws.on('message', (data, isBinary) => {
            if (!agentSocket) return;
            agentSocket.emit('ws_data', { id, data: Buffer.from(data).toString('base64'), binary: isBinary });
        });
        ws.on('close', (code, reason) => {
            if (agentSocket) agentSocket.emit('ws_close', { id, code, reason: reason.toString() });
            delete pendingWs[id];
        });
        ws.on('error', () => {});
    });
});

// ── Peticiones HTTP del móvil ─────────────────────────────────────────────────
server.on('request', (req, res) => {
    if (req.url.startsWith('/socket.io')) return; // socket.io lo gestiona

    if (req.url === '/health' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(agentSocket ? 'connected' : 'no-agent');
        return;
    }
    if (!agentSocket) {
        res.writeHead(503, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<h2>PC agent no conectado — ¿está encendido el PC?</h2>');
        return;
    }
    const id     = crypto.randomBytes(4).toString('hex');
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
        pendingHttp[id] = res;
        agentSocket.emit('http_req', {
            id, method: req.method, url: req.url,
            headers: req.headers,
            body: Buffer.concat(chunks).toString('base64')
        });
        pendingHttpTimeout[id] = setTimeout(() => {
            if (pendingHttp[id]) {
                try { res.writeHead(504); res.end('Gateway Timeout'); } catch (_) {}
                delete pendingHttp[id];
            }
        }, 60000);
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`[relay] escuchando en :${PORT}  (secret: ${SECRET.slice(0, 4)}****)`);
});
