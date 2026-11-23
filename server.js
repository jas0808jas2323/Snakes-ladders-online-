// Ups & Downs relay server. Keeps game rooms (numbered 1-100) and passes
// each player's shared state to everyone in the same room.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PER_ROOM = 20;
const KEY = /^[A-Za-z_][A-Za-z0-9_]{0,31}$/;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('Ups & Downs server is running\n');
});
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // number -> { clients:Set, state:{id:{...}} }
let counter = 1;

const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const broadcast = (room) => {
  const msg = JSON.stringify({ t: 's', s: room.state });
  for (const c of room.clients) if (c.readyState === 1) c.send(msg);
};

wss.on('connection', (ws) => {
  ws.id = 'c' + (counter++).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.room = null;
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'hi', id: ws.id });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if ((m.t === 'create' || m.t === 'join') && ws.room === null) {
      const n = m.n;
      if (!Number.isInteger(n) || n < 1 || n > 100) return send(ws, { t: 'err', m: 'bad' });
      let room = rooms.get(n);
      if (m.t === 'create') {
        if (room) return send(ws, { t: 'err', m: 'exists' });
        room = { clients: new Set(), state: Object.create(null) };
        rooms.set(n, room);
      } else if (!room) {
        return send(ws, { t: 'err', m: 'none' });
      }
      if (room.clients.size >= MAX_PER_ROOM) return send(ws, { t: 'err', m: 'full' });
      ws.room = n;
      room.clients.add(ws);
      room.state[ws.id] = Object.create(null);
      send(ws, { t: 'ok' });
      broadcast(room);
    } else if (m.t === 'p' && ws.room !== null) {
      const room = rooms.get(ws.room);
      if (!room || !m.p || typeof m.p !== 'object') return;
      const mine = room.state[ws.id];
      for (const k of Object.keys(m.p)) {
        if (!KEY.test(k) || k === '__proto__') continue;
        if (m.p[k] === null) delete mine[k]; else mine[k] = m.p[k];
      }
      if (JSON.stringify(mine).length > 6000) return ws.close(1009);
      broadcast(room);
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    room.clients.delete(ws);
    delete room.state[ws.id];
    if (room.clients.size === 0) rooms.delete(ws.room); else broadcast(room);
  });
});

// Drop dead connections and keep hosts from idling out.
setInterval(() => {
  for (const c of wss.clients) {
    if (!c.alive) { c.terminate(); continue; }
    c.alive = false;
    c.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Ups & Downs server listening on port ' + PORT));
