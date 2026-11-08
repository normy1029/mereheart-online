'use strict';
/* Mereheart Online — multiplayer server (no dependencies, Node 18+)
   - serves the game page (index.html) at "/"
   - WebSocket endpoint at "/ws" (hand-rolled RFC 6455, no npm packages needed)
   Run:  node server.js     (PORT env var optional, default 3000) */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 3000;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 100;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '';   // e.g. https://my-game.example.com  (empty = allow any)
const MAX_MSG = 4096, TICK_MS = 100;
const CLASSES = new Set(['witch', 'dq', 'assassin', 'goddess']);
const players = new Map();
let nextId = 1;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/healthz') { res.writeHead(200); return res.end('ok ' + players.size); }
  if (url === '/' || url === '/index.html') {
    const f = ['index.html', 'Mereheart_Online.html'].map(n => path.join(__dirname, n)).find(fs.existsSync);
    if (!f) { res.writeHead(404); return res.end('game html not found next to server.js'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    return fs.createReadStream(f).pipe(res);
  }
  res.writeHead(404); res.end('not found');
});

/* ---------- WebSocket plumbing ---------- */
function frame(op, buf) {
  const n = buf.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, buf]);
}
function send(p, obj) {
  if (p.sock.destroyed || !p.sock.writable) return;
  p.sock.write(frame(1, Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj))));
}
function broadcast(obj, except) {
  const s = JSON.stringify(obj);
  for (const p of players.values()) if (p.joined && p !== except) send(p, s);
}
function kill(p) { try { p.sock.destroy(); } catch (e) {} drop(p); }
function drop(p) {
  if (p.gone) return; p.gone = true;
  if (players.delete(p.id) && p.joined) broadcast({ t: 'leave', id: p.id });
}
function onData(p, chunk) {
  p.buf = Buffer.concat([p.buf, chunk]);
  if (p.buf.length > MAX_MSG * 4) return kill(p);
  for (;;) {
    const b = p.buf;
    if (b.length < 2) return;
    const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80;
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_MSG)) return kill(p); len = Number(big); off = 10; }
    if (len > MAX_MSG || !masked) return kill(p);
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    p.buf = b.subarray(off + 4 + len);
    if (op === 8) return kill(p);
    if (op === 9) { p.sock.write(frame(10, data)); continue; }
    if (op === 10) { p.alive = true; continue; }
    if (op === 1 || op === 0) {
      p.frag = op === 1 ? [data] : (p.frag || []).concat([data]);
      if (p.frag.reduce((a, x) => a + x.length, 0) > MAX_MSG) return kill(p);
      if (fin) { const msg = Buffer.concat(p.frag).toString('utf8'); p.frag = null; if (!rate(p)) return kill(p); try { handle(p, msg); } catch (e) {} }
    }
  }
}
function rate(p) { const s = Math.floor(Date.now() / 1000); if (p.rs !== s) { p.rs = s; p.rc = 0; } return ++p.rc <= 60; }

server.on('upgrade', (req, sock) => {
  const ok = req.url.split('?')[0] === '/ws' && String(req.headers.upgrade).toLowerCase() === 'websocket' && req.headers['sec-websocket-key'];
  if (!ok || (ALLOWED_ORIGIN && req.headers.origin !== ALLOWED_ORIGIN)) { sock.destroy(); return; }
  if (players.size >= MAX_PLAYERS) { sock.end('HTTP/1.1 503 Service Unavailable\r\n\r\n'); return; }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const p = { id: nextId++, sock, buf: Buffer.alloc(0), joined: false, alive: true, lastChat: 0, lastSk: 0, d: null };
  players.set(p.id, p);
  sock.on('data', d => onData(p, d));
  sock.on('close', () => drop(p));
  sock.on('error', () => { try { sock.destroy(); } catch (e) {} });
});

/* ---------- validation ---------- */
const num = (v, lo, hi, def) => { v = +v; return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : def; };
const str = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').slice(0, n);
const col = v => (typeof v === 'string' && /^#[0-9a-f]{3,6}$/i.test(v)) ? v : null;
function cleanLook(l, old) {
  l = l && typeof l === 'object' ? l : {};
  return { cls: CLASSES.has(l.cls) ? l.cls : (old ? old.cls : 'witch'), hair: col(l.hair) || (old ? old.hair : '#d8dde8'),
    weapon: l.weapon ? str(l.weapon, 16) : null, shield: !!l.shield, rib: col(l.rib) };
}
function cleanState(m, d) {
  d.map = /^[a-z0-9_]{1,16}$/i.test(m.map) ? m.map : (d.map || 'village');
  d.x = Math.round(num(m.x, 0, 20000, d.x || 0)); d.y = Math.round(num(m.y, 0, 20000, d.y || 0));
  d.dir = 'dulr'.includes(m.dir) && m.dir ? m.dir : (d.dir || 'd');
  d.mv = !!m.mv; d.sit = !!m.sit; d.lv = Math.round(num(m.lv, 1, 999, d.lv || 1));
}

function handle(p, msg) {
  const m = JSON.parse(msg);
  if (!m || typeof m !== 'object') return;
  if (m.t === 'join') {
    if (p.joined) return;
    const d = { id: p.id, name: str(m.name, 14).trim() || 'Player' + p.id, look: cleanLook(m.lk) };
    cleanState(m, d); p.d = d; p.joined = true;
    send(p, { t: 'hello', id: p.id, players: [...players.values()].filter(q => q.joined && q !== p).map(q => q.d) });
    broadcast({ t: 'join', p: d }, p);
    console.log(`+ ${d.name} (#${p.id})  online=${[...players.values()].filter(q => q.joined).length}`);
    return;
  }
  if (!p.joined) return;
  if (m.t === 'st') { cleanState(m, p.d); if (m.lk) { p.d.look = cleanLook(m.lk, p.d.look); p.lkDirty = true; } p.dirty = true; }
  else if (m.t === 'chat') {
    const now = Date.now(); if (now - p.lastChat < 700) return; p.lastChat = now;
    const text = str(m.text, 90).trim(); if (!text) return;
    broadcast({ t: 'chat', id: p.id, name: p.d.name, text });
  } else if (m.t === 'sk') {
    const now = Date.now(); if (now - p.lastSk < 200) return; p.lastSk = now;
    if (!/^[a-z]{3,12}$/.test(String(m.id))) return;
    broadcast({ t: 'sk', id: p.id, sk: m.id, tx: Math.round(num(m.tx, 0, 20000, 0)), ty: Math.round(num(m.ty, 0, 20000, 0)), has: !!m.has }, p);
  }
}

/* batch position updates: one message per tick for everyone */
setInterval(() => {
  const a = [];
  for (const p of players.values()) {
    if (!p.joined || !p.dirty) continue;
    p.dirty = false; const d = p.d, u = { id: d.id, map: d.map, x: d.x, y: d.y, dir: d.dir, mv: d.mv, sit: d.sit, lv: d.lv };
    if (p.lkDirty) { u.lk = d.look; p.lkDirty = false; }
    a.push(u);
  }
  if (a.length) broadcast({ t: 'sts', a });
}, TICK_MS);

/* drop dead connections */
setInterval(() => {
  for (const p of players.values()) {
    if (!p.alive) { kill(p); continue; }
    p.alive = false; try { p.sock.write(frame(9, Buffer.alloc(0))); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => console.log(`Mereheart Online server on http://localhost:${PORT}  (ws path: /ws)`));
