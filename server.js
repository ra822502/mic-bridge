'use strict';
/**
 * Mic Bridge — سيرفر الربط بين روبلوكس وموقع المايك
 *  - روبلوكس (سيرفر اللعبة) يرسل المواقع والحالة كل ~0.35 ثانية عبر POST /api/sync
 *  - الموقع يتصل عبر WebSocket، ويستلم قائمة القريبين وحجم صوت كل واحد
 *  - الصوت نفسه يمر بين المتصفحات مباشرة (WebRTC) ولا يمر عبر هذا السيرفر
 */
const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const SECRET = process.env.SHARED_SECRET || '';
const DEFAULT_MAX = 50;                    // نصف قطر السماع (studs) إذا ما أرسلته اللعبة
const STALE_MS = 20000;                    // لو اللعبة سكتت أكثر من هذا ننظف
const HEAR_WHEN_MUTED = process.env.HEAR_WHEN_MUTED === '1'; // 1 = تسمع القريبين حتى لو مايكك مقفل

if (!SECRET) console.warn('[!] SHARED_SECRET غير مضبوط — كل طلبات اللعبة ستُرفض');

const app = express();
app.use(express.json({ limit: '512kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.type('text').send('ok'));

const safeEq = (a, b) => {
  const A = Buffer.from(String(a)), B = Buffer.from(String(b));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
};
const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const clamp01 = (n) => Math.max(0, Math.min(1, Number(n) || 0));

/** key = jobId:userId */
const players = new Map();
const tokens = new Map();   // token -> key
const jobs = new Map();     // jobId -> { max, last }

function removePlayer(p, msg) {
  if (p.token) tokens.delete(p.token);
  if (p.ws) {
    send(p.ws, { type: 'error', fatal: true, msg: msg || 'خرجت من اللعبة.' });
    try { p.ws.close(); } catch {}
  }
  players.delete(p.key);
}

function pushState(jobId, max, callMap, liveMap) {
  const list = [...players.values()].filter((p) => p.jobId === jobId && p.ws && p.ready);
  for (const p of list) {
    const peers = [];
    for (const q of list) {
      if (q === p) continue;
      const d = Math.hypot(p.pos[0] - q.pos[0], p.pos[1] - q.pos[1], p.pos[2] - q.pos[2]);
      // مكالمة جوال: يسمعون بعض مهما كانت المسافة
      const inCall = !!(callMap && callMap.get(p.userId) && callMap.get(p.userId).has(q.userId));
      // بث مباشر: المشاهد (p) يسمع صاحب البث (q) مهما كانت المسافة
      const liveHear = !!(liveMap && liveMap.get(p.userId) && liveMap.get(p.userId).has(q.userId));
      const liveLink = liveHear || !!(liveMap && liveMap.get(q.userId) && liveMap.get(q.userId).has(p.userId));
      if (!inCall && !liveLink && d > max * 1.3) continue; // نبقي الاتصال قليلاً بعد الخروج من النطاق (hysteresis)
      let vol = 0;
      const canHear = q.active && (p.active || p.rec || HEAR_WHEN_MUTED || liveHear);
      if (canHear) {
        if (inCall || liveHear) vol = 1;
        else if (d < max) vol = Math.round(Math.pow(1 - d / max, 1.5) * 100) / 100;
      }
      peers.push({ id: q.userId, name: q.displayName || q.name, vol, call: inCall });
    }
    send(p.ws, { type: 'state', active: p.active, rec: !!p.rec, cmd: p.cmd || '', peers });
  }
}

/* ---------- Roblox -> backend ---------- */
app.post('/api/sync', (req, res) => {
  const b = req.body || {};
  if (!SECRET || !safeEq(b.secret || '', SECRET)) return res.status(401).json({ ok: false });

  const jobId = String(b.jobId || 'studio');
  const max = Math.min(Math.max(Number(b.maxDistance) || DEFAULT_MAX, 5), 500);
  jobs.set(jobId, { max, last: Date.now() });

  // مشاهدو البث: [[صاحب البث, المشاهد], ...]
  const liveMap = new Map(); // listenerId -> Set(speakerId)
  for (const pair of Array.isArray(b.lives) ? b.lives : []) {
    if (!Array.isArray(pair) || pair.length !== 2) continue;
    const sp = String(pair[0]), li = String(pair[1]);
    if (!liveMap.has(li)) liveMap.set(li, new Set());
    liveMap.get(li).add(sp);
  }

  // أزواج المكالمات من الجوال: [[userIdA, userIdB], ...]
  const callMap = new Map(); // userId -> Set(userId)
  for (const pair of Array.isArray(b.calls) ? b.calls : []) {
    if (!Array.isArray(pair) || pair.length !== 2) continue;
    const x = String(pair[0]), y = String(pair[1]);
    if (!callMap.has(x)) callMap.set(x, new Set());
    if (!callMap.has(y)) callMap.set(y, new Set());
    callMap.get(x).add(y);
    callMap.get(y).add(x);
  }

  const seen = new Set();
  for (const d of Array.isArray(b.players) ? b.players : []) {
    const userId = String(d.userId);
    const key = `${jobId}:${userId}`;
    seen.add(key);
    let p = players.get(key);
    if (!p) {
      p = { key, jobId, userId, ws: null, bound: null, ready: false, peak: 0, active: false, token: null, pos: [0, 0, 0] };
      players.set(key, p);
    }
    p.name = String(d.name || '');
    p.displayName = String(d.displayName || d.name || '');
    p.pos = [Number(d.x) || 0, Number(d.y) || 0, Number(d.z) || 0];
    p.active = !!d.active;
    p.rec = !!d.rec; // الجوال يطلب تسجيل الصوت
    p.cmd = typeof d.cmd === 'string' ? d.cmd.slice(0, 80) : '';

    if (typeof d.token === 'string' && d.token && d.token !== p.token) {
      if (p.token) tokens.delete(p.token);
      p.token = d.token;
      tokens.set(d.token, key);
      p.bound = null; p.ready = false; p.peak = 0;
      if (p.ws) {
        send(p.ws, { type: 'error', fatal: true, msg: 'تم إنشاء رابط جديد من اللعبة. استخدم الرابط الجديد.' });
        try { p.ws.close(); } catch {}
        p.ws = null;
      }
    }
  }
  // اللي ما رجع في القائمة = خرج من اللعبة
  for (const p of [...players.values()]) if (p.jobId === jobId && !seen.has(p.key)) removePlayer(p);

  const out = {};
  for (const p of players.values()) {
    if (p.jobId !== jobId) continue;
    out[p.userId] = { linked: !!p.ws, ready: !!(p.ws && p.ready), level: p.active ? Math.round(p.peak * 100) / 100 : 0, recs: p.recs || '[]' };
    p.peak = 0;
  }
  pushState(jobId, max, callMap, liveMap);
  res.json({ ok: true, players: out });
});

/* ---------- Website helpers ---------- */
app.get('/api/link/:token', (req, res) => {
  const key = tokens.get(String(req.params.token));
  const p = key && players.get(key);
  if (!p) return res.status(404).json({ ok: false });
  res.json({ ok: true, name: p.name, displayName: p.displayName, userId: p.userId });
});

app.get('/api/ice', (_req, res) => {
  const iceServers = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(','),
      username: process.env.TURN_USER || '',
      credential: process.env.TURN_PASS || '',
    });
  }
  res.json({ iceServers });
});

/* ---------- WebSocket (website <-> backend) ---------- */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let p = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'bind') {
      const key = tokens.get(String(m.token || ''));
      const q = key && players.get(key);
      if (!q) return send(ws, { type: 'error', fatal: true, msg: 'الرابط غير صالح أو انتهى. اضغط زر المايك في اللعبة وخذ رابط جديد.' });
      const cid = String(m.clientId || '').slice(0, 64);
      if (q.bound && q.bound !== cid) return send(ws, { type: 'error', fatal: true, msg: 'هذا الرابط مرتبط بجهاز آخر. اطلب رابطاً جديداً من اللعبة.' });
      q.bound = cid;
      if (q.ws && q.ws !== ws) {
        send(q.ws, { type: 'error', fatal: true, msg: 'فُتح الموقع من مكان آخر.' });
        try { q.ws.close(); } catch {}
      }
      q.ws = ws; q.ready = false; p = q;
      return send(ws, { type: 'bound', name: q.name, displayName: q.displayName, userId: q.userId });
    }

    if (!p || players.get(p.key) !== p) return;

    if (m.type === 'ready') p.ready = true;
    else if (m.type === 'recs') {
      const list = Array.isArray(m.list) ? m.list.slice(0, 10) : [];
      p.recs = JSON.stringify(list.map((r) => ({ id: Number(r.id) || 0, n: Number(r.n) || 0, d: Number(r.d) || 0, s: String(r.s || '').slice(0, 80) })));
    }
    else if (m.type === 'level') { if (p.active) p.peak = Math.max(p.peak, clamp01(m.v)); }
    else if (m.type === 'signal') {
      const t = players.get(`${p.jobId}:${String(m.to)}`);
      if (t && t.ws && t.ready) send(t.ws, { type: 'signal', from: p.userId, data: m.data });
    }
  });

  ws.on('close', () => {
    if (p && p.ws === ws) { p.ws = null; p.ready = false; p.peak = 0; p.recs = '[]'; }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30000);

setInterval(() => {
  const now = Date.now();
  for (const [jobId, j] of jobs) {
    if (now - j.last < STALE_MS) continue;
    for (const p of [...players.values()]) if (p.jobId === jobId) removePlayer(p, 'سيرفر اللعبة انتهى.');
    jobs.delete(jobId);
  }
}, 5000);

server.listen(PORT, () => console.log(`Mic Bridge يعمل على المنفذ ${PORT}`));
