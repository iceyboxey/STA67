'use strict';
// STA 67 multiplayer server. Zero dependencies: HTTP + a small WebSocket implementation.
// The server owns the shared world: seed, unlocked houses, collected cash, wallets, moved cars and all pedestrians.
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path'), zlib = require('zlib');
const PORT = process.env.PORT || 8080;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'state.json');
const PUB = path.join(__dirname, 'public');
const S = 72;

/* ---------------- persistent world ---------------- */
let world = { seed: (Math.random() * 1e9) | 0, unlocked: [], collected: [], wallets: {}, cars: {} };
try { const d = JSON.parse(fs.readFileSync(DATA, 'utf8')); if (d && d.seed) world = Object.assign(world, d); } catch (e) {}
const unlocked = new Set(world.unlocked), collected = new Set(world.collected), cars = new Map(Object.entries(world.cars || {}));
let dirty = false;
function save() {
  if (!dirty) return; dirty = false;
  try { fs.writeFileSync(DATA, JSON.stringify({ seed: world.seed, unlocked: [...unlocked], collected: [...collected], wallets: world.wallets, cars: Object.fromEntries(cars) })); } catch (e) {}
}
setInterval(save, 10000);

/* ---------------- minimal WebSocket ---------------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const clients = new Map(); let nextId = 1;
function frame(op, payload) {
  const len = payload.length; let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
function sendRaw(c, buf) { if (c.socket.destroyed || c.socket.writableLength > 2e6) return; try { c.socket.write(buf); } catch (e) {} }
function send(c, obj) { sendRaw(c, frame(1, Buffer.from(JSON.stringify(obj)))); }
function bcast(obj, except) { const f = frame(1, Buffer.from(JSON.stringify(obj))); for (const c of clients.values()) if (c !== except && c.ready) sendRaw(c, f); }

function onUpgrade(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.url || '').split('?')[0] !== '/ws') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: nextId++, socket, buf: Buffer.alloc(0), frags: [], ready: false, name: 'Guest', token: '', x: 0, y: 0, z: 0, yaw: 0, d: null, seen: Date.now() };
  clients.set(c.id, c);
  socket.on('data', d => { c.buf = Buffer.concat([c.buf, d]); parse(c); });
  socket.on('close', () => drop(c)); socket.on('error', () => drop(c));
}
function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 15, masked = (b[1] & 0x80) !== 0; let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > 1e6) { c.socket.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
    let payload = b.subarray(off + (masked ? 4 : 0), need);
    if (masked) { const m = b.subarray(off, off + 4), out = Buffer.alloc(len); for (let i = 0; i < len; i++) out[i] = payload[i] ^ m[i & 3]; payload = out; }
    c.buf = b.subarray(need);
    if (op === 8) { try { c.socket.end(frame(8, Buffer.alloc(0))); } catch (e) {} return; }
    if (op === 9) { sendRaw(c, frame(10, payload)); continue; }
    if (op === 10) continue;
    if (op === 1 || op === 0) {
      c.frags.push(payload);
      if (fin) { const txt = Buffer.concat(c.frags).toString('utf8'); c.frags = []; try { onMsg(c, JSON.parse(txt)); } catch (e) {} }
    }
  }
}
function drop(c) {
  if (!clients.has(c.id)) return; clients.delete(c.id);
  for (const [id, car] of cars) if (car.driver === c.id) { car.driver = null; bcast({ t: 'carExit', id, x: car.x, z: car.z, yaw: car.yaw }); dirty = true; }
  bcast({ t: 'leave', id: c.id });
}

/* ---------------- pedestrians (server side) ---------------- */
const npcs = new Map(); let npcId = 1, npcSpawn = 0;
function rnd(a, b) { return a + Math.random() * (b - a); }
function npcNew(px, pz) {
  const ang = Math.random() * 6.283, d = rnd(30, 75), wx = px + Math.cos(ang) * d, wz = pz + Math.sin(ang) * d;
  const off = (Math.random() < 0.5 ? -1 : 1) * rnd(4.7, 6.2), sg = Math.random() < 0.5 ? 1 : -1;
  let x, z, dx = 0, dz = 0;
  if (Math.random() < 0.5) { x = Math.round(wx / S) * S + off; z = wz; dz = sg; } else { z = Math.round(wz / S) * S + off; x = wx; dx = sg; }
  npcs.set(npcId, { id: npcId, x, z, dx, dz, sp: rnd(1.2, 2.1), st: 'w', hurt: false, wi: 0, t: 0, c: (Math.random() * 1e6) | 0, pitch: rnd(0.85, 1.25) });
  npcId++;
}
function tickNpcs(dt) {
  const ps = [...clients.values()].filter(c => c.ready);
  npcSpawn -= dt;
  const want = Math.min(40, ps.length * 16);
  let alive = 0; for (const n of npcs.values()) if (n.st === 'w' || n.st === 'h') alive++;
  if (ps.length && alive < want && npcSpawn <= 0) { const p = ps[(Math.random() * ps.length) | 0]; npcNew(p.x, p.z); npcSpawn = 0.2; }
  for (const [id, n] of npcs) {
    if (n.st === 'w') {
      n.x += n.dx * n.sp * dt; n.z += n.dz * n.sp * dt;
      let near = false; for (const p of ps) if ((p.x - n.x) ** 2 + (p.z - n.z) ** 2 < 110 * 110) { near = true; break; }
      if (!near) npcs.delete(id);
    } else if (n.st === 'rag') {
      n.t -= dt; if (n.t <= 0) { n.st = 'rise'; n.t = 1.3; bcast({ t: 'npcRise', id, x: n.x, z: n.z }); }
    } else if (n.st === 'rise') {
      n.t -= dt; if (n.t <= 0) { n.st = 'h'; n.t = 2.4; n.hurt = true; }
    } else if (n.st === 'h') {
      n.t -= dt; if (n.t <= 0) { n.st = 'w'; n.sp = 0.9; }
    }
  }
}
function npcSnapshot() {
  const out = [];
  for (const n of npcs.values()) if (n.st === 'w' || n.st === 'h') out.push([n.id, +n.x.toFixed(2), +n.z.toFixed(2), n.dx, n.dz, +n.sp.toFixed(2), n.st === 'h' ? 1 : 0, n.hurt ? 1 : 0, n.wi, n.c, +n.pitch.toFixed(2)]);
  return out;
}

/* ---------------- messages ---------------- */
function num(v) { return typeof v === 'number' && isFinite(v) ? v : 0; }
function onMsg(c, m) {
  c.seen = Date.now();
  switch (m.t) {
    case 'hello': {
      c.name = String(m.name || 'Guest').slice(0, 16); c.token = String(m.token || ('anon' + c.id)).slice(0, 40);
      if (world.wallets[c.token] === undefined) world.wallets[c.token] = 0;
      c.ready = true;
      send(c, { t: 'welcome', id: c.id, seed: world.seed, unlocked: [...unlocked], collected: [...collected], cash: world.wallets[c.token],
        cars: [...cars].map(([id, k]) => Object.assign({ id }, k)), players: playersList(c) });
      break;
    }
    case 'p': c.x = num(m.x); c.y = num(m.y); c.z = num(m.z); c.yaw = num(m.yaw); c.d = m.d || null; c.f = num(m.f); c.mv = num(m.mv); c.ins = m.ins ? 1 : 0; break;
    case 'name': c.name = String(m.name || 'Guest').slice(0, 16); break;
    case 'carEnter': {
      const id = String(m.id), car = cars.get(id) || { x: num(m.x), z: num(m.z), yaw: num(m.yaw), col: num(m.col), driver: null };
      if (car.driver && car.driver !== c.id && clients.has(car.driver)) { send(c, { t: 'carDeny', id }); break; }
      car.driver = c.id; car.col = num(m.col) || car.col; cars.set(id, car); dirty = true;
      bcast({ t: 'carEnter', id, by: c.id, col: car.col, x: car.x, z: car.z, yaw: car.yaw }, c);
      break;
    }
    case 'car': {
      const car = cars.get(String(m.id)); if (!car || car.driver !== c.id) break;
      car.x = num(m.x); car.z = num(m.z); car.yaw = num(m.yaw); dirty = true;
      bcast({ t: 'car', id: String(m.id), x: car.x, z: car.z, yaw: car.yaw, vx: num(m.vx), vz: num(m.vz), w: num(m.w), st: num(m.st) }, c);
      break;
    }
    case 'carExit': {
      const car = cars.get(String(m.id)); if (!car || car.driver !== c.id) break;
      car.driver = null; car.x = num(m.x); car.z = num(m.z); car.yaw = num(m.yaw); dirty = true;
      bcast({ t: 'carExit', id: String(m.id), x: car.x, z: car.z, yaw: car.yaw }, c);
      break;
    }
    case 'unlock': { const k = String(m.key); if (!unlocked.has(k)) { unlocked.add(k); dirty = true; bcast({ t: 'unlock', key: k }, c); } break; }
    case 'collect': {
      const k = String(m.key); if (collected.has(k)) break;
      collected.add(k); const amt = Math.max(1, Math.min(500, m.amt | 0));
      world.wallets[c.token] = (world.wallets[c.token] || 0) + amt; dirty = true;
      send(c, { t: 'cash', v: world.wallets[c.token] }); bcast({ t: 'collected', key: k }, c);
      break;
    }
    case 'spend': {
      const v = Math.max(0, Math.min(100000, m.v | 0));
      if ((world.wallets[c.token] || 0) >= v) { world.wallets[c.token] -= v; dirty = true; }
      send(c, { t: 'cash', v: world.wallets[c.token] || 0 }); break;
    }
    case 'earn': {
      const v = Math.max(0, Math.min(30000, m.v | 0));
      world.wallets[c.token] = (world.wallets[c.token] || 0) + v; dirty = true;
      send(c, { t: 'cash', v: world.wallets[c.token] }); break;
    }
    case 'hit': {
      const n = npcs.get(m.id | 0); if (!n) break;
      if (n.st === 'rag' && m.gun && !m.cut) { n.t = 3.0; bcast({ t: 'npcHit', id: n.id, vx: num(m.vx), vy: num(m.vy), vz: num(m.vz), cut: 0, gun: 1, wi: m.wi | 0, ox: num(m.ox), oy: num(m.oy), oz: num(m.oz) }, c); break; }
      if (n.st !== 'w' && n.st !== 'h') break;
      const ev = { t: 'npcHit', id: n.id, vx: num(m.vx), vy: num(m.vy), vz: num(m.vz), cut: m.cut === true ? 1 : Math.max(0, Math.min(9, m.cut | 0)), gun: m.gun ? 1 : 0, wi: m.wi | 0, ox: num(m.ox), oy: num(m.oy), oz: num(m.oz) };
      bcast(ev, c);
      if (m.gun && !m.cut) { n.st = 'rag'; n.t = 3.0; n.wi = m.wi | 0; n.x += Math.max(-4, Math.min(4, ev.vx * 0.35)); n.z += Math.max(-4, Math.min(4, ev.vz * 0.35)); }
      else npcs.delete(n.id);
      break;
    }
    case 'pHit': {
      const t = clients.get(m.to | 0); if (!t || t === c || !t.ready || t.d) break;
      const now = Date.now(); if (t.ragUntil && now < t.ragUntil) break;
      t.ragUntil = now + 4600;
      const vx = Math.max(-6, Math.min(6, num(m.vx))), vz = Math.max(-6, Math.min(6, num(m.vz)));
      send(t, { t: 'shotAt', by: c.id, vx, vz }); bcast({ t: 'pRag', id: t.id, vx, vz }, t);
      break;
    }
    case 'shot': bcast({ t: 'shot', by: c.id, ox: num(m.ox), oy: num(m.oy), oz: num(m.oz), ex: num(m.ex), ey: num(m.ey), ez: num(m.ez) }, c); break;
  }
}
function playersList(except) {
  const out = [];
  for (const p of clients.values()) if (p.ready && p !== except) out.push([p.id, p.name, +p.x.toFixed(2), +p.y.toFixed(2), +p.z.toFixed(2), +p.yaw.toFixed(3), p.d, +(p.f || 0).toFixed(3), p.mv ? 1 : 0, p.ins ? 1 : 0]);
  return out;
}

/* ---------------- loops ---------------- */
let last = Date.now();
setInterval(() => { const now = Date.now(), dt = Math.min(0.2, (now - last) / 1000); last = now; tickNpcs(dt); }, 50);
setInterval(() => { for (const c of clients.values()) if (c.ready) send(c, { t: 'players', p: playersList(c) }); }, 66);
setInterval(() => { const s = npcSnapshot(); if (clients.size) bcast({ t: 'npcs', n: s }); }, 100);
setInterval(() => { const f = frame(9, Buffer.alloc(0)), now = Date.now(); for (const c of clients.values()) { if (now - c.seen > 60000) { c.socket.destroy(); continue; } sendRaw(c, f); } }, 20000);

/* ---------------- http ---------------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.txt': 'text/plain', '.json': 'application/json', '.css': 'text/css', '.glb': 'model/gltf-binary' };
const gz = new Map();
const srv = http.createServer((req, res) => {
  let u = decodeURIComponent((req.url || '/').split('?')[0]);
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: true, players: clients.size, npcs: npcs.size, seed: world.seed })); return; }
  if (u === '/' || u === '/index.html') u = '/index.html';
  const f = path.normalize(path.join(PUB, u));
  if (!f.startsWith(PUB) || !fs.existsSync(f) || !fs.statSync(f).isFile()) {
    if (u === '/index.html') { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('STA 67 multiplayer server is running (' + clients.size + ' online).\nPut the game file at public/index.html to serve it from here.'); return; }
    res.writeHead(404); res.end('not found'); return;
  }
  const st = fs.statSync(f), type = TYPES[path.extname(f)] || 'application/octet-stream', ae = String(req.headers['accept-encoding'] || '');
  const h = { 'Content-Type': type, 'Cache-Control': 'no-cache' };
  if (/gzip/.test(ae) && st.size > 2048) {
    const k = f + st.mtimeMs; let buf = gz.get(k);
    if (!buf) { buf = zlib.gzipSync(fs.readFileSync(f), { level: 6 }); gz.clear(); gz.set(k, buf); }
    h['Content-Encoding'] = 'gzip'; res.writeHead(200, h); res.end(buf); return;
  }
  res.writeHead(200, h); fs.createReadStream(f).pipe(res);
});
srv.on('upgrade', onUpgrade);
srv.listen(PORT, () => console.log('STA 67 server on :' + PORT + ' seed ' + world.seed));
process.on('SIGTERM', () => { dirty = true; save(); process.exit(0); });
