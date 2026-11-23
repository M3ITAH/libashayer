// يجمع شات تيك توك + كيك ويرسله للصفحة. (تويتش يشتغل مباشرة من المتصفح)
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

const CFG = {
  tiktok: (process.env.TIKTOK_USER || 'lb10.q').replace('@', ''),
  twitch: process.env.TWITCH_CHANNEL || 'libashayer',
  kick: process.env.KICK_CHANNEL || 'lbashayer',
  kickRoom: process.env.KICK_CHATROOM_ID || '',
  twitchClientId: process.env.TWITCH_CLIENT_ID || '',
  tiktokSignKey: process.env.TIKTOK_SIGN_API_KEY || '',
};

const clients = new Set();
const history = [];
const stats = { tiktok: {}, twitch: {}, kick: {} };

function emit(o) {
  o.ts = o.ts || Date.now();
  history.push(o); if (history.length > 100) history.shift();
  const line = `data: ${JSON.stringify(o)}\n\n`;
  for (const r of clients) r.write(line);
}
function stat(src, patch) {
  Object.assign(stats[src], patch);
  const line = `data: ${JSON.stringify({ type: 'stats', source: src, ...patch })}\n\n`;
  for (const r of clients) r.write(line);
}

// ---------------- TikTok ----------------
let tt = null;
let ttTimer = null;
let giftMap = {};

// طباعة آمنة (بيانات تيك توك فيها أرقام BigInt تكسر JSON.stringify العادي)
const bigRep = (k, v) => (typeof v === 'bigint' ? v.toString() : v);
const clone = o => { try { return JSON.parse(JSON.stringify(o, bigRep)); } catch { return String(o); } };

// آخر عينات من بيانات تيك توك الخام، تنعرض في /debug عشان نتأكد من الحقول
const dbgBuf = { chat: [], gift: [], member: [], roomUser: [], room: [], giftList: [] };
const dbgAt = {};
function dbgPush(kind, raw, parsed) {
  const a = dbgBuf[kind], now = Date.now();
  if (a.length >= 3 && now - (dbgAt[kind] || 0) < 5000) return;
  dbgAt[kind] = now;
  a.push({ at: new Date().toISOString(), parsed, raw: clone(raw) });
  if (a.length > 3) a.shift();
}

// يجمع كل الروابط داخل أي كائن، ويفضّل صيغ الصور اللي المتصفح يعرضها (يتجنب heic)
function urlsIn(o, out = [], depth = 0) {
  if (!o || depth > 5) return out;
  if (typeof o === 'string') { if (/^https?:\/\//.test(o)) out.push(o); return out; }
  if (Array.isArray(o)) { o.forEach(x => urlsIn(x, out, depth + 1)); return out; }
  if (typeof o === 'object') Object.values(o).forEach(x => urlsIn(x, out, depth + 1));
  return out;
}
function bestImg(o) {
  const u = urlsIn(o);
  return u.find(x => /\.(jpe?g|png|webp)/i.test(x) && !/heic/i.test(x)) || u.find(x => !/heic/i.test(x)) || u[0] || '';
}
// يدور على رقم باسم من الأسماء المعطاة داخل أي كائن (يتحمل اختلاف شكل البيانات)
function findNum(o, keys, depth = 0) {
  if (!o || typeof o !== 'object' || depth > 4) return undefined;
  for (const k of keys) { const v = o[k]; if (typeof v === 'number' || typeof v === 'bigint') return Number(v); }
  for (const v of Object.values(o)) { const r = findNum(v, keys, depth + 1); if (r !== undefined) return r; }
  return undefined;
}
const tu = d => {
  const u = d.user || d;
  return {
    source: 'tiktok', id: u.uniqueId, name: u.nickname || u.uniqueId,
    avatar: bestImg(u.profilePicture) || bestImg(u.avatarThumb) || bestImg(u.avatarMedium) || bestImg(u.avatarLarger) || bestImg(u.profilePictureUrl) || bestImg(u.avatar),
    badges: [u.isModerator && 'moderator', u.isSubscriber && 'subscriber'].filter(Boolean),
  };
};

// عدد المشاهدين والمتابعين
function applyRoom(ri) {
  if (!ri) return;
  dbgPush('room', ri, null);
  const v = findNum(ri, ['user_count', 'userCount', 'viewerCount', 'viewer_count']);
  const f = findNum(ri.owner || ri, ['follower_count', 'followerCount']);
  const patch = {};
  if (v !== undefined) patch.viewers = v;
  if (f !== undefined) patch.followers = f;
  if (Object.keys(patch).length) stat('tiktok', { ...patch, live: true });
}
setInterval(async () => {
  if (!tt || !stats.tiktok.live || !tt.fetchRoomInfo) return;
  try { applyRoom(await tt.fetchRoomInfo()); } catch {}
}, 20000);

// عدد المتابعين: نقرأه من صفحة الحساب العامة في تيك توك (يشتغل حتى لو البث أوفلاين)
const ttFollowDbg = {};
async function ttFollowers() {
  try {
    const r = await fetch(`https://www.tiktok.com/@${encodeURIComponent(CFG.tiktok)}`, {
      headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36', 'accept-language': 'en-US,en;q=0.9' },
      signal: AbortSignal.timeout(12000),
    });
    const html = await r.text();
    const m = html.match(/"followerCount":\s*(\d+)/);
    Object.assign(ttFollowDbg, { at: new Date().toISOString(), http: r.status, found: m ? +m[1] : null, htmlLength: html.length });
    if (m) stat('tiktok', { followers: +m[1] });
  } catch (e) {
    Object.assign(ttFollowDbg, { at: new Date().toISOString(), error: String(e.message || e) });
  }
}
ttFollowers();
setInterval(ttFollowers, 120000);

// قائمة الهدايا (اسم + صورة + سعر) نستخدمها لو الهدية جت بدون اسم
async function loadGifts() {
  if (!tt || !tt.fetchAvailableGifts || Object.keys(giftMap).length) return;
  try {
    const r = await tt.fetchAvailableGifts();
    const arr = Array.isArray(r) ? r : (r?.gifts || r?.data?.gifts || []);
    for (const x of arr) giftMap[String(x.id)] = { name: x.name, diamonds: x.diamond_count ?? x.diamondCount, icon: bestImg(x.image || x.icon) };
    dbgPush('giftList', arr.slice(0, 2), { count: arr.length });
    console.log('tiktok gifts loaded:', Object.keys(giftMap).length);
  } catch (e) { console.log('tiktok gifts list:', e.message || e); }
}

function ttRetry(ms) {
  clearTimeout(ttTimer);
  ttTimer = setTimeout(ttConnect, ms);
}
function ttConnect() {
  stat('tiktok', { status: 'جاري الاتصال...' });
  if (tt) { try { tt.removeAllListeners(); tt.disconnect(); } catch {} }
  tt = new TikTokLiveConnection(CFG.tiktok, CFG.tiktokSignKey ? { signApiKey: CFG.tiktokSignKey } : {});
  const ev = (name, fallback) => (WebcastEvent && WebcastEvent[name]) || fallback;

  tt.on(ev('CHAT', 'chat'), d => {
    const text = d.comment || d.content || d.msg || d.text || d.message?.content || '';
    const u = tu(d);
    dbgPush('chat', d, { ...u, text });
    emit({ ...u, type: 'chat', text });
  });
  tt.on(ev('GIFT', 'gift'), d => {
    const g = d.giftDetails || d.gift || {};
    const id = String(d.giftId ?? g.id ?? g.giftId ?? '');
    const m = giftMap[id] || {};
    const name = g.giftName || g.name || d.giftName || m.name ||
      String(d.describe || g.describe || '').replace(/^.*?\b(sent|sends)\s+/i, '').replace(/\s*x\d+\s*$/i, '').trim();
    const type = g.giftType ?? g.type ?? d.giftType;
    const n = d.repeatCount || d.comboCount || 1;
    const unit = g.diamondCount ?? g.diamond_count ?? d.diamondCount ?? m.diamonds ?? 0;
    const icon = bestImg(g.giftImage || g.image || g.icon) || m.icon || d.giftPictureUrl || '';
    const u = tu(d);
    dbgPush('gift', d, { ...u, name, id, type, repeatEnd: d.repeatEnd, n, unit, icon });
    if (type === 1 && !d.repeatEnd) return;
    emit({ ...u, type: 'gift', gift: name, giftId: id, count: n, diamonds: unit * n, icon });
  });
  tt.on(ev('FOLLOW', 'follow'), d => emit({ ...tu(d), type: 'follow' }));
  tt.on(ev('SHARE', 'share'), d => emit({ ...tu(d), type: 'share' }));
  tt.on(ev('SUBSCRIBE', 'subscribe'), d => emit({ ...tu(d), type: 'sub' }));
  tt.on(ev('MEMBER', 'member'), d => { const u = tu(d); dbgPush('member', d, u); emit({ ...u, type: 'join' }); });
  tt.on(ev('ROOM_USER', 'roomUser'), d => {
    const v = findNum(d, ['viewerCount', 'viewer_count', 'userCount', 'user_count', 'total']);
    dbgPush('roomUser', d, { viewers: v });
    stat('tiktok', v !== undefined ? { viewers: v, live: true } : { live: true });
  });
  tt.on(ev('STREAM_END', 'streamEnd'), () => stat('tiktok', { status: 'انتهى البث', live: false }));
  tt.on((ControlEvent && ControlEvent.DISCONNECTED) || 'disconnected', () => {
    stat('tiktok', { status: 'انقطع الاتصال', live: false });
    ttRetry(5000);
  });

  tt.connect().then(s => {
    stat('tiktok', { status: 'متصل', live: true });
    applyRoom(s.roomInfo);
    loadGifts();
  }).catch(e => {
    console.log('tiktok:', e.message || e);
    stat('tiktok', { status: 'البث غير شغال، أعيد المحاولة...', live: false });
    ttRetry(15000);
  });
}
ttConnect();

// ---------------- Kick ----------------
let kickId = CFG.kickRoom;
const kickSeen = new Set();
let kickPollOk = null;
const kickHeaders = { 'user-agent': 'Mozilla/5.0', accept: 'application/json' };
const kickMsg = d => ({ source: 'kick', type: 'chat', id: d.sender?.username, name: d.sender?.username,
  color: d.sender?.identity?.color, text: d.content, badges: (d.sender?.identity?.badges || []).map(b => b.type),
  ts: d.created_at ? new Date(d.created_at).getTime() : undefined });
// قراءة شات كيك كل 4 ثواني (تشتغل حتى لو البث أوفلاين)
async function kickPoll() {
  if (!kickId) return;
  try {
    const r = await fetch(`https://kick.com/api/v2/chatrooms/${kickId}/messages`, { headers: kickHeaders });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const msgs = ((await r.json()).data?.messages || []).sort((x, y) => new Date(x.created_at) - new Date(y.created_at));
    const first = kickSeen.size === 0;
    for (const d of msgs.slice(first ? -50 : 0)) {
      if (!d.id || kickSeen.has(d.id)) continue;
      kickSeen.add(d.id);
      emit(kickMsg(d));
    }
    if (kickSeen.size > 600) { const keep = [...kickSeen].slice(-200); kickSeen.clear(); keep.forEach(x => kickSeen.add(x)); }
    if (kickPollOk !== true) { kickPollOk = true; stat('kick', { status: 'متصل (قراءة كل 4 ثواني)' }); }
  } catch (e) {
    console.log('kick poll:', e.message);
    if (kickPollOk !== false) { kickPollOk = false; stat('kick', { status: 'تعذر قراءة شات كيك (' + e.message + ')' }); }
  }
}
setInterval(kickPoll, 4000);

async function kickInfo() {
  try {
    const r = await fetch(`https://kick.com/api/v2/channels/${CFG.kick}`, {
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/json' },
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    if (!kickId) kickId = String(j.chatroom?.id || '');
    stat('kick', { followers: j.followers_count, viewers: j.livestream?.viewer_count ?? 0, live: !!j.livestream });
  } catch (e) {
    console.log('kick api:', e.message);
    stat('kick', { status: 'تعذر جلب بيانات كيك (' + e.message + ')' });
  }
}
function kickConnect() {
  if (!kickId) return setTimeout(() => kickInfo().then(kickConnect), 20000);
  const ws = new WebSocket('wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0-rc2&flash=false');
  ws.on('message', m => {
    let p; try { p = JSON.parse(m); } catch { return; }
    const data = () => { try { return JSON.parse(p.data); } catch { return {}; } };
    if (p.event === 'pusher:connection_established') {
      ws.send(JSON.stringify({ event: 'pusher:subscribe', data: { auth: '', channel: `chatrooms.${kickId}.v2` } }));
    } else if (p.event === 'pusher:ping') {
      ws.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
    } else if (p.event === 'App\\Events\\ChatMessageEvent') {
      const d = data();
      if (d.id && kickSeen.has(d.id)) return;
      if (d.id) kickSeen.add(d.id);
      emit(kickMsg(d));
    } else if (p.event === 'App\\Events\\SubscriptionEvent') {
      emit({ source: 'kick', type: 'sub', name: data().username });
    } else if (p.event === 'App\\Events\\GiftedSubscriptionsEvent') {
      const d = data();
      emit({ source: 'kick', type: 'subgift', name: d.gifter_username, count: (d.gifted_usernames || []).length });
    }
  });
  ws.on('close', code => {
    if (code === 4001) {
      console.log('kick ws: 4001 (الطريقة القديمة متوقفة)، الاعتماد على القراءة الدورية');
      return setTimeout(kickConnect, 600000);
    }
    setTimeout(kickConnect, 5000);
  });
  ws.on('error', e => console.log('kick ws:', e.message));
}
kickInfo().then(kickConnect);
setInterval(kickInfo, 30000);

// ---------------- صور الحسابات (تويتش + كيك) ----------------
const avCache = new Map();
function getAvatar(src, name) {
  if (!name || (src !== 'twitch' && src !== 'kick')) return Promise.resolve('');
  const key = src + ':' + name.toLowerCase();
  if (!avCache.has(key)) {
    if (avCache.size > 3000) avCache.clear();
    const p = (async () => {
      try {
        if (src === 'twitch') {
          const t = (await (await fetch('https://decapi.me/twitch/avatar/' + encodeURIComponent(name))).text()).trim();
          return /^https?:\/\//.test(t) ? t : '';
        }
        const slug = name.toLowerCase().replace(/_/g, '-');
        const r = await fetch(`https://kick.com/api/v2/channels/${encodeURIComponent(slug)}`, { headers: kickHeaders });
        if (!r.ok) return '';
        const j = await r.json();
        return j.user?.profile_pic || '';
      } catch { return ''; }
    })();
    avCache.set(key, p);
    p.then(u => { if (!u) setTimeout(() => avCache.delete(key), 300000); });
  }
  return avCache.get(key);
}

// صور تيك توك: نسمح بس بنطاقات تيك توك (حماية من استغلال السيرفر)
const IMG_OK = /(^|\.)(tiktokcdn\.com|tiktokcdn-us\.com|ibyteimg\.com|byteimg\.com|ibytedtos\.com|tiktokv\.com|tiktokv\.us|muscdn\.com)$/i;

// ---------------- HTTP ----------------
http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    for (const s of Object.keys(stats)) res.write(`data: ${JSON.stringify({ type: 'stats', source: s, ...stats[s] })}\n\n`);
    for (const h of history) res.write(`data: ${JSON.stringify(h)}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }
  if (url.pathname === '/avatar') {
    const src = url.searchParams.get('src') || '', name = (url.searchParams.get('name') || '').slice(0, 60);
    return getAvatar(src, name).then(u => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=3600' });
      res.end(JSON.stringify({ url: u }));
    });
  }
  if (url.pathname === '/debug') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ tiktokStats: stats.tiktok, followersFetch: ttFollowDbg, giftListSize: Object.keys(giftMap).length, samples: dbgBuf }, bigRep, 1));
  }
  if (url.pathname === '/img') {
    let u; try { u = new URL(url.searchParams.get('u') || ''); } catch { res.writeHead(400); return res.end(); }
    if (u.protocol !== 'https:' || !IMG_OK.test(u.hostname)) { res.writeHead(403); return res.end(); }
    return fetch(u, { redirect: 'manual', headers: { 'user-agent': 'Mozilla/5.0', referer: 'https://www.tiktok.com/' } }).then(async r => {
      const ct = r.headers.get('content-type') || '';
      if (!r.ok || !ct.startsWith('image/')) { res.writeHead(502); return res.end(); }
      const buf = Buffer.from(await r.arrayBuffer());
      if (buf.length > 2000000) { res.writeHead(413); return res.end(); }
      res.writeHead(200, { 'Content-Type': ct, 'Cache-Control': 'public, max-age=86400' });
      res.end(buf);
    }).catch(() => { res.writeHead(502); res.end(); });
  }
  if (url.pathname === '/config') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ tiktok: CFG.tiktok, twitch: CFG.twitch, kick: CFG.kick, twitchClientId: CFG.twitchClientId }));
  }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, buf) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(buf);
  });
}).listen(process.env.PORT || 3000, () => console.log('جاهز على المنفذ', process.env.PORT || 3000));

// ما نخلي أي خطأ غير متوقع من مكتبة تيك توك يطيّح السيرفر كله
process.on('unhandledRejection', e => console.log('unhandledRejection:', e?.message || e));
process.on('uncaughtException', e => console.log('uncaughtException:', e?.message || e));
