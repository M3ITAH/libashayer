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
const tu = d => {
  const u = d.user || d;
  const pic = u.profilePicture;
  return {
    source: 'tiktok', id: u.uniqueId, name: u.nickname || u.uniqueId,
    avatar: (pic?.urls || pic?.url || [])[0] || u.profilePictureUrl,
    badges: [u.isModerator && 'moderator', u.isSubscriber && 'subscriber'].filter(Boolean),
  };
};
function ttRetry(ms) {
  clearTimeout(ttTimer);
  ttTimer = setTimeout(ttConnect, ms);
}
function ttConnect() {
  stat('tiktok', { status: 'جاري الاتصال...' });
  if (tt) { try { tt.removeAllListeners(); tt.disconnect(); } catch {} }
  tt = new TikTokLiveConnection(CFG.tiktok, CFG.tiktokSignKey ? { signApiKey: CFG.tiktokSignKey } : {});
  const ev = (name, fallback) => (WebcastEvent && WebcastEvent[name]) || fallback;

  tt.on(ev('CHAT', 'chat'), d => emit({ ...tu(d), type: 'chat', text: d.comment }));
  tt.on(ev('GIFT', 'gift'), d => {
    const g = d.giftDetails || d;
    if (g.giftType === 1 && !d.repeatEnd) return;
    const n = d.repeatCount || 1;
    emit({ ...tu(d), type: 'gift', gift: g.giftName, count: n,
      diamonds: (g.diamondCount || 0) * n,
      icon: g.giftPictureUrl || g.giftImage?.imageUrl || g.icon?.urls?.[0] });
  });
  tt.on(ev('FOLLOW', 'follow'), d => emit({ ...tu(d), type: 'follow' }));
  tt.on(ev('SHARE', 'share'), d => emit({ ...tu(d), type: 'share' }));
  tt.on(ev('SUBSCRIBE', 'subscribe'), d => emit({ ...tu(d), type: 'sub' }));
  tt.on(ev('MEMBER', 'member'), d => emit({ ...tu(d), type: 'join' }));
  tt.on(ev('ROOM_USER', 'roomUser'), d => stat('tiktok', { viewers: d.viewerCount, live: true }));
  tt.on(ev('STREAM_END', 'streamEnd'), () => stat('tiktok', { status: 'انتهى البث', live: false }));
  tt.on((ControlEvent && ControlEvent.DISCONNECTED) || 'disconnected', () => {
    stat('tiktok', { status: 'انقطع الاتصال', live: false });
    ttRetry(5000);
  });

  tt.connect().then(s => {
    stat('tiktok', {
      status: 'متصل', live: true,
      followers: s.roomInfo?.owner?.follow_info?.follower_count,
      viewers: s.roomInfo?.user_count,
    });
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
