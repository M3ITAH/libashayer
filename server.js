// يجمع شات تيك توك + كيك ويرسله للصفحة. (تويتش يشتغل مباشرة من المتصفح)
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const CFG = {
  tiktok: (process.env.TIKTOK_USER || 'lb10.q').replace('@', ''),
  twitch: process.env.TWITCH_CHANNEL || 'libashayer',
  kick: process.env.KICK_CHANNEL || 'lbashayer',
  kickRoom: process.env.KICK_CHATROOM_ID || '',
  twitchClientId: process.env.TWITCH_CLIENT_ID || '',
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
const tt = new WebcastPushConnection(CFG.tiktok);
const tu = d => ({
  source: 'tiktok', id: d.uniqueId, name: d.nickname || d.uniqueId, avatar: d.profilePictureUrl,
  badges: [d.isModerator && 'moderator', d.isSubscriber && 'subscriber'].filter(Boolean),
});
function ttConnect() {
  stat('tiktok', { status: 'جاري الاتصال...' });
  tt.connect().then(s => {
    stat('tiktok', {
      status: 'متصل', live: true,
      followers: s.roomInfo?.owner?.follow_info?.follower_count,
      viewers: s.roomInfo?.user_count,
    });
  }).catch(e => {
    console.log('tiktok:', e.message || e);
    stat('tiktok', { status: 'البث غير شغال، أعيد المحاولة...', live: false });
    setTimeout(ttConnect, 15000);
  });
}
tt.on('chat', d => emit({ ...tu(d), type: 'chat', text: d.comment }));
tt.on('gift', d => {
  if (d.giftType === 1 && !d.repeatEnd) return;
  emit({ ...tu(d), type: 'gift', gift: d.giftName, count: d.repeatCount || 1,
    diamonds: (d.diamondCount || 0) * (d.repeatCount || 1), icon: d.giftPictureUrl });
});
tt.on('follow', d => emit({ ...tu(d), type: 'follow' }));
tt.on('share', d => emit({ ...tu(d), type: 'share' }));
tt.on('subscribe', d => emit({ ...tu(d), type: 'sub' }));
tt.on('member', d => emit({ ...tu(d), type: 'join' }));
tt.on('roomUser', d => stat('tiktok', { viewers: d.viewerCount, live: true }));
tt.on('streamEnd', () => stat('tiktok', { status: 'انتهى البث', live: false }));
tt.on('disconnected', () => { stat('tiktok', { status: 'انقطع الاتصال', live: false }); setTimeout(ttConnect, 5000); });
ttConnect();

// ---------------- Kick ----------------
let kickId = CFG.kickRoom;
let kickHist = false;
// آخر رسائل كيك (محاولة، وإذا كيك رفض ما يصير شي)
async function kickHistory() {
  try {
    const r = await fetch(`https://kick.com/api/v2/chatrooms/${kickId}/messages`, {
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/json' },
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const msgs = ((await r.json()).data?.messages || [])
      .sort((a, b) => new Date(a.created_at) - new Date(b.created_at)).slice(-50);
    for (const d of msgs) emit({ source: 'kick', type: 'chat', id: d.sender?.username, name: d.sender?.username,
      color: d.sender?.identity?.color, text: d.content, badges: (d.sender?.identity?.badges || []).map(b => b.type) });
  } catch (e) { console.log('kick history:', e.message); }
}
async function kickInfo() {
  try {
    const r = await fetch(`https://kick.com/api/v2/channels/${CFG.kick}`, {
      headers: { 'user-agent': 'Mozilla/5.0', accept: 'application/json' },
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    if (!kickId) kickId = String(j.chatroom?.id || '');
    if (!kickHist && kickId) { kickHist = true; kickHistory(); }
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
      stat('kick', { status: 'متصل' });
    } else if (p.event === 'pusher:ping') {
      ws.send(JSON.stringify({ event: 'pusher:pong', data: {} }));
    } else if (p.event === 'App\\Events\\ChatMessageEvent') {
      const d = data();
      emit({ source: 'kick', type: 'chat', id: d.sender?.username, name: d.sender?.username,
        color: d.sender?.identity?.color, text: d.content,
        badges: (d.sender?.identity?.badges || []).map(b => b.type) });
    } else if (p.event === 'App\\Events\\SubscriptionEvent') {
      emit({ source: 'kick', type: 'sub', name: data().username });
    } else if (p.event === 'App\\Events\\GiftedSubscriptionsEvent') {
      const d = data();
      emit({ source: 'kick', type: 'subgift', name: d.gifter_username, count: (d.gifted_usernames || []).length });
    }
  });
  ws.on('close', code => {
    if (code === 4001) {
      stat('kick', { status: 'كيك أوقف طريقة الاتصال القديمة (4001) — شات كيك غير متاح حالياً' });
      return setTimeout(kickConnect, 600000);
    }
    stat('kick', { status: 'انقطع الاتصال (' + code + ')، أعيد المحاولة' });
    setTimeout(kickConnect, 5000);
  });
  ws.on('error', e => { console.log('kick ws:', e.message); stat('kick', { status: 'خطأ اتصال: ' + e.message }); });
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
