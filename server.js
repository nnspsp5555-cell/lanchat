const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const ROOT = __dirname;
const UPLOAD_DIR = path.join(ROOT, 'uploads');
const DATA_DIR = path.join(ROOT, 'data');
const HISTORY_FILE = path.join(DATA_DIR, 'messages.json');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(HISTORY_FILE)) fs.writeFileSync(HISTORY_FILE, '[]', 'utf8');

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 40 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    // Allow ordinary documents, photos, audio and video. Never execute uploads.
    cb(null, true);
  }
});
app.use(express.json({ limit: '100kb' }));
app.use('/uploads', express.static(UPLOAD_DIR, { fallthrough: false, dotfiles: 'deny' }));
app.use(express.static(path.join(ROOT, 'public')));

function readHistory() {
  try { const x = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); return Array.isArray(x) ? x : []; }
  catch { return []; }
}
function writeHistory(items) {
  const tmp = HISTORY_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(items.slice(-2000), null, 2), 'utf8');
  fs.renameSync(tmp, HISTORY_FILE);
}
let history = readHistory();
const clients = new Set();
const safeText = (v, max = 2000) => String(v ?? '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').trim().slice(0, max);
function send(ws, payload) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); }
function broadcast(payload, predicate = () => true) {
  const raw = JSON.stringify(payload);
  for (const c of clients) if (c.readyState === WebSocket.OPEN && predicate(c)) c.send(raw);
}
function visibleTo(message, c) {
  if (message.type === 'file' || message.type === 'message') {
    if (message.room !== c.room) return false;
    return message.chatType !== 'private' || message.from === c.username || message.to === c.username;
  }
  return true;
}
function pushMessage(message) {
  history.push(message);
  history = history.slice(-2000);
  writeHistory(history);
  broadcast({ type: 'message', message }, c => visibleTo(message, c));
}
function onlineUsers(room) {
  return [...new Set([...clients].filter(c => c.readyState === WebSocket.OPEN && c.room === room).map(c => c.username))].sort();
}

app.post('/api/upload', (req, res, next) => {
  upload.single('file')(req, res, err => {
    if (err) return res.status(err.code === 'LIMIT_FILE_SIZE' ? 413 : 400).json({ error: err.code === 'LIMIT_FILE_SIZE' ? 'الحد الأعلى للملف 40 ميغابايت' : 'تعذر رفع الملف' });
    if (!req.file) return res.status(400).json({ error: 'لم يتم اختيار ملف' });
    const original = path.basename(req.file.originalname || 'file').replace(/[^\p{L}\p{N}._ -]/gu, '_').slice(0, 120) || 'file';
    const ext = path.extname(original).slice(0, 12);
    const stored = req.file.filename + ext;
    const destination = path.join(UPLOAD_DIR, stored);
    fs.renameSync(req.file.path, destination);
    res.json({ name: original, url: '/uploads/' + encodeURIComponent(stored), mime: req.file.mimetype || 'application/octet-stream', size: req.file.size });
  });
});
app.get('/api/status', (_req, res) => res.json({ ok: true, app: 'LanChat Local', online: clients.size, historyCount: history.length, encrypted: false }));

wss.on('connection', ws => {
  ws.username = 'مستخدم';
  ws.room = 'عام';
  clients.add(ws);
  send(ws, { type: 'hello', history: history.filter(m => m.room === ws.room && m.chatType !== 'private').slice(-100), users: onlineUsers(ws.room) });
  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'صيغة الرسالة غير صحيحة' }); }
    if (data.type === 'join') {
      const oldRoom = ws.room;
      ws.username = safeText(data.username, 32) || 'مستخدم';
      ws.room = safeText(data.room, 40) || 'عام';
      send(ws, { type: 'hello', history: history.filter(m => m.room === ws.room && m.chatType !== 'private').slice(-100), users: onlineUsers(ws.room) });
      broadcast({ type: 'users', users: onlineUsers(oldRoom) }, c => c.room === oldRoom);
      broadcast({ type: 'users', users: onlineUsers(ws.room) }, c => c.room === ws.room);
      return;
    }
    if (data.type === 'send') {
      const text = safeText(data.text, 2000);
      if (!text) return;
      const chatType = data.chatType === 'private' ? 'private' : 'group';
      const to = chatType === 'private' ? safeText(data.to, 32) : '';
      if (chatType === 'private' && (!to || to === ws.username)) return send(ws, { type: 'error', message: 'اكتب اسم مستخدم آخر للمحادثة الخاصة' });
      if (chatType === 'private' && !onlineUsers(ws.room).includes(to)) return send(ws, { type: 'error', message: 'هذا المستخدم غير متصل في الغرفة الحالية' });
      pushMessage({ id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`, type: 'message', from: ws.username, to, chatType, room: ws.room, text, time: new Date().toISOString() });
      return;
    }
    if (data.type === 'file') {
      const file = data.file || {};
      const url = safeText(file.url, 300);
      if (!url.startsWith('/uploads/')) return send(ws, { type: 'error', message: 'رابط الملف غير صالح' });
      const chatType = data.chatType === 'private' ? 'private' : 'group';
      const to = chatType === 'private' ? safeText(data.to, 32) : '';
      if (chatType === 'private' && (!to || !onlineUsers(ws.room).includes(to))) return send(ws, { type: 'error', message: 'المستخدم الخاص غير متصل' });
      pushMessage({ id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`, type: 'file', from: ws.username, to, chatType, room: ws.room, file: { name: safeText(file.name, 120), url, mime: safeText(file.mime, 100), size: Number(file.size) || 0 }, time: new Date().toISOString() });
    }
  });
  ws.on('close', () => {
    clients.delete(ws);
    broadcast({ type: 'users', users: onlineUsers(ws.room) }, c => c.room === ws.room);
  });
});

const PORT = Number(process.env.PORT) || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`LanChat running on port ${PORT}`);
  for (const nets of Object.values(os.networkInterfaces())) for (const n of nets || []) if (n.family === 'IPv4' && !n.internal) console.log(`Open on same Wi-Fi: http://${n.address}:${PORT}`);
});
