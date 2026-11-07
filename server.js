const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  } else { res.writeHead(404); res.end(); }
});

const wss = new WebSocketServer({ server });
const clients = new Map();   // ws -> {id, name, room, voice}
const history = {};          // room -> [сообщения]
let nextId = 1;

const send = (ws, msg) => ws.readyState === 1 && ws.send(JSON.stringify(msg));
const users = room => [...clients.values()]
  .filter(c => c.room === room).map(({ id, name, voice }) => ({ id, name, voice }));
function broadcast(room, msg) {
  for (const [ws, c] of clients) if (c.room === room) send(ws, msg);
}

wss.on('connection', ws => {
  const me = { id: nextId++, name: null, room: null, voice: false };
  clients.set(ws, me);

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    switch (m.type) {
      case 'join':
        me.name = String(m.name || '').slice(0, 32) || 'Аноним';
        me.room = String(m.room || '').slice(0, 32) || 'general';
        send(ws, { type: 'welcome', id: me.id, history: history[me.room] || [] });
        broadcast(me.room, { type: 'users', users: users(me.room) });
        broadcast(me.room, { type: 'system', text: `${me.name} зашёл` });
        break;
      case 'chat': {
        if (!me.room) return;
        const msg = { type: 'chat', name: me.name, text: String(m.text).slice(0, 2000), ts: Date.now() };
        (history[me.room] ??= []).push(msg);
        if (history[me.room].length > 100) history[me.room].shift();
        broadcast(me.room, msg);
        break;
      }
      case 'voice':
        me.voice = !!m.on;
        broadcast(me.room, { type: 'users', users: users(me.room) });
        break;
      case 'signal': // пересылка WebRTC-сигналов
        for (const [w, c] of clients)
          if (c.id === m.to && c.room === me.room) send(w, { type: 'signal', from: me.id, data: m.data });
        break;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    if (me.room) {
      broadcast(me.room, { type: 'users', users: users(me.room) });
      broadcast(me.room, { type: 'system', text: `${me.name} вышел` });
    }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Сервер: http://localhost:${PORT}`));
