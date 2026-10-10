const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000, rooms = new Map();
let nid = 1;

const srv = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  try {
    const html = fs.readFileSync(path.join(__dirname, 'index.html'));
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(html);
  } catch (e) { res.writeHead(500); res.end('index.html not found'); }
});

const wss = new WebSocketServer({ server: srv, maxPayload: 4096 });
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const list = r => [...r.p.values()].map(p => ({ id: p.id, name: p.name, bike: p.bike, sc: p.sc, pl: p.pl }));
const bcast = (r, o, except) => r.p.forEach(p => { if (p !== except) send(p.ws, o); });
const clean = s => String(s || '').replace(/[<>&"]/g, '').trim().slice(0, 12);

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  let me = null, room = null;

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }

    if ((m.t === 'create' || m.t === 'join') && !me) {
      const name = clean(m.name) || 'Игрок', bike = Math.max(0, Math.min(8, m.bike | 0));
      if (m.t === 'create') {
        let code; do { code = 'MOTO-' + (100 + Math.floor(Math.random() * 900)); } while (rooms.has(code));
        room = { code, trk: Math.max(0, Math.min(1, m.trk | 0)), host: null, p: new Map(), started: false, fin: 0 };
        rooms.set(code, room);
      } else {
        room = rooms.get(String(m.code || '').toUpperCase());
        if (!room) { send(ws, { t: 'err', m: 'Комната не найдена' }); return; }
        if (room.started) { room = null; send(ws, { t: 'err', m: 'Гонка уже началась' }); return; }
        if (room.p.size >= 8) { room = null; send(ws, { t: 'err', m: 'Комната заполнена' }); return; }
      }
      me = { id: nid++, ws, name, bike, sc: 0, pl: 0 };
      room.p.set(me.id, me);
      if (!room.host) room.host = me;
      send(ws, { t: 'room', id: me.id, host: room.host === me, code: room.code, list: list(room), trk: room.trk });
      bcast(room, { t: 'players', list: list(room) }, me);
      return;
    }
    if (!me || !room) return;

    if (m.t === 'start' && me === room.host && !room.started) {
      room.started = true;
      bcast(room, { t: 'start', trk: room.trk });
    } else if (m.t === 'pos') {
      const o = { t: 'pos', id: me.id };
      for (const k of ['x', 'y', 'z', 'yaw', 'pit', 'rol', 'v', 'sc']) o[k] = +m[k] || 0;
      o.tp = Array.isArray(m.tp) ? m.tp.slice(0, 3).map(a => +a || 0) : [0, 0, 0];
      me.sc = o.sc;
      bcast(room, o, me);
    } else if (m.t === 'fin' && !me.pl) {
      me.pl = ++room.fin;
      me.sc = +m.sc || me.sc;
      bcast(room, { t: 'players', list: list(room) });
    }
  });

  ws.on('close', () => {
    if (!me || !room) return;
    room.p.delete(me.id);
    bcast(room, { t: 'left', id: me.id });
    if (!room.p.size) { rooms.delete(room.code); return; }
    bcast(room, { t: 'players', list: list(room) });
    if (room.host === me) {
      room.host = [...room.p.values()][0];
      if (!room.started) send(room.host.ws, { t: 'room', id: room.host.id, host: true, code: room.code, list: list(room), trk: room.trk });
    }
  });
});

setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 30000);
srv.listen(PORT, () => console.log('Server on ' + PORT));
