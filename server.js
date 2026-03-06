/**
 * OSSN Italia v8 — Backend Server
 *
 * Risolve il problema delle connessioni WebSocket dirette da browser verso
 * aisstream.io (policy/timing). Il server mantiene la connessione upstream
 * stabile e ri-espone i messaggi raw ai browser via ws://localhost:3001/ws.
 *
 * AVVIO:
 *   npm install
 *   node server.js
 *   → apri http://localhost:3001
 *   → attiva "Navi AIS live" nel pannello layer
 */

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const path = require('path');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const PORT     = 3001;
const AIS_KEY  = '7a197b841b6701faf083fe0480b223ca96722489';
const AIS_URL  = 'wss://stream.aisstream.io/v0/stream';
const BBOX     = [[[35, 6], [48, 19]]];
const MSG_TYPES = ['PositionReport', 'ClassBPositionReport', 'ShipStaticData'];

// ─── HTTP SERVER ─────────────────────────────────────────────────────────────
const app = express();

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.use(express.static(path.join(__dirname)));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/status', (req, res) => {
  res.json({
    ok: true,
    connected: upstream?.readyState === WebSocket.OPEN,
    ships: shipCount,
    clients: wss.clients.size,
    msgTotal
  });
});

const httpServer = http.createServer(app);

// ─── BROWSER WEBSOCKET SERVER ────────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer, path: '/ws' });

wss.on('connection', (client, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[ws] Browser connesso  (${wss.clients.size} totale)  ${ip}`);
  client.on('close', () => console.log(`[ws] Browser disconnesso (${wss.clients.size} rimasti)`));
  client.on('error', (e) => console.error('[ws] Errore client:', e.message));
});

function broadcast(data) {
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

// ─── AISSTREAM UPSTREAM ───────────────────────────────────────────────────────
let upstream = null;
let retries  = 0;
let msgTotal = 0;
let shipCount = 0;
const shipsSeen = new Set();

function connectUpstream() {
  console.log(`[ais] Connessione a aisstream.io… (tentativo ${retries + 1})`);
  upstream = new WebSocket(AIS_URL);

  upstream.on('open', () => {
    retries = 0;
    console.log('[ais] Connesso — invio subscription');
    setTimeout(() => {
      if (upstream.readyState === WebSocket.OPEN) {
        upstream.send(JSON.stringify({
          APIKey: AIS_KEY,
          BoundingBoxes: BBOX,
          FilterMessageTypes: MSG_TYPES
        }));
        console.log('[ais] Subscription inviata ✓');
      }
    }, 100);
  });

  upstream.on('message', (data) => {
    const str = data.toString();
    msgTotal++;

    // tracking contatore navi
    try {
      const msg = JSON.parse(str);
      if (msg.error || msg.Error) {
        console.error('[ais] Errore API:', msg.error || msg.Error);
        upstream.close();
        return;
      }
      const mmsi = msg.MetaData?.MMSI;
      if (mmsi && msg.MessageType !== 'ShipStaticData') {
        const had = shipsSeen.has(mmsi);
        shipsSeen.add(mmsi);
        if (!had) { shipCount = shipsSeen.size; }
      }
    } catch(_) {}

    if (msgTotal % 50 === 0) {
      process.stdout.write(`\r[ais] ${msgTotal} msg  ${shipCount} navi  ${wss.clients.size} browser   `);
    }

    broadcast(str);
  });

  upstream.on('error', (err) => {
    console.error('\n[ais] Errore:', err.message);
  });

  upstream.on('close', (code, reason) => {
    retries++;
    const delay = Math.min(2000 * Math.pow(2, retries - 1), 30000);
    console.log(`\n[ais] Chiuso (${code} "${reason}") — riconnessione in ${delay}ms`);
    setTimeout(connectUpstream, delay);
  });
}

// ─── AVVIO ───────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════╗
║       OSSN Italia v8 — AIS Backend Server          ║
╠════════════════════════════════════════════════════╣
║  Apri nel browser:  http://localhost:${PORT}           ║
║  Status API:        http://localhost:${PORT}/api/status ║
╠════════════════════════════════════════════════════╣
║  → Attiva "Navi AIS live" nel pannello Layer       ║
║  → Le navi appaiono sulla mappa entro ~30 secondi  ║
╚════════════════════════════════════════════════════╝
`);
  connectUpstream();
});

httpServer.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`[server] Porta ${PORT} già in uso. Chiudi l'altro processo e riprova.`);
  } else {
    console.error('[server] Errore:', err.message);
  }
  process.exit(1);
});

process.on('SIGINT', () => {
  console.log('\n[server] Chiusura...');
  if (upstream) upstream.close(1000);
  httpServer.close(() => process.exit(0));
});
