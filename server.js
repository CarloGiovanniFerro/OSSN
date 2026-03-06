/**
 * OSSN Italia v8 — Backend AIS
 * Zero dipendenze npm — richiede Node.js >= 21
 *
 * AVVIO:  node server.js
 * BROWSER: http://localhost:3001  → attiva "Navi AIS live"
 */

const http  = require('http');
const fs    = require('fs');
const path  = require('path');

const PORT    = 3001;
const AIS_KEY = '7a197b841b6701faf083fe0480b223ca96722489';
const AIS_URL = 'wss://stream.aisstream.io/v0/stream';

// ─── SSE CLIENT POOL ─────────────────────────────────────────────────────────
const clients = new Set();
let msgTotal  = 0;
let shipsSeen = new Set();

function broadcast(data) {
  const payload = `data: ${data}\n\n`;
  for (const res of clients) {
    try { res.write(payload); }
    catch(_) { clients.delete(res); }
  }
}

// ─── HTTP SERVER ─────────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.png':  'image/png',
  '.ico':  'image/x-icon',
};

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const url = req.url.split('?')[0];

  // ── SSE endpoint ─────────────────────────────────────────────────────────
  if (url === '/ais-stream') {
    res.writeHead(200, {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
    });
    res.flushHeaders();
    clients.add(res);
    console.log(`[sse] Browser connesso  (${clients.size} totale)`);
    req.on('close', () => {
      clients.delete(res);
      console.log(`[sse] Browser disconnesso (${clients.size} rimasti)`);
    });
    return;
  }

  // ── Status JSON ───────────────────────────────────────────────────────────
  if (url === '/api/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok:        true,
      connected: !!upstream && upstream.readyState === WebSocket.OPEN,
      ships:     shipsSeen.size,
      clients:   clients.size,
      msgTotal,
    }));
    return;
  }

  // ── File statici ──────────────────────────────────────────────────────────
  const target = url === '/' ? '/index.html' : url;
  const filePath = path.join(__dirname, target);
  const ext = path.extname(filePath);

  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403); res.end(); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ─── AISSTREAM UPSTREAM (WebSocket nativo Node 22) ───────────────────────────
let upstream = null;
let retries  = 0;

function connectAIS() {
  console.log(`[ais] Connessione a aisstream.io… (tentativo ${retries + 1})`);

  upstream = new WebSocket(AIS_URL);

  upstream.addEventListener('open', () => {
    retries = 0;
    console.log('[ais] Connesso — invio subscription');
    setTimeout(() => {
      if (upstream.readyState === WebSocket.OPEN) {
        upstream.send(JSON.stringify({
          APIKey:             AIS_KEY,
          BoundingBoxes:      [[[35, 6], [48, 19]]],
          FilterMessageTypes: ['PositionReport', 'ClassBPositionReport', 'ShipStaticData'],
        }));
        console.log('[ais] Subscription inviata ✓');
      }
    }, 100);
  });

  upstream.addEventListener('message', ({ data }) => {
    msgTotal++;

    try {
      const msg = JSON.parse(data);
      if (msg.error || msg.Error) {
        console.error('[ais] Errore API:', msg.error || msg.Error);
        upstream.close();
        return;
      }
      const mmsi = msg.MetaData?.MMSI;
      if (mmsi && msg.MessageType !== 'ShipStaticData') shipsSeen.add(mmsi);
    } catch(_) {}

    if (msgTotal % 50 === 0) {
      process.stdout.write(
        `\r[ais] ${msgTotal} msg  ${shipsSeen.size} navi  ${clients.size} browser   `
      );
    }

    broadcast(data);
  });

  upstream.addEventListener('error', ({ message }) => {
    console.error('\n[ais] Errore:', message || '(senza dettagli)');
  });

  upstream.addEventListener('close', ({ code, reason }) => {
    retries++;
    const delay = Math.min(2000 * Math.pow(2, retries - 1), 30000);
    console.log(`\n[ais] Chiuso (${code}) — riconnessione in ${delay / 1000}s`);
    setTimeout(connectAIS, delay);
  });
}

// ─── AVVIO ────────────────────────────────────────────────────────────────────
server.listen(PORT, '127.0.0.1', () => {
  console.log(`
╔══════════════════════════════════════════════════════╗
║         OSSN Italia v8 — AIS Backend                 ║
╠══════════════════════════════════════════════════════╣
║  Browser →  http://localhost:${PORT}                    ║
║  Status  →  http://localhost:${PORT}/api/status          ║
╠══════════════════════════════════════════════════════╣
║  Attiva "Navi AIS live" nel pannello Layer           ║
║  Le navi appaiono sulla mappa entro ~30 secondi      ║
╚══════════════════════════════════════════════════════╝
`);
  connectAIS();
});

server.on('error', err => {
  if (err.code === 'EADDRINUSE')
    console.error(`[server] Porta ${PORT} già in uso — chiudi l'altro processo.`);
  else
    console.error('[server] Errore:', err.message);
  process.exit(1);
});

process.on('SIGINT', () => {
  console.log('\n[server] Arresto...');
  if (upstream) upstream.close(1000);
  server.close(() => process.exit(0));
});
