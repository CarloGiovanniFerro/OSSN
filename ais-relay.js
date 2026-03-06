#!/usr/bin/env node
/**
 * AIS Relay — proxy locale per aisstream.io
 *
 * Risolve il problema delle connessioni WebSocket dirette da browser
 * verso aisstream.io (CORS / timing / policy).
 *
 * UTILIZZO:
 *   npm install ws          (solo la prima volta)
 *   node ais-relay.js
 *
 * Poi in index.html attivare "Navi AIS live" — il relay è già configurato
 * come sorgente locale (ws://localhost:8765).
 *
 * La chiave AIS è già embedded nel relay. Modifica AIS_KEY se necessario.
 */

const WebSocket = require('ws');

const AIS_KEY    = '7a197b841b6701faf083fe0480b223ca96722489';
const LOCAL_PORT = 8765;
const UPSTREAM   = 'wss://stream.aisstream.io/v0/stream';

const SUBSCRIPTION = JSON.stringify({
  APIKey: AIS_KEY,
  BoundingBoxes: [[[35, 6], [48, 19]]],
  FilterMessageTypes: ['PositionReport', 'ClassBPositionReport', 'ShipStaticData']
});

const server = new WebSocket.Server({ port: LOCAL_PORT });
let upstreamWs = null;
let clients = new Set();
let msgCount = 0;

function connectUpstream() {
  if (upstreamWs && upstreamWs.readyState === WebSocket.OPEN) return;

  console.log('[relay] Connessione a aisstream.io…');
  upstreamWs = new WebSocket(UPSTREAM);

  upstreamWs.on('open', () => {
    console.log('[relay] Connesso — invio subscription');
    upstreamWs.send(SUBSCRIPTION);
  });

  upstreamWs.on('message', (data) => {
    msgCount++;
    if (msgCount % 100 === 0) process.stdout.write(`\r[relay] ${msgCount} messaggi ricevuti, ${clients.size} client connessi   `);
    const str = data.toString();
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) client.send(str);
    }
  });

  upstreamWs.on('error', (err) => {
    console.error('\n[relay] Errore upstream:', err.message);
  });

  upstreamWs.on('close', (code, reason) => {
    console.warn(`\n[relay] Upstream chiuso — code:${code} reason:"${reason}" — riconnessione in 5s`);
    setTimeout(connectUpstream, 5000);
  });
}

server.on('connection', (client, req) => {
  console.log(`\n[relay] Browser connesso (${clients.size + 1} totale)`);
  clients.add(client);

  // Avvia upstream se non attivo
  connectUpstream();

  client.on('close', () => {
    clients.delete(client);
    console.log(`\n[relay] Browser disconnesso (${clients.size} rimasti)`);
  });

  client.on('error', (err) => {
    console.error('[relay] Errore client:', err.message);
    clients.delete(client);
  });
});

server.on('listening', () => {
  console.log(`
╔═══════════════════════════════════════════════╗
║        AIS Relay — OSSN Italia v8             ║
╠═══════════════════════════════════════════════╣
║  Porta locale : ws://localhost:${LOCAL_PORT}          ║
║  Upstream     : aisstream.io                  ║
║  Chiave AIS   : ${AIS_KEY.substring(0,8)}…          ║
╠═══════════════════════════════════════════════╣
║  Apri index.html e attiva "Navi AIS live"     ║
║  Il layer si connette automaticamente         ║
║  al relay locale invece di aisstream.io       ║
╚═══════════════════════════════════════════════╝
`);
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`[relay] Porta ${LOCAL_PORT} già in uso. Chiudi l'altro relay e riprova.`);
  } else {
    console.error('[relay] Errore server:', err.message);
  }
  process.exit(1);
});

process.on('SIGINT', () => {
  console.log('\n[relay] Chiusura...');
  if (upstreamWs) upstreamWs.close();
  server.close(() => process.exit(0));
});
