#!/usr/bin/env python3
"""
OSSN Italia v8 — Backend AIS (sorgente: AISHub)
Zero dipendenze pip — richiede Python 3.8+ (preinstallato su Windows 10/11)

PREREQUISITO: account gratuito su https://www.aishub.net/join
              → inserire il proprio username in AISHUB_USER qui sotto

AVVIO:
  Windows   → doppio clic su  start.bat
  Mac/Linux → python3 server.py

BROWSER:  http://localhost:3001
          Attiva "Navi AIS live" nel pannello Layer
"""

import http.server
import socketserver
import threading
import json
import os
import time
from urllib.parse import urlparse, urlencode
from urllib.request import urlopen
from urllib.error import URLError

# ─── CONFIGURAZIONE ───────────────────────────────────────────────────────────
PORT          = 3001
AISHUB_USER   = 'AH_XXXXXXX'   # ← sostituire con il proprio username AISHub
POLL_INTERVAL = 65              # secondi tra un polling e il successivo (min 60)
BBOX          = dict(latmin=35, latmax=48, lonmin=6, lonmax=19)  # acque italiane

# ─── SSE CLIENT POOL ─────────────────────────────────────────────────────────
_lock      = threading.Lock()
_clients   = set()
_ships     = set()
_msg_total = 0

def broadcast(data: str):
    line = f'data: {data}\n\n'.encode()
    dead = set()
    with _lock:
        for wf in list(_clients):
            try:
                wf.write(line)
                wf.flush()
            except Exception:
                dead.add(wf)
        _clients.difference_update(dead)

# ─── HTTP + SSE HANDLER ───────────────────────────────────────────────────────
MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js':   'application/javascript',
    '.css':  'text/css',
    '.json': 'application/json',
    '.png':  'image/png',
    '.ico':  'image/x-icon',
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # log silenzioso

    def do_GET(self):
        path = urlparse(self.path).path

        # ── SSE stream ───────────────────────────────────────────────────────
        if path == '/ais-stream':
            self.send_response(200)
            self.send_header('Content-Type',  'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection',    'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            with _lock:
                _clients.add(self.wfile)
            print(f'[sse] Browser connesso ({len(_clients)} totale)')
            try:
                while True:
                    time.sleep(20)
                    self.wfile.write(b': ping\n\n')
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                with _lock:
                    _clients.discard(self.wfile)
                print(f'[sse] Browser disconnesso ({len(_clients)} rimasti)')
            return

        # ── Status JSON ──────────────────────────────────────────────────────
        if path == '/api/status':
            body = json.dumps({
                'ok':       True,
                'ships':    len(_ships),
                'clients':  len(_clients),
                'msgTotal': _msg_total,
            }).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)
            return

        # ── File statici ─────────────────────────────────────────────────────
        rel = '/index.html' if path == '/' else path
        fp  = os.path.normpath(os.path.join(BASE_DIR, rel.lstrip('/')))
        if not fp.startswith(BASE_DIR) or not os.path.isfile(fp):
            self.send_response(404)
            self.end_headers()
            return
        ext  = os.path.splitext(fp)[1].lower()
        mime = MIME.get(ext, 'application/octet-stream')
        with open(fp, 'rb') as f:
            data = f.read()
        self.send_response(200)
        self.send_header('Content-Type', mime)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

# ─── CONVERSIONE FORMATO AISHub → aisstream.io ───────────────────────────────

def _vessel_to_sse(v: dict) -> str | None:
    """Converte un record AISHub in JSON formato aisstream.io atteso dal frontend."""
    try:
        mmsi = int(v['MMSI'])
        lat  = int(v['LATITUDE'])  / 600000.0
        lon  = int(v['LONGITUDE']) / 600000.0
        sog  = int(v.get('SOG', 0))  / 10.0
        cog  = int(v.get('COG', 0))  / 10.0
        navstat = int(v.get('NAVSTAT', 0))
        name    = str(v.get('NAME', '')).strip()
        ship_type = int(v.get('TYPE', 0))
        dest    = str(v.get('DEST', '')).strip()
    except (KeyError, ValueError, TypeError):
        return None

    # Filtra coordinate nulle o fuori bbox (doppia verifica)
    if lat == 0.0 and lon == 0.0:
        return None
    if not (BBOX['latmin'] <= lat <= BBOX['latmax'] and
            BBOX['lonmin'] <= lon <= BBOX['lonmax']):
        return None

    msg = {
        'MessageType': 'PositionReport',
        'MetaData': {
            'MMSI':      mmsi,
            'latitude':  lat,
            'longitude': lon,
            'ShipName':  name,
            'Type':      ship_type,
        },
        'Message': {
            'PositionReport': {
                'Latitude':           lat,
                'Longitude':          lon,
                'Sog':                sog,
                'Cog':                cog,
                'NavigationalStatus': navstat,
            }
        },
    }
    if dest:
        msg['MetaData']['Destination'] = dest

    return json.dumps(msg)

# ─── AISHub POLLING LOOP ──────────────────────────────────────────────────────

_AISHUB_URL = 'https://data.aishub.net/ws.php'

def aishub_loop():
    global _msg_total

    if AISHUB_USER == 'AH_XXXXXXX':
        print('[ais] ATTENZIONE: inserire il proprio username AISHub in AISHUB_USER')
        print('[ais]             Registrazione gratuita su https://www.aishub.net/join')

    retries = 0
    while True:
        params = urlencode({
            'username': AISHUB_USER,
            'format':   1,
            'output':   'json',
            'compress': 0,
            **BBOX,
        })
        url = f'{_AISHUB_URL}?{params}'

        try:
            print(f'[ais] Polling AISHub...', end=' ', flush=True)
            with urlopen(url, timeout=20) as resp:
                raw = resp.read().decode('utf-8', errors='replace')

            records = json.loads(raw)

            # Il primo elemento è l'header di stato
            if not isinstance(records, list) or len(records) < 1:
                raise ValueError('Risposta inattesa')

            header = records[0]
            err = header.get('ERROR', '')
            if err:
                print(f'\n[ais] Errore AISHub: {err}')
                retries += 1
                time.sleep(min(30, 5 * retries))
                continue

            vessels = records[1:]
            count   = 0
            for v in vessels:
                sse = _vessel_to_sse(v)
                if sse is None:
                    continue
                mmsi = v.get('MMSI')
                if mmsi:
                    _ships.add(int(mmsi))
                _msg_total += 1
                count += 1
                broadcast(sse)

            retries = 0
            print(f'{count} navi  (totale {len(_ships)} MMSI unici, {len(_clients)} browser)')

        except URLError as e:
            retries += 1
            delay = min(2 * (2 ** (retries - 1)), 60)
            print(f'\n[ais] Errore rete: {e.reason} — riprovo in {delay}s')
            time.sleep(delay)
            continue
        except Exception as e:
            retries += 1
            delay = min(2 * (2 ** (retries - 1)), 60)
            print(f'\n[ais] Errore: {e} — riprovo in {delay}s')
            time.sleep(delay)
            continue

        time.sleep(POLL_INTERVAL)

# ─── AVVIO ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"""
╔══════════════════════════════════════════════════════╗
║         OSSN Italia v8 — AIS Backend (AISHub)        ║
╠══════════════════════════════════════════════════════╣
║  Browser →  http://localhost:{PORT}                    ║
║  Status  →  http://localhost:{PORT}/api/status          ║
╠══════════════════════════════════════════════════════╣
║  Sorgente dati: AISHub (polling ogni {POLL_INTERVAL}s)        ║
║  Attiva "Navi AIS live" nel pannello Layer           ║
╚══════════════════════════════════════════════════════╝
""")
    threading.Thread(target=aishub_loop, daemon=True).start()
    httpd = ThreadedServer(('127.0.0.1', PORT), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\n[server] Arresto.')
