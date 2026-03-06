#!/usr/bin/env python3
"""
OSSN Italia v8 — Backend AIS
Zero dipendenze pip — richiede Python 3.8+ (preinstallato su Windows 10/11)

AVVIO:
  Windows  → doppio clic su  start.bat
  Mac/Linux → python3 server.py

BROWSER:  http://localhost:3001
          Attiva "Navi AIS live" nel pannello Layer
"""

import http.server
import socketserver
import threading
import socket
import ssl
import json
import struct
import os
import base64
import time
from urllib.parse import urlparse

PORT    = 3001
AIS_KEY = '7a197b841b6701faf083fe0480b223ca96722489'
AIS_HOST = 'stream.aisstream.io'
AIS_PATH = '/v0/stream'

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
            n = len(_clients)
            print(f'[sse] Browser connesso ({n} totale)')
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
                'ok':      True,
                'ships':   len(_ships),
                'clients': len(_clients),
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

# ─── WebSocket CLIENT (stdlib puro) ──────────────────────────────────────────

def _recv_exact(sock, n: int) -> bytes:
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError('socket chiuso')
        buf += chunk
    return buf

def _ws_send(sock, text: str):
    payload = text.encode()
    n = len(payload)
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    hdr = bytearray([0x81])
    if n < 126:
        hdr.append(0x80 | n)
    elif n < 65536:
        hdr.append(0x80 | 126)
        hdr += struct.pack('>H', n)
    else:
        hdr.append(0x80 | 127)
        hdr += struct.pack('>Q', n)
    hdr += mask
    sock.sendall(bytes(hdr) + masked)

def _ws_recv(sock) -> str | None:
    hdr    = _recv_exact(sock, 2)
    opcode = hdr[0] & 0x0F
    masked = bool(hdr[1] & 0x80)
    length = hdr[1] & 0x7F
    if opcode == 8:
        return None                          # close
    if opcode == 9:                          # ping → pong
        sock.sendall(b'\x8a\x00')
        return _ws_recv(sock)
    if length == 126:
        length = struct.unpack('>H', _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack('>Q', _recv_exact(sock, 8))[0]
    mk      = _recv_exact(sock, 4) if masked else b'\x00' * 4
    payload = _recv_exact(sock, length)
    payload = bytes(b ^ mk[i % 4] for i, b in enumerate(payload))
    if opcode in (1, 2):
        return payload.decode('utf-8', errors='replace')
    return ''

# ─── AIS CONNECTION LOOP ─────────────────────────────────────────────────────

def ais_loop():
    global _msg_total
    retries = 0
    while True:
        print(f'[ais] Connessione a {AIS_HOST} (tentativo {retries + 1})...')
        sock = None
        try:
            ctx = ssl.create_default_context()
            raw = socket.create_connection((AIS_HOST, 443), timeout=15)
            sock = ctx.wrap_socket(raw, server_hostname=AIS_HOST)

            # HTTP upgrade
            key = base64.b64encode(os.urandom(16)).decode()
            sock.sendall((
                f'GET {AIS_PATH} HTTP/1.1\r\n'
                f'Host: {AIS_HOST}\r\n'
                f'Upgrade: websocket\r\n'
                f'Connection: Upgrade\r\n'
                f'Sec-WebSocket-Key: {key}\r\n'
                f'Sec-WebSocket-Version: 13\r\n'
                f'User-Agent: OSSN/8.0\r\n'
                f'\r\n'
            ).encode())

            resp = b''
            while b'\r\n\r\n' not in resp:
                resp += sock.recv(4096)
            if b'101' not in resp:
                raise Exception(f'Handshake fallito: {resp[:120]}')

            retries = 0
            print('[ais] Connesso ✓')
            time.sleep(0.1)
            _ws_send(sock, json.dumps({
                'APIKey':             AIS_KEY,
                'BoundingBoxes':      [[[35, 6], [48, 19]]],
                'FilterMessageTypes': ['PositionReport', 'ClassBPositionReport', 'ShipStaticData'],
            }))
            print('[ais] Subscription inviata ✓')

            while True:
                data = _ws_recv(sock)
                if data is None:
                    break
                if not data:
                    continue
                _msg_total += 1
                try:
                    msg = json.loads(data)
                    err = msg.get('error') or msg.get('Error')
                    if err:
                        print(f'[ais] Errore API: {err}')
                        break
                    mmsi = (msg.get('MetaData') or {}).get('MMSI')
                    if mmsi and msg.get('MessageType') != 'ShipStaticData':
                        _ships.add(mmsi)
                except Exception:
                    pass
                if _msg_total % 50 == 0:
                    print(f'\r[ais] {_msg_total} msg  {len(_ships)} navi  {len(_clients)} browser   ', end='', flush=True)
                broadcast(data)

        except Exception as e:
            print(f'\n[ais] Errore: {e}')
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

        retries += 1
        delay = min(2 * (2 ** (retries - 1)), 30)
        print(f'\n[ais] Riconnessione in {delay}s...')
        time.sleep(delay)


# ─── AVVIO ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"""
╔══════════════════════════════════════════════════════╗
║         OSSN Italia v8 — AIS Backend                 ║
╠══════════════════════════════════════════════════════╣
║  Browser →  http://localhost:{PORT}                    ║
║  Status  →  http://localhost:{PORT}/api/status          ║
╠══════════════════════════════════════════════════════╣
║  Attiva "Navi AIS live" nel pannello Layer           ║
║  Le navi appaiono sulla mappa entro ~30 secondi      ║
╚══════════════════════════════════════════════════════╝
""")
    threading.Thread(target=ais_loop, daemon=True).start()
    httpd = ThreadedServer(('127.0.0.1', PORT), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\n[server] Arresto.')
