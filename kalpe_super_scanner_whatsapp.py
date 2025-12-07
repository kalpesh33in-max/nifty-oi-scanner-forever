# kalpe_super_scanner_whatsapp.py
import os
import time
import threading
import requests
import pytz
from datetime import datetime, time as dtime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ==========================
# RAILWAY HEALTH CHECK SERVER (MUST BE FIRST)
# ==========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    port = int(os.getenv("PORT", 8080))
    try:
        server = HTTPServer(('0.0.0.0', port), HealthHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print(f"Health server running on port {port}")
    except Exception as e:
        print("Health server failed:", e)

# Start health server immediately
start_health_server()

# ==========================
# WHATSAPP CONFIG + ANTI-SPAM
# ==========================
ULTRA_INSTANCE = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRA_TOKEN = os.getenv("ULTRAMSG_TOKEN")
WA_TO = os.getenv("ULTRAMSG_GROUP_ID")

STARTUP_SENT = False

def wa_send(message: str):
    global STARTUP_SENT
    if any(x in message.upper() for x in ["STARTED", "ACTIVE", "SCANNER"]):
        if STARTUP_SENT:
            return
        STARTUP_SENT = True

    if not all([ULTRA_INSTANCE, ULTRA_TOKEN, WA_TO]):
        print("WhatsApp credentials missing")
        return

    try:
        url = f"https://api.ultramsg.com/{ULTRA_INSTANCE}/messages/chat"
        payload = {"token": ULTRA_TOKEN, "to": WA_TO, "body": message}
        requests.post(url, data=payload, timeout=10)
        print("WA → Sent")
    except Exception as e:
        print("WA Error:", e)

# ==========================
# MAIN SCANNER
# ==========================
def run_scanner():
    global STARTUP_SENT
    STARTUP_SENT = False

    NIFTY_LOT = 75
    ATM_RANGE = 500
    COOLDOWN_SEC = 80

    SUPER_A = {"SPIKE": 45, "LOTS": 45}
    SUPER_B = {"SPIKE": 75, "LOTS": 70}

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/129 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/129 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/129 Safari/537.36"
    ]

    def refresh_headers():
        session.headers.update({
            "User-Agent": USER_AGENTS[int(time.time()) % len(USER_AGENTS)],
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/option-chain",
            "Origin": "https://www.nseindia.com",
            "X-Requested-With": "XMLHttpRequest"
        })

    refresh_headers()
    latest = None
    blocked = False

    def ensure_session():
        try:
            session.get("https://www.nseindia.com", timeout=10)
            time.sleep(1)
            refresh_headers()
        except:
            pass

    def is_market_open():
        now = datetime.now(IST)
        return now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 30)

    def fetch_chain():
        nonlocal latest, blocked
        if blocked:
            time.sleep(60)
            return False

        urls = [
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY"
        ]
        for url in urls:
            try:
                time.sleep(3 + (time.time() % 4))
                r = session.get(url, timeout=15)
                if r.status_code == 200:
                    j = r.json()
                    rec = j.get("records") or j.get("filtered") or {}
                    data = rec.get("data") or []
                    spot = round(rec.get("underlyingValue") or 0)
                    if data and spot > 15000:
                        latest = (data, spot, time.time())
                        if blocked:
                            blocked = False
                            wa_send("Scanner Resumed")
                        return True
            except:
                continue
        blocked = True
        wa_send("NSE Temp Block — Retrying...")
        return False

    def scanner_loop():
        wa_send("KALPE SUPER SCANNER STARTED\nRunning 24x7 Smoothly")
        hist = {}
        cooldown = {}

        while True:
            if not is_market_open():
                time.sleep(60)
                continue
            if not latest:
                time.sleep(5)
                continue

            data, spot, ts = latest
            if time.time() - ts > 180:
                time.sleep(5)
                continue

            for row in data:
                strike = row["strikePrice"]
                if abs(strike - spot) > ATM_RANGE:
                    continue
                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt: continue
                    key = f"{strike}_{typ}_{row['expiryDate']}"
                    oi = opt["openInterest"]
                    chg = opt["changeinOpenInterest"]
                    lots = abs(chg) // NIFTY_LOT
                    iv = opt.get("impliedVolatility", 0)

                    if key not in hist:
                        hist[key] = {"oi": oi, "iv": iv}
                        continue

                    old_oi = hist[key]["oi"]
                    spike = (oi - old_oi) / old_oi * 100 if old_oi > 0 else 0

                    if time.time() - cooldown.get(key, 0) > COOLDOWN_SEC:
                        if spike >= SUPER_B["SPIKE"] and lots >= SUPER_B["LOTS"]:
                            wa_send(f"EXTREME SPIKE\n{strike} {typ}\nLots: {lots} | Spike: {spike:.1f}%\nIV: {iv:.1f}% | Spot: {spot}")
                            cooldown[key] = time.time()
                        elif spike >= SUPER_A["SPIKE"] and lots >= SUPER_A["LOTS"]:
                            wa_send(f"SUPER SPIKE\n{strike} {typ}\nLots: {lots} | Spike: {spike:.1f}%")
                            cooldown[key] = time.time()

                    hist[key] = {"oi": oi, "iv": iv}
            time.sleep(1)

    def fetch_loop():
        ensure_session()
        while True:
            if is_market_open():
                fetch_chain()
                time.sleep(75 + (time.time() % 30))
            else:
                time.sleep(60)

    def heartbeat():
        while True:
            print(f"ALIVE → {datetime.now(IST).strftime('%H:%M:%S')}")
            time.sleep(60)

    threading.Thread(target=fetch_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

    while True:
        time.sleep(3600)

if __name__ == "__main__":
    run_scanner()
