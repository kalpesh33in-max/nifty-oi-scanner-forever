import os
import time
import threading
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# ==========================
# WHATSAPP CONFIG
# ==========================
ULTRA_INSTANCE = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRA_TOKEN = os.getenv("ULTRAMSG_TOKEN")
WA_TO = os.getenv("ULTRAMSG_GROUP_ID")  # group_id@g.us


def wa_send(message: str):
    """Send message via UltraMSG WhatsApp API"""
    try:
        url = f"https://api.ultramsg.com/{ULTRA_INSTANCE}/messages/chat"
        payload = {
            "token": ULTRA_TOKEN,
            "to": WA_TO,
            "body": message
        }
        r = requests.post(url, data=payload, timeout=10)
        print("WA SENT:", r.text[:120])
    except Exception as e:
        print("WA ERROR:", e)


# ==========================
# MAIN SCANNER
# ==========================
def run_scanner():

    NIFTY_LOT = 75
    ATM_RANGE = 450
    COOLDOWN_SEC = 65

    SUPER_A = {"SPIKE": 45, "LOTS": 45}
    SUPER_B = {"SPIKE": 80, "LOTS": 75}

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    BASE_HEADERS = {
        "user-agent": "Mozilla/5.0",
        "accept": "*/*",
        "referer": "https://www.nseindia.com/option-chain"
    }
    session.headers.update(BASE_HEADERS)

    latest = None
    latest_future = None
    blocked = False
    last_block = 0
    lock = threading.Lock()

    _cookie_ts = 0

    # -------------------------------
    # NSE SESSION MANAGEMENT
    # -------------------------------
    def ensure_session(force=False):
        nonlocal _cookie_ts
        now = time.time()
        if not force and (now - _cookie_ts) < 1800:
            return

        try:
            r = session.get("https://www.nseindia.com", timeout=10)
            if r.status_code == 200:
                _cookie_ts = now
                print("[NSE] Session refreshed OK")
            else:
                print("[NSE] Refresh failed:", r.status_code)
        except:
            pass

    def is_market_open():
        t = datetime.now(IST)
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    # -------------------------------
    # FETCH FUTURES
    # -------------------------------
    def fetch_futures():
        nonlocal latest_future
        try:
            ensure_session()
            url = "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY"
            r = session.get(url, timeout=10)

            if r.status_code in (401, 403, 429, 500):
                print("[FUT BLOCK]", r.status_code)
                return False

            j = r.json()
            items = j.get("stocks", [])
            futs = [x for x in items if x.get("instrumentType") == "FUTIDX"]
            if not futs:
                return False

            futs = sorted(
                futs,
                key=lambda x: datetime.strptime(x["expiryDate"], "%d-%b-%Y")
            )

            f = futs[0]

            latest_future = {
                "price": f.get("lastPrice", 0),
                "prev_price": f.get("prevClose", 0),
                "oi": f.get("openInterest", 0),
                "prev_oi": f.get("openInterest", 0) - f.get("changeinOpenInterest", 0),
                "expiry": f.get("expiryDate", "")
            }
            print("[FUT OK]", latest_future["expiry"])
            return True

        except Exception as e:
            print("[FUT ERROR]", e)
            return False

    # -------------------------------
    # FETCH OPTION CHAIN
    # -------------------------------
    def fetch_chain():
        nonlocal latest, blocked, last_block

        if blocked and (time.time() - last_block) < 60:
            return False

        urls = [
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY"
        ]

        ok = False

        for url in urls:
            try:
                r = session.get(url, timeout=15)
                if r.status_code in (401, 403, 429, 500):
                    print("[BLOCK]", url, r.status_code)
                    continue

                j = r.json()
                rec = j.get("records") or j.get("filtered") or {}
                rows = rec.get("data") or []
                spot = round(rec.get("underlyingValue") or 0)

                if rows and spot > 15000:
                    with lock:
                        latest = (rows, spot, time.time())
                    if blocked:
                        blocked = False
                        wa_send("🟢 *Scanner Resumed (NSE Unblocked)*")
                    ok = True
                    print("[OC OK] spot:", spot)
                    break

            except Exception as e:
                print("[CHAIN ERROR]", e)

        if not ok:
            if not blocked:
                blocked = True
                last_block = time.time()
                wa_send("🔴 *Scanner Blocked — Retrying every 1 min*")
            else:
                last_block = time.time()
        return ok

    # -------------------------------
    # TREND CALCULATORS
    # -------------------------------
    def option_trend(now_p, old_p, oi, old_oi):
        if oi > old_oi and now_p > old_p:
            return "Buyer Dominant"
        if oi > old_oi and now_p < old_p:
            return "Writer Dominant"
        if oi < old_oi and now_p > old_p:
            return "Short Covering"
        if oi < old_oi and now_p < old_p:
            return "Long Unwinding"
        return "Neutral"

    def future_trend(f):
        if not f:
            return "Unknown"

        p, pp = f["price"], f["prev_price"]
        o, po = f["oi"], f["prev_oi"]

        if o > po and p > pp:
            return "Long Build-up"
        if o > po and p < pp:
            return "Short Build-up"
        if o < po and p > pp:
            return "Short Cover"
        if o < po and p < pp:
            return "Long Unwinding"
        return "Unknown"

    # -------------------------------
    # SPIKE SCANNER
    # -------------------------------
    def scanner_loop():
        wa_send("🚀 *SUPER-SPIKE SCANNER STARTED*")

        hist = {}
        cooldown = {}

        market_open_sent = False
        market_close_sent = False

        while True:
            time.sleep(1)

            # MARKET START MESSAGE
            if is_market_open() and not market_open_sent:
                wa_send("🟢 *Market Live — Scanner Active*")
                market_open_sent = True
                market_close_sent = False

            # MARKET CLOSE MESSAGE
            if (not is_market_open()) and not market_close_sent:
                wa_send("🔴 *Market Closed — Scanner Sleeping*")
                market_close_sent = True
                market_open_sent = False

            if not is_market_open():
                continue

            if not latest:
                continue

            data, spot, ts = latest
            if time.time() - ts > 120:
                continue

            for row in data:
                strike = row["strikePrice"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt:
                        continue

                    expiry = row["expiryDate"]
                    key = f"{strike}_{typ}_{expiry}"

                    oi = opt["openInterest"]
                    chg = opt["changeinOpenInterest"]
                    lots = abs(chg) // NIFTY_LOT

                    iv = opt.get("impliedVolatility") or 0
                    price = opt.get("lastPrice") or 0

                    if key not in hist:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    old = hist[key]
                    old_oi, old_iv, old_price = old["oi"], old["iv"], old["price"]

                    spike = ((oi - old_oi) / old_oi * 100) if old_oi else 0
                    ivroc = ((iv - old_iv) / old_iv * 100) if old_iv else 0

                    now_t = time.time()
                    if now_t - cooldown.get(key, 0) < COOLDOWN_SEC:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    trigger = None
                    if spike >= SUPER_B["SPIKE"] and lots >= SUPER_B["LOTS"]:
                        trigger = "👑 EXTREME SPIKE"
                    elif spike >= SUPER_A["SPIKE"] and lots >= SUPER_A["LOTS"]:
                        trigger = "🔥 SUPER SPIKE"

                    if trigger:
                        ftrend = future_trend(latest_future)
                        opttrend = option_trend(price, old_price, oi, old_oi)

                        msg = (
                            f"{trigger}\n\n"
                            f"Strike: {strike} {typ}\n"
                            f"Price: ₹{price} | OI: {oi:,} | ΔOI: {chg:+,} | Lots: {lots}\n"
                            f"IV: {iv:.2f}% | IV ROC: {ivroc:+.1f}%\n\n"
                            f"Option Trend: {opttrend}\n"
                            f"Future Trend: {ftrend}\n\n"
                            f"Spot: {spot}\n"
                            f"Time: {datetime.now(IST).strftime('%H:%M:%S')} IST"
                        )

                        wa_send(msg)
                        cooldown[key] = now_t

                    hist[key] = {"oi": oi, "iv": iv, "price": price}

    # -------------------------------
    # FETCH LOOP THREAD
    # -------------------------------
    def fetch_loop():
        ensure_session(force=True)
        wa_send("⚡ Scanner Active")

        while True:
            if is_market_open():
                fetch_chain()
                fetch_futures()
                time.sleep(30)
            else:
                time.sleep(60)

    # -------------------------------
    # HEARTBEAT
    # -------------------------------
    def heartbeat():
        while True:
            print("ALIVE:", datetime.now(IST))
            time.sleep(60)

    # -------------------------------
    # START THREADS
    # -------------------------------
    threading.Thread(target=fetch_loop, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

    while True:
        time.sleep(99999)


# RUN
if __name__ == "__main__":
    run_scanner()
