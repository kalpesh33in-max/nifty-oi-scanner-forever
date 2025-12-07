import os
import time
import threading
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# ==========================================================
#                WHATSAPP SENDER (UltraMsg)
# ==========================================================
ULTRA_INSTANCE = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRA_TOKEN = os.getenv("ULTRAMSG_TOKEN")
WA_TO = os.getenv("WHATSAPP_TO")  # group id or number

def send_wa(msg: str):
    """Send WhatsApp alert only."""
    if not ULTRA_INSTANCE or not ULTRA_TOKEN or not WA_TO:
        print("WA_OFF:", msg)
        return

    url = f"https://api.ultramsg.com/{ULTRA_INSTANCE}/messages/chat"
    try:
        r = requests.post(url, json={"token": ULTRA_TOKEN, "to": WA_TO, "body": msg}, timeout=10)
        print("WA SENT:", r.json())
    except Exception as e:
        print("WA ERROR:", e)


# ==========================================================
#                 SUPER SCANNER MAIN ENGINE
# ==========================================================
def run_super_scanner():

    NIFTY_LOT = 75
    ATM_RANGE = 450
    COOLDOWN_SEC = 70

    SUPER_A = {"SPIKE": 45, "LOTS": 45}
    SUPER_B = {"SPIKE": 80, "LOTS": 75}

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    BASE_HEADERS = {
        "authority": "www.nseindia.com",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://www.nseindia.com/option-chain",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "connection": "keep-alive",
    }
    session.headers.update(BASE_HEADERS)

    _last_cookie_refresh = 0

    def ensure_nse_session(force=False):
        nonlocal _last_cookie_refresh
        now = time.time()
        if not force and (now - _last_cookie_refresh < 1800):
            return
        try:
            r = session.get("https://www.nseindia.com", timeout=10)
            if r.status_code == 200:
                _last_cookie_refresh = now
                print("[NSE] Session refreshed")
            else:
                print("[NSE] Refresh failed:", r.status_code)
        except:
            pass

    def _ensure_json(r):
        ct = r.headers.get("content-type", "")
        if "json" not in ct.lower():
            raise RuntimeError("HTML_BLOCK")
        return r

    def now_ist():
        return datetime.now(IST)

    def market_open():
        t = now_ist()
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    # Global states
    latest = None
    latest_future = None
    blocked = False
    last_block = 0

    lock = threading.Lock()

    # =======================================================
    #                 FETCH FUTURE MONTHLY
    # =======================================================
    def fetch_future():
        nonlocal latest_future
        try:
            ensure_nse_session()
            r = session.get("https://www.nseindia.com/api/quote-derivative?symbol=NIFTY", timeout=10)
            if r.status_code in (401, 403, 429, 500):
                print("[FUT BLOCK]", r.status_code)
                return False

            _ensure_json(r)
            j = r.json()

            futs = [x for x in j.get("stocks", []) if x.get("instrumentType") == "FUTIDX"]
            if not futs:
                return False

            futs_sorted = sorted(
                futs,
                key=lambda x: datetime.strptime(x["expiryDate"], "%d-%b-%Y")
            )
            fut = futs_sorted[0]

            latest_future = {
                "price": fut["lastPrice"],
                "prev_price": fut["prevClose"],
                "oi": fut["openInterest"],
                "prev_oi": fut["openInterest"] - fut["changeinOpenInterest"],
                "expiry": fut["expiryDate"],
            }
            print("[FUT] OK:", fut["expiry"])
            return True

        except Exception as e:
            print("FUT ERROR:", e)
            return False

    # =======================================================
    #                 FETCH OPTION CHAIN
    # =======================================================
    def fetch_chain():
        nonlocal latest, blocked, last_block

        if blocked and (time.time() - last_block < 60):
            return False

        urls = [
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY",
        ]

        got = False

        for attempt in range(2):
            if attempt == 1 and not got:
                ensure_nse_session(force=True)
                time.sleep(2)

            for url in urls:
                try:
                    r = session.get(url, timeout=12)
                    if r.status_code in (401, 403, 429, 500):
                        print("[BLOCK]", r.status_code)
                        continue

                    _ensure_json(r)
                    j = r.json()
                    rec = j.get("records") or j.get("filtered") or {}
                    data = rec.get("data") or []
                    spot = round(rec.get("underlyingValue") or 0)

                    if data and spot > 15000:
                        with lock:
                            latest = (data, spot, time.time())

                        if blocked:
                            send_wa("🟢 Scanner Unblocked — NSE connection OK")
                            blocked = False

                        got = True
                        return True

                except:
                    continue

        if not got:
            last_block = time.time()
            if not blocked:
                blocked = True
                send_wa("🔴 Scanner Blocked — Retrying every 60 sec")
            return False

    # =======================================================
    #                    OPTION + FUTURE TRENDS
    # =======================================================
    def classify_option(price, price_old, oi, oi_old):
        if oi > oi_old and price > price_old: return "Buyer Dominant"
        if oi > oi_old and price < price_old: return "Writer Dominant"
        if oi < oi_old and price > price_old: return "Short Covering"
        if oi < oi_old and price < price_old: return "Long Unwinding"
        return "Mixed"

    def classify_future(f):
        if not f: return ("Unknown", "")
        p, pp = f["price"], f["prev_price"]
        o, po = f["oi"], f["prev_oi"]

        if o > po and p > pp: return ("Long Build-up", "Price↑ OI↑")
        if o > po and p < pp: return ("Short Build-up", "Price↓ OI↑")
        if o < po and p > pp: return ("Short Cover", "Price↑ OI↓")
        if o < po and p < pp: return ("Long Unwinding", "Price↓ OI↓")
        return ("Unknown", "")

    def market_bias(opt_type, opt_trend, fut_trend):
        fb = fut_trend in ["Long Build-up", "Short Cover"]
        fs = fut_trend in ["Short Build-up", "Long Unwinding"]

        if opt_type == "CE":
            if fs: return "Bearish"
            if fb: return "Weak Bullish"

        if opt_type == "PE":
            if fb: return "Strong Bearish"
            if fs: return "Bullish"

        return "Neutral"

    # =======================================================
    #                       FETCH LOOP
    # =======================================================
    def fetch_loop():
        ensure_nse_session(force=True)
        while True:
            if market_open():
                fetch_chain()
                fetch_future()
                time.sleep(30)
            else:
                time.sleep(60)

    # =======================================================
    #                    SUPER SCANNER LOGIC
    # =======================================================
    def scanner():
        hist = {}
        cooldown = {}

        send_wa("⚡ SUPER SCANNER ACTIVE")

        while True:
            time.sleep(1)

            if not market_open() or not latest:
                continue

            data, spot, ts = latest
            if time.time() - ts > 100:
                continue

            expiries = [x["expiryDate"] for x in data]

            for row in data:
                strike = row["strikePrice"]
                expiry = row["expiryDate"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt: continue

                    key = f"{strike}_{typ}_{expiry}"

                    oi = opt["openInterest"]
                    doi = opt["changeinOpenInterest"]
                    lots = abs(doi) // NIFTY_LOT
                    price = opt.get("lastPrice") or 0
                    iv = opt.get("impliedVolatility") or 0

                    if key not in hist:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    oi_old = hist[key]["oi"]
                    iv_old = hist[key]["iv"]
                    price_old = hist[key]["price"]

                    spike = ((oi - oi_old) / oi_old * 100) if oi_old else 0
                    ivroc = ((iv - iv_old) / iv_old * 100) if iv_old else 0

                    if time.time() - cooldown.get(key, 0) < COOLDOWN_SEC:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    trigger = None
                    if spike >= SUPER_B["SPIKE"] and lots >= SUPER_B["LOTS"]:
                        trigger = "👑 EXTREME SPIKE"
                    elif spike >= SUPER_A["SPIKE"] and lots >= SUPER_A["LOTS"]:
                        trigger = "🔥 SUPER SPIKE"

                    if trigger:
                        fut_trend, fut_msg = classify_future(latest_future)
                        opt_trend = classify_option(price, price_old, oi, oi_old)
                        bias = market_bias(typ, opt_trend, fut_trend)

                        if latest_future:
                            fut_price = latest_future["price"]
                            fut_oi = latest_future["oi"]
                            fut_doi = fut_oi - latest_future["prev_oi"]
                            fut_exp = latest_future["expiry"]
                        else:
                            fut_price = 0
                            fut_oi = 0
                            fut_doi = 0
                            fut_exp = "N/A"

                        msg = (
                            f"{trigger}\n\n"
                            f"Strike: {strike} {typ}\n"
                            f"Spot: {spot}\n\n"
                            f"Price: ₹{price}\n"
                            f"OI: {oi:,}  ΔOI: {doi:+,}  Lots: {lots}\n"
                            f"OI Spike: {spike:.1f}%\n"
                            f"IV: {iv:.2f}%  IV ROC: {ivroc:+.1f}%\n\n"
                            f"Option Trend: {opt_trend}\n"
                            f"Future Trend: {fut_trend} ({fut_msg})\n"
                            f"Market Bias: {bias}\n\n"
                            f"Future Price: ₹{fut_price}  OI: {fut_oi:,}  ΔOI: {fut_doi:+,}\n"
                            f"Future Expiry: {fut_exp}\n\n"
                            f"Time: {now_ist().strftime('%H:%M:%S')}"
                        )

                        send_wa(msg)
                        cooldown[key] = time.time()

                    hist[key] = {"oi": oi, "iv": iv, "price": price}

    # =======================================================
    #                     HEARTBEAT
    # =======================================================
    def heartbeat():
        while True:
            print("ALIVE:", now_ist())
            time.sleep(60)

    # =======================================================
    #                       MAIN
    # =======================================================
    def main():
        send_wa("🚀 SUPER SCANNER STARTED")

        threading.Thread(target=fetch_loop, daemon=True).start()
        threading.Thread(target=scanner, daemon=True).start()
        threading.Thread(target=heartbeat, daemon=True).start()

        while True:
            time.sleep(999999)

    main()


# ==========================================================
#                     SCRIPT ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    run_super_scanner()
