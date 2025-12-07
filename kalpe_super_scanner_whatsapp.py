import os
import time
import threading
import requests
import pytz
from datetime import datetime, timedelta, time as dtime


def run_kalpe_super_scanner():

    # ================== WHATSAPP VARIABLES ==================
    INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
    TOKEN = os.getenv("ULTRAMSG_TOKEN")
    GROUP_ID = os.getenv("ULTRAMSG_GROUP_ID")

    # ================== SEND WHATSAPP MESSAGE ==================
    def send(msg):
        url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/chat"
        payload = {
            "token": TOKEN,
            "to": GROUP_ID,
            "body": msg
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            print("WA SENT:", r.json())
        except Exception as e:
            print("WA ERROR:", e)

    # ================== YOUR ORIGINAL SETTINGS ==================
    NIFTY_LOT = 75
    ATM_RANGE = 450
    COOLDOWN_SEC = 65

    SUPER_A = {"SPIKE": 45, "LOTS": 45}
    SUPER_B = {"SPIKE": 80, "LOTS": 75}

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    # ================== NSE SESSION & HEADERS ==================
    BASE_HEADERS = {
        "authority": "www.nseindia.com",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,en-IN;q=0.8",
        "referer": "https://www.nseindia.com/option-chain",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "connection": "keep-alive",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "sec-ch-ua": '"Not/A)Brand";v="99", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "x-requested-with": "XMLHttpRequest",
    }
    session.headers.update(BASE_HEADERS)
    _last_cookie_refresh = 0.0

    def ensure_nse_session(force: bool = False):
        nonlocal _last_cookie_refresh
        now = time.time()

        if not force and (now - _last_cookie_refresh) < 1800:
            return

        try:
            r = session.get("https://www.nseindia.com", timeout=10)
            if r.status_code == 200:
                _last_cookie_refresh = now
                print("[NSE] Session refreshed OK")
            else:
                print("[NSE] Session refresh status:", r.status_code)
        except Exception as e:
            print("[NSE] Session refresh error:", e)

    def _ensure_json_response(r, label):
        ct = r.headers.get("content-type", "").lower()
        text_preview = r.text[:120].strip().lower()

        if "text/html" in ct or text_preview.startswith("<html"):
            raise RuntimeError(f"{label}: HTML_BLOCKED")

        return r

    # ================== TIME HELPERS ==================
    def now_ist():
        return datetime.now(IST)

    def market_open():
        t = now_ist()
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    # ================== GLOBAL STATE ==================
    latest = None
    latest_future = None
    blocked = False
    last_block_time = 0
    lock = threading.Lock()

    # -------------------------- FETCH FUTURE --------------------------
    def fetch_nifty_future_auto():
        nonlocal latest_future
        try:
            ensure_nse_session()
            url = "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY"
            r = session.get(url, timeout=10)

            if r.status_code in (401, 403, 429, 500):
                print("[FUTURE BLOCK]", r.status_code)
                return False

            r.raise_for_status()
            _ensure_json_response(r, "FUTURES")
            j = r.json()

            items = j.get("stocks", [])
            futures = [x for x in items if x.get("instrumentType") == "FUTIDX"]
            if not futures:
                return False

            fut = sorted(
                futures,
                key=lambda x: datetime.strptime(x["expiryDate"], "%d-%b-%Y")
            )[0]

            latest_future = {
                "price": fut.get("lastPrice", 0),
                "prev_price": fut.get("prevClose", 0),
                "oi": fut.get("openInterest", 0),
                "prev_oi": fut.get("openInterest", 0) - fut.get("changeinOpenInterest", 0),
                "expiry": fut.get("expiryDate", "")
            }

            print("[FUT OK]", latest_future["expiry"], latest_future["oi"])
            return True

        except Exception as e:
            print("[FUTURE ERROR]", e)
            return False

    # -------------------------- EXPIRY LABEL --------------------------
    def expiry_type(expiry, all_exp):
        unique_sorted = sorted(set(all_exp), key=lambda x: datetime.strptime(x, "%d-%b-%Y"))
        today = now_ist().date()

        valid = [
            e for e in unique_sorted
            if datetime.strptime(e, "%d-%b-%Y").date() >= today
        ]

        if not valid:
            return ""

        current = valid[0]
        next_week = valid[1] if len(valid) > 1 else None

        def last_thursday(dt):
            temp = datetime(dt.year, dt.month, 28)
            while temp.month == dt.month:
                temp += timedelta(days=1)
            temp -= timedelta(days=1)
            while temp.weekday() != 3:
                temp -= timedelta(days=1)
            return temp.strftime("%d-%b-%Y")

        monthly = None
        for e in valid:
            dt = datetime.strptime(e, "%d-%b-%Y")
            if e == last_thursday(dt):
                monthly = e

        if expiry == current:
            return "(Weekly)"
        if expiry == next_week:
            return "(Next Weekly)"
        if expiry == monthly:
            return "(Monthly)"
        return ""

    # -------------------------- OPTION TREND --------------------------
    def classify_option_trend(price_now, price_old, oi_now, oi_old):
        if oi_now > oi_old and price_now > price_old:
            return "Call Buy"
        if oi_now > oi_old and price_now < price_old:
            return "Call Write"
        if oi_now < oi_old and price_now > price_old:
            return "Short Cover"
        if oi_now < oi_old and price_now < price_old:
            return "Long Unwind"
        return "Mixed"

    # -------------------------- FUTURE TREND --------------------------
    def classify_future_trend(f):
        if not f:
            return "Unknown"

        p, pp = f["price"], f["prev_price"]
        o, po = f["oi"], f["prev_oi"]

        if o > po and p > pp:
            return "Future Buy"
        if o > po and p < pp:
            return "Future Sell"
        if o < po and p > pp:
            return "Short Cover"
        if o < po and p < pp:
            return "Long Unwind"
        return "Unknown"

    # -------------------------- MARKET BIAS --------------------------
    def market_bias(opt_type, opt_trend, fut_trend):
        if opt_type == "CE" and opt_trend == "Call Buy" and fut_trend == "Future Sell":
            return "Strong Bearish", "Call Buy + Future Sell"
        if opt_type == "PE" and opt_trend == "Call Buy" and fut_trend == "Future Buy":
            return "Strong Bullish", "Put Buy + Future Buy"
        return "Neutral", "No clear bias"

    # -------------------------- FETCH OPTION CHAIN --------------------------
    def fetch_data():
        nonlocal latest, blocked, last_block_time

        if blocked and (time.time() - last_block_time) < 60:
            return False

        urls = [
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY"
        ]

        got = False

        for attempt in range(2):
            if attempt == 1 and not got:
                ensure_nse_session(force=True)
                time.sleep(1)

            for url in urls:
                try:
                    r = session.get(url, timeout=15)

                    if r.status_code in (401, 403, 429, 500):
                        continue

                    r.raise_for_status()
                    _ensure_json_response(r, "OC")

                    j = r.json()
                    records = j.get("records") or j.get("filtered") or {}
                    data = records.get("data") or []
                    spot = round(records.get("underlyingValue") or 0)

                    if data and spot > 15000:
                        latest = (data, spot, time.time())
                        got = True
                        break

                except Exception as e:
                    print("OC ERR:", e)

            if got:
                break

        if not got:
            blocked = True
            last_block_time = time.time()
            return False

        return True

    # -------------------------- FETCH LOOP --------------------------
    def fetch_loop():
        ensure_nse_session(force=True)
        while True:
            if market_open():
                fetch_data()
                fetch_nifty_future_auto()
                time.sleep(30)
            else:
                time.sleep(60)

    # -------------------------- MAIN SUPER LOGIC --------------------------
    def super_scanner():
        hist = {}
        cooldown = {}

        send("⚡ KALPE SUPER SCANNER ACTIVE")

        while True:
            time.sleep(1)

            if not market_open() or not latest:
                continue

            data, spot, ts = latest
            if time.time() - ts > 120:
                continue

            expiries = [r["expiryDate"] for r in data]

            for row in data:
                strike = row["strikePrice"]
                expiry = row["expiryDate"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                label = expiry_type(expiry, expiries)

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt:
                        continue

                    key = f"{strike}_{typ}_{expiry}"

                    oi = opt["openInterest"]
                    chg_oi = opt["changeinOpenInterest"]
                    lots = abs(chg_oi) // NIFTY_LOT
                    iv = opt.get("impliedVolatility") or 0
                    price = opt.get("lastPrice") or 0

                    if key not in hist:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    oi_old = hist[key]["oi"]
                    iv_old = hist[key]["iv"]
                    price_old = hist[key]["price"]

                    spike = ((oi - oi_old) / oi_old * 100) if oi_old else 0
                    ivroc = ((iv - iv_old) / iv_old * 100) if iv_old else 0

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

                        fut = latest_future or {}

                        fut_trend = classify_future_trend(fut)
                        opt_trend = classify_option_trend(price, price_old, oi, oi_old)
                        bias, reason = market_bias(typ, opt_trend, fut_trend)

                        fut_price = fut.get("price", 0)
                        fut_oi = fut.get("oi", 0)
                        fut_doi = fut_oi - fut.get("prev_oi", 0)
                        fut_exp = fut.get("expiry", "N/A")

                        msg = (
                            f"{trigger}\n\n"
                            f"Strike: {strike} {typ}\n"
                            f"Spot: {spot} | Price: ₹{price}\n"
                            f"Expiry: {expiry} {label}\n\n"
                            f"📊 OPTION\n"
                            f"• OI: {oi:,} | ΔOI: {chg_oi:+,} | Lots: {lots}\n"
                            f"• OI%: {spike:+.2f}% | IV: {iv:.2f}% | IVROC: {ivroc:+.2f}%\n\n"
                            f"📈 FUTURE ({fut_exp})\n"
                            f"• Price: ₹{fut_price}\n"
                            f"• OI: {fut_oi:,} | ΔOI: {fut_doi:+,}\n"
                            f"• Trend: {opt_trend} + {fut_trend}\n\n"
                            f"📌 Market Bias: {bias}\n"
                            f"Reason: {reason}\n\n"
                            f"⏰ {now_ist().strftime('%H:%M:%S')}"
                        )

                        send(msg)
                        cooldown[key] = now_t

                    hist[key] = {"oi": oi, "iv": iv, "price": price}

    # -------------------------- HEARTBEAT --------------------------
    def heartbeat():
        while True:
            print("ALIVE:", now_ist())
            time.sleep(60)

    # -------------------------- MAIN RUNNER --------------------------
    def main():
        send("🚀 KALPE SUPER SCANNER STARTED")
        threading.Thread(target=fetch_loop, daemon=True).start()
        threading.Thread(target=super_scanner, daemon=True).start()
        threading.Thread(target=heartbeat, daemon=True).start()

        while True:
            time.sleep(99999)

    main()


# START ✔
if __name__ == "__main__":
    run_kalpe_super_scanner()
