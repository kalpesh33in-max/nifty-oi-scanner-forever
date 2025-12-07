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

    # Base spike filters (you can still tune later)
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

    latest = None            # (data, spot, ts)
    latest_future = None     # dict with future details
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
        except Exception as e:
            print("[NSE] Refresh error:", e)

    def is_market_open():
        t = datetime.now(IST)
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    # -------------------------------
    # EXPIRY LABEL (Weekly / Monthly)
    # -------------------------------
    def expiry_type(expiry, all_exp):
        """
        Return label for option expiry:
        (Weekly) / (Next Weekly) / (Monthly)
        """
        if not all_exp:
            return ""

        # unique sorted dates
        unique_sorted = sorted(
            set(all_exp),
            key=lambda x: datetime.strptime(x, "%d-%b-%Y")
        )

        today = datetime.now(IST).date()

        valid = [
            e for e in unique_sorted
            if datetime.strptime(e, "%d-%b-%Y").date() >= today
        ]

        if not valid:
            return ""

        current_week = valid[0]
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

        if expiry == current_week:
            return "(Weekly)"
        if expiry == next_week:
            return "(Next Weekly)"
        if expiry == monthly:
            return "(Monthly)"
        return ""

    # -------------------------------
    # SIZE LABEL FOR LOTS
    # -------------------------------
    def size_label(lots: int) -> str:
        """
        Classify position size by lots
        > 50  : Small
        > 75  : Medium
        > 100 : High
        > 150 : Super High
        > 200 : Extreme
        """
        if lots >= 200:
            return "Extreme"
        if lots >= 150:
            return "Super High"
        if lots >= 100:
            return "High"
        if lots >= 75:
            return "Medium"
        if lots >= 50:
            return "Small"
        return "Tiny"

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

            f = futs[0]  # nearest expiry future

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

            # all expiries for label
            expiries = [row["expiryDate"] for row in data]

            for row in data:
                strike = row["strikePrice"]
                expiry = row["expiryDate"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                opt_exp_label = expiry_type(expiry, expiries)

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt:
                        continue

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
                    iv_change = iv - old_iv
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
                        # -------- Future side ----------
                        ftrend = future_trend(latest_future)

                        if latest_future:
                            f_price = latest_future["price"]
                            f_prev = latest_future["prev_price"]
                            f_oi = latest_future["oi"]
                            f_prev_oi = latest_future["prev_oi"]
                            f_expiry = latest_future["expiry"]

                            f_dprice = f_price - f_prev
                            f_doi = f_oi - f_prev_oi
                            f_lots = abs(f_doi) // NIFTY_LOT
                            f_size = size_label(f_lots)
                        else:
                            f_price = f_prev = f_oi = f_prev_oi = 0
                            f_dprice = f_doi = 0
                            f_lots = 0
                            f_size = "N/A"
                            f_expiry = "N/A"

                        # -------- Option trend ----------
                        opttrend = option_trend(price, old_price, oi, old_oi)

                        # -------- Combo signal lines ----------
                        combo_tags = []

                        future_buy = ftrend in ("Long Build-up", "Short Cover")
                        future_sell = ftrend in ("Short Build-up", "Long Unwinding")

                        # call buy + future sell
                        if typ == "CE" and opttrend in ("Buyer Dominant", "Short Covering") and future_sell:
                            combo_tags.append("CALL BUY + FUTURE SELL")

                        # put buy + future buy
                        if typ == "PE" and opttrend in ("Buyer Dominant", "Short Covering") and future_buy:
                            combo_tags.append("PUT BUY + FUTURE BUY")

                        # call / put write
                        if typ == "CE" and opttrend == "Writer Dominant":
                            combo_tags.append("CALL WRITE")
                        if typ == "PE" and opttrend == "Writer Dominant":
                            combo_tags.append("PUT WRITE")

                        combo_text = " | ".join(combo_tags) if combo_tags else "No strong combo"

                        opt_size = size_label(lots)

                        msg = (
                            f"{trigger}\n\n"
                            f"Option: NIFTY {strike} {typ}  Exp: {expiry} {opt_exp_label}\n"
                            f"Future: Exp: {f_expiry}\n\n"
                            f"Option OI: {oi:,}  (ΔOI: {chg:+,} | {spike:+.1f}% | Lots: {lots} → {opt_size})\n"
                            f"IV: {iv:.2f}%  (ΔIV: {iv_change:+.2f} | IV ROC: {ivroc:+.1f}%)\n\n"
                            f"Future Price: {f_price:.2f}  (Δ: {f_dprice:+.2f})\n"
                            f"Future OI: {f_oi:,}  (ΔOI: {f_doi:+,} | Lots: {f_lots} → {f_size})\n\n"
                            f"Option Trend: {opttrend}\n"
                            f"Future Trend: {ftrend}\n"
                            f"Combo Signal: {combo_text}\n\n"
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
