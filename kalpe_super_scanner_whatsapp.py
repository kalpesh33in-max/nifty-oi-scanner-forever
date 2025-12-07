import os
import time
import threading
import requests
import pytz
from datetime import datetime, timedelta, time as dtime

# ============================================================
# WHATSAPP SENDER  (UltraMSG)
# ============================================================
INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRA_TOKEN = os.getenv("ULTRAMSG_TOKEN")
WA_GROUP = os.getenv("ULTRAMSG_GROUP_ID")  # your group id

def wa_send(message):
    """Send WhatsApp message to group."""
    try:
        url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/group"
        payload = {
            "token": ULTRA_TOKEN,
            "groupId": WA_GROUP,
            "body": message
        }
        r = requests.post(url, json=payload, timeout=10)
        print("WA SENT:", r.json())
    except Exception as e:
        print("WA ERROR:", e)


# ============================================================
# START OF ORIGINAL SCANNER LOGIC (MODIFIED FOR WHATSAPP)
# ============================================================
def run_kalpe_super_scanner():

    NIFTY_LOT = 75
    ATM_RANGE = 450
    COOLDOWN_SEC = 65

    SUPER_A = {"SPIKE": 45, "LOTS": 45}
    SUPER_B = {"SPIKE": 80, "LOTS": 75}

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    # NSE Headers
    BASE_HEADERS = {
        "authority": "www.nseindia.com",
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0",
        "x-requested-with": "XMLHttpRequest"
    }
    session.headers.update(BASE_HEADERS)

    latest = None
    latest_future = None
    blocked = False
    last_block_time = 0
    lock = threading.Lock()

    # ------------------------------------------------------------
    def now_ist():
        return datetime.now(IST)

    def market_open():
        t = now_ist()
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    # ------------------------------------------------------------
    def ensure_session():
        try:
            session.get("https://www.nseindia.com", timeout=10)
            print("[NSE] Session OK")
        except:
            pass

    # ------------------------------------------------------------
    def fetch_option_chain():
        nonlocal latest, blocked, last_block_time

        if blocked and (time.time() - last_block_time) < 60:
            return False

        urls = [
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY"
        ]

        for url in urls:
            try:
                r = session.get(url, timeout=15)
                if r.status_code in (401, 403, 429):
                    print("[BLOCK]", r.status_code)
                    if not blocked:
                        blocked = True
                        wa_send("⚠ *NSE BLOCKED — Scanner Paused (Retrying)*")
                    last_block_time = time.time()
                    return False

                j = r.json()
                records = j.get("records") or j.get("filtered") or {}
                data = records.get("data") or []
                spot = round(records.get("underlyingValue") or 0)

                if data and spot > 15000:
                    latest = (data, spot, time.time())
                    if blocked:
                        blocked = False
                        wa_send("🟢 *NSE Unblocked — Scanner Resumed*")
                    print("[OC OK]", spot)
                    return True

            except Exception as e:
                print("FETCH ERROR:", e)

        return False

    # ------------------------------------------------------------
    def fetch_future():
        nonlocal latest_future
        try:
            url = "https://www.nseindia.com/api/quote-derivative?symbol=NIFTY"
            r = session.get(url, timeout=10)
            j = r.json()
            items = j.get("stocks", [])
            futs = [x for x in items if x.get("instrumentType") == "FUTIDX"]
            if not futs:
                return False
            futs.sort(key=lambda x: datetime.strptime(x["expiryDate"], "%d-%b-%Y"))
            fut = futs[0]

            latest_future = {
                "price": fut["lastPrice"],
                "prev_price": fut["prevClose"],
                "oi": fut["openInterest"],
                "prev_oi": fut["openInterest"] - fut["changeinOpenInterest"],
                "expiry": fut["expiryDate"]
            }
            return True

        except Exception as e:
            print("[FUTURE ERROR]", e)
            return False

    # ------------------------------------------------------------
    def fetch_loop():
        ensure_session()
        sent_open = False
        sent_close = False

        while True:
            if market_open():
                if not sent_open:
                    wa_send("🟢 *MARKET OPEN — Scanner Started*")
                    sent_open = True
                    sent_close = False

                fetch_option_chain()
                fetch_future()
                time.sleep(25)

            else:
                if not sent_close:
                    wa_send("🔴 *MARKET CLOSED — Scanner Sleeping*")
                    sent_close = True
                    sent_open = False
                time.sleep(60)

    # ------------------------------------------------------------
    def super_scanner():
        wa_send("⚡ *SUPER SCANNER ACTIVE*")

        hist = {}
        cooldown = {}

        while True:
            time.sleep(1)

            if not market_open() or not latest:
                continue

            data, spot, ts = latest
            if time.time() - ts > 120:
                continue

            for row in data:
                strike = row["strikePrice"]
                expiry = row["expiryDate"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt:
                        continue

                    oi = opt["openInterest"]
                    chg_oi = opt["changeinOpenInterest"]
                    lots = abs(chg_oi) // NIFTY_LOT
                    price = opt["lastPrice"]

                    key = f"{strike}_{typ}_{expiry}"

                    # first time
                    if key not in hist:
                        hist[key] = oi
                        continue

                    old_oi = hist[key]
                    spike = ((oi - old_oi) / old_oi * 100) if old_oi else 0

                    now_t = time.time()
                    if now_t - cooldown.get(key, 0) < COOLDOWN_SEC:
                        hist[key] = oi
                        continue

                    if spike >= SUPER_B["SPIKE"] and lots >= SUPER_B["LOTS"]:
                        msg = (
                            f"👑 *EXTREME SPIKE*\n"
                            f"{strike} {typ}\n"
                            f"OI Δ: {chg_oi:+,}  |  Lots: {lots}\n"
                            f"Spot: {spot}\n"
                            f"Time: {now_ist().strftime('%H:%M:%S')}"
                        )
                        wa_send(msg)
                        cooldown[key] = now_t

                    elif spike >= SUPER_A["SPIKE"] and lots >= SUPER_A["LOTS"]:
                        msg = (
                            f"🔥 *SUPER SPIKE*\n"
                            f"{strike} {typ}\n"
                            f"OI Δ: {chg_oi:+,}  |  Lots: {lots}\n"
                            f"Spot: {spot}\n"
                            f"Time: {now_ist().strftime('%H:%M:%S')}"
                        )
                        wa_send(msg)
                        cooldown[key] = now_t

                    hist[key] = oi

    # ------------------------------------------------------------
    threading.Thread(target=fetch_loop, daemon=True).start()
    threading.Thread(target=super_scanner, daemon=True).start()

    while True:
        print("ALIVE:", now_ist())
        time.sleep(60)


# ============================================================
if __name__ == "__main__":
    run_kalpe_super_scanner()
