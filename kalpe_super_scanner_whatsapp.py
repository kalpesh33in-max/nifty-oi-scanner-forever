import os
import time
import pytz
import requests
from datetime import datetime, time as dtime

# ------------------------------
#  LOAD ENV VARIABLES
# ------------------------------
INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")         # example: instance154723
TOKEN = os.getenv("ULTRAMSG_TOKEN")                     # example: 1ixul9eyweanldnt
GROUP_ID = os.getenv("WHATSAPP_TO")                     # example: 120363408331503118@g.us

# ------------------------------
#  WHATSAPP GROUP ALERT FUNCTION
# ------------------------------
def send_whatsapp_group(msg):
    try:
        url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/group"

        payload = {
            "token": TOKEN,
            "to": GROUP_ID,
            "body": msg
        }

        r = requests.post(url, data=payload)
        print("WA SENT:", r.text)

    except Exception as e:
        print("WA ERROR:", e)


# ------------------------------
#  NSE OPTION DATA FETCH
# ------------------------------
def fetch_option_data(symbol):
    try:
        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        r = requests.get(url, headers=headers, timeout=10)
        return r.json()

    except Exception as e:
        print("NSE ERROR:", e)
        return None


# ------------------------------
#  SUPER-SPIKE LOGIC
# ------------------------------
def check_super_spike(symbol):
    data = fetch_option_data(symbol)
    if not data:
        return

    calls = []
    puts = []

    try:
        for item in data["records"]["data"]:
            ce = item.get("CE")
            pe = item.get("PE")

            if ce:
                calls.append({
                    "strike": ce["strikePrice"],
                    "oi": ce["openInterest"],
                    "chng_oi": ce["changeinOpenInterest"],
                    "price": ce.get("lastPrice", 0)
                })

            if pe:
                puts.append({
                    "strike": pe["strikePrice"],
                    "oi": pe["openInterest"],
                    "chng_oi": pe["changeinOpenInterest"],
                    "price": pe.get("lastPrice", 0)
                })

        # ------------------------------
        # DETECT SUPER-SPIKE CE SIDE
        # ------------------------------
        for c in calls:
            if c["chng_oi"] >= 75000:   # 75+ lots
                msg = f"""
🔥 SUPER-SPIKE SCANNER ACTIVE (WhatsApp Group)

Symbol: {symbol}
Strike: {c['strike']}
CE Price: {c['price']}
OI Change: {c['chng_oi']}
"""
                send_whatsapp_group(msg)

            elif c["chng_oi"] >= 50000:  # 50+ lots
                msg = f"""
💥 KALPE SUPER-SPIKE STARTED (WhatsApp Group)

Symbol: {symbol}
Strike: {c['strike']}
CE Price: {c['price']}
OI Change: {c['chng_oi']}
"""
                send_whatsapp_group(msg)

        # ------------------------------
        # DETECT SUPER-SPIKE PE SIDE
        # ------------------------------
        for p in puts:
            if p["chng_oi"] >= 75000:
                msg = f"""
🔥 SUPER-SPIKE SCANNER ACTIVE (WhatsApp Group)

Symbol: {symbol}
Strike: {p['strike']}
PE Price: {p['price']}
OI Change: {p['chng_oi']}
"""
                send_whatsapp_group(msg)

            elif p["chng_oi"] >= 50000:
                msg = f"""
💥 KALPE SUPER-SPIKE STARTED (WhatsApp Group)

Symbol: {symbol}
Strike: {p['strike']}
PE Price: {p['price']}
OI Change: {p['chng_oi']}
"""
                send_whatsapp_group(msg)

    except Exception as e:
        print("LOGIC ERROR:", e)


# ------------------------------
#  MARKET TIMING
# ------------------------------
IST = pytz.timezone("Asia/Kolkata")

def market_open_now():
    now = datetime.now(IST).time()
    return dtime(9,15) <= now <= dtime(15,30)


# ------------------------------
#  MAIN LOOP
# ------------------------------
print("🔄 Kalpe Bhai SUPER-SPIKE Scanner Started…")

while True:
    try:
        if market_open_now():
            print("[ALIVE] Market Open — Checking...")
            check_super_spike("NIFTY")
            check_super_spike("BANKNIFTY")

        else:
            print("[SLEEP] Market Closed…")
            time.sleep(60)

        time.sleep(5)

    except Exception as e:
        print("FATAL:", e)
        time.sleep(10)
