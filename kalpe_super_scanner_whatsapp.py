import os
import time
import json
import random
import requests
import pytz
from datetime import datetime

# =====================================================
#  WHATSAPP (UltraMSG)
# =====================================================
ULTRA_INSTANCE = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRA_TOKEN = os.getenv("ULTRAMSG_TOKEN")
WA_TO = os.getenv("ULTRAMSG_GROUP_ID")  # group_id@g.us OR 91xxxxxxxxxx

def wa_send(message: str):
    """Send WhatsApp message via UltraMSG"""
    try:
        url = f"https://api.ultramsg.com/{ULTRA_INSTANCE}/messages/chat"
        payload = {
            "token": ULTRA_TOKEN,
            "to": WA_TO,
            "body": message
        }
        r = requests.post(url, data=payload, timeout=10)
        print("WA SENT:", r.text[:150])
    except Exception as e:
        print("WA ERROR:", e)


# =====================================================
#  SAFE GET (Prevents NSE Block)
# =====================================================
def safe_get(url, headers=None):
    """NSE safe GET with retry + delay"""
    for attempt in range(4):
        try:
            time.sleep(random.uniform(0.7, 1.5))
            r = requests.get(url, headers=headers, timeout=10)

            if r.status_code == 200:
                return r.json()

            print(f"[NSE ERROR] {url} → {r.status_code}")
        except Exception as e:
            print("[SAFE_GET]", e)

        time.sleep(1.2)

    return None


# =====================================================
#  CONSTANTS
# =====================================================
LOT_SIZE = {"NIFTY": 50}
IST = pytz.timezone("Asia/Kolkata")


# =====================================================
#  SPIKE LABELS
# =====================================================
def spike_label(lots):
    if lots >= 200: return "SUPER EXTREME"
    if lots >= 150: return "EXTREME"
    if lots >= 100: return "SUPER HIGH"
    if lots >= 75:  return "HIGH"
    if lots >= 50:  return "MEDIUM"
    if lots >= 25:  return "SMALL"
    return ""


# =====================================================
#  TREND CALCULATOR
# =====================================================
def option_trend(price, old_price, oi, old_oi):
    if oi > old_oi and price > old_price:
        return "Buyer Dominant"
    if oi > old_oi and price < old_price:
        return "Writer Dominant"
    if oi < old_oi and price > old_price:
        return "Short Covering"
    if oi < old_oi and price < old_price:
        return "Long Unwinding"
    return "Neutral"


# =====================================================
#  FORMAT WHATSAPP MESSAGE (2 COLUMN LAYOUT)
# =====================================================
def build_msg(opt, fut, spot, fut_exp):

    return f"""
🟢 {opt['alert']} — {opt['type']} {opt['trend']}

🟦 OPTION DATA                     | 🟥 FUTURE DATA
----------------------------------|----------------------------------
Expiry: {opt['expiry']}           | Future Price: {fut['price']}
Strike: {opt['strike']}           | Fut ΔPrice: {fut['dprice']}
Price: ₹{opt['price']}            | Fut OI: {fut['oi']}
OI: {opt['oi']}                   | ΔOI: {fut['doi']}
ΔOI: {opt['doi']}                 | Fut OI %: {fut['oi_pct']}
OI %: {opt['oi_pct']}             | Fut Lots: {fut['lots']} 
Lots: {opt['lots']}               | Trend: {fut['trend']}
IV: {opt['iv']}                   | {fut['trend2']}
IV ROC: {opt['ivroc']}            |
Trend: {opt['trend']}             |

Spot: {spot}  •  Fut Expiry: {fut_exp}
Time: {datetime.now(IST).strftime("%H:%M:%S IST")}
""".strip()


# =====================================================
#  MAIN LOOP
# =====================================================
last_alert = ""

wa_send("🟢 SAFE NIFTY SCANNER STARTED…")

while True:
    try:
        # ------------------------
        # SPOT PRICE
        # ------------------------
        spot_json = safe_get("https://www.nseindia.com/api/market-data-pre-open?key=NIFTY")
        if not spot_json:
            print("SPOT FAIL")
            continue

        spot = spot_json["marketData"][0]["indexLastValue"]


        # ------------------------
        # OPTION CHAIN
        # ------------------------
        oc = safe_get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY")
        if not oc:
            print("OPTION FAIL")
            continue

        expiry = oc["records"]["expiryDates"][0]
        data = oc["records"]["data"]

        # Pick near-the-money CE
        row = next((x for x in data if "CE" in x), data[0])
        CE = row["CE"]

        strike = CE["strikePrice"]
        price = CE["lastPrice"]
        oi = CE["openInterest"]
        doi = CE["changeinOpenInterest"]
        iv = CE["impliedVolatility"]
        oi_pct = CE.get("pchangeinOpenInterest", 0)

        lots = doi // LOT_SIZE["NIFTY"]
        spike = spike_label(lots)

        opt = {
            "alert": spike or "SPIKE",
            "type": "CE",
            "expiry": expiry,
            "strike": strike,
            "price": price,
            "oi": oi,
            "doi": doi,
            "oi_pct": f"{oi_pct}%",
            "lots": f"{lots} ({spike})",
            "iv": f"{iv}%",
            "ivroc": "+0.0%",
            "trend": option_trend(price, price, oi, oi)  # placeholder
        }


        # ------------------------
        # FUTURE OI (LIVE ANALYSIS)
        # ------------------------
        fut_json = safe_get("https://www.nseindia.com/api/live-analysis-oi/NIFTY")

        fut = {
            "price": "-",
            "dprice": "-",
            "oi": "-",
            "doi": "-",
            "oi_pct": "-",
            "lots": "-",
            "trend": "-",
            "trend2": "-"
        }

        if fut_json and fut_json.get("filtered", {}).get("data"):
            f = fut_json["filtered"]["data"][0]

            f_oi = f.get("openInterest", 0)
            f_doi = f.get("changeinOpenInterest", 0)
            f_price = f.get("lastPrice", 0)
            f_dprice = f.get("change", 0)

            lots_fut = f_doi // LOT_SIZE["NIFTY"]
            levels = spike_label(lots_fut)

            trend = "Neutral"
            extra = ""

            if f_doi > 0 and f_dprice < 0:
                trend = "Short Build-up"
                extra = "Future Sellers Active"

            fut = {
                "price": f_price,
                "dprice": f_dprice,
                "oi": f_oi,
                "doi": f_doi,
                "oi_pct": f.get("pchangeinOpenInterest", "-"),
                "lots": f"{lots_fut} ({levels})",
                "trend": trend,
                "trend2": extra
            }


        # ------------------------
        # BUILD MESSAGE
        # ------------------------
        msg = build_msg(opt, fut, spot, expiry)

        if msg != last_alert:
            wa_send(msg)
            last_alert = msg

        print("✓ LOOP OK")
        time.sleep(5)

    except Exception as e:
        print("MAIN ERROR:", e)
        time.sleep(3)
        continue
