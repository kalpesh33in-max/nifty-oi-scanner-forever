import os, time, json, random, requests, pytz
from datetime import datetime

# ------------------------------
# WHATSAPP SENDER
# ------------------------------
WA_URL = os.getenv("WA_URL")
WA_ID = os.getenv("WA_ID")

def wa_send(msg):
    try:
        r = requests.post(WA_URL, json={"id": WA_ID, "message": msg}, timeout=10)
        print("WA SENT:", r.text)
    except Exception as e:
        print("WA ERROR:", e)


# ------------------------------
# SAFE REQUEST (Prevents NSE block)
# ------------------------------
def safe_get(url, headers=None):
    for attempt in range(4):
        try:
            time.sleep(random.uniform(0.6, 1.4))  # random delay → NSE anti-block
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            print(f"[NSE] {url} failed {r.status_code}")
        except Exception as e:
            print(f"[SAFE_GET ERROR] {e}")
        time.sleep(1.2)
    return None


# ------------------------------
# LOT SIZE TABLE
# ------------------------------
LOT_SIZE = {
    "NIFTY": 50,
    "BANKNIFTY": 15
}


# ------------------------------
# OI SPIKE LABELS
# ------------------------------
def spike_label(lots):
    if lots >= 200: return "SUPER EXTREME"
    if lots >= 150: return "EXTREME"
    if lots >= 100: return "SUPER HIGH"
    if lots >= 75:  return "HIGH"
    if lots >= 50:  return "MEDIUM"
    if lots >= 25:  return "SMALL"
    return ""


# ------------------------------
# FORMAT MESSAGE BLOCK
# ------------------------------
def create_message(option, future, spot, fut_expiry):
    return f"""
🟢 EXTREME SPIKE — CE BUYER DOMINANT
____________________________________________

🟦 OPTION DATA                     | 🟥 FUTURE DATA
----------------------------------|----------------------------------
Expiry: {option['expiry']}        | Future Price: {future['price']}
Strike: {option['strike']}        | Fut ΔPrice: {future['dprice']}
Price: ₹{option['price']}         | Fut OI: {future['oi']}
OI: {option['oi']}                | ΔOI: {future['doi']}
ΔOI: {option['doi']}              | Fut OI %: {future['oi_pct']}
OI %: {option['oi_pct']}          | Fut Lots: {future['lots']}
Lots: {option['lots']}            | Trend: {future['trend']}
IV: {option['iv']}                | {future['trend2']}
IV ROC: {option['ivroc']}         |
Trend: {option['trend']}          |

Spot: {spot} • Fut Expiry: {fut_expiry}
Time: {datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S IST")}
""".strip()


# ------------------------------
# START SCANNER
# ------------------------------
last_alert = ""

while True:
    try:
        now = datetime.now(pytz.timezone("Asia/Kolkata"))

        # -------- GET SPOT PRICE ----------
        spot_url = "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY"
        spot_json = safe_get(spot_url)

        if not spot_json:
            print("SPOT FAIL")
            time.sleep(3)
            continue

        spot = spot_json.get("marketData", [{}])[0].get("indexLastValue", "-")
        

        # -------- OPTION CHAIN ----------
        oc_url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        oc_json = safe_get(oc_url)

        if not oc_json:
            print("OPTION FAIL")
            time.sleep(3)
            continue

        # Pick ANY CE with high OI spike for demo
        data = oc_json["records"]["data"]
        if not data:
            time.sleep(2)
            continue

        row = data[0]  # simplified sample

        ce = row.get("CE", {})
        expiry = oc_json["records"]["expiryDates"][0]

        option = {
            "expiry": expiry,
            "strike": ce.get("strikePrice", "-"),
            "price": ce.get("lastPrice", "-"),
            "oi": ce.get("openInterest", "-"),
            "doi": ce.get("changeinOpenInterest", "-"),
            "oi_pct": ce.get("pchangeinOpenInterest", "-"),
            "lots": spike_label( ce.get("changeinOpenInterest",0) // LOT_SIZE["NIFTY"] ),
            "iv": ce.get("impliedVolatility", "-"),
            "ivroc": "-",
            "trend": "Buyer Dominant"
        }

        # -------- FUTURE OI (SAFE MODE) ----------
        fut_url = "https://www.nseindia.com/api/live-analysis-oi/NIFTY"
        fj = safe_get(fut_url)

        future = {
            "price": "-",
            "dprice": "-",
            "oi": "-",
            "doi": "-",
            "oi_pct": "-",
            "lots": "-",
            "trend": "-",
            "trend2": "-"
        }

        if fj and "filtered" in fj and "data" in fj["filtered"] and fj["filtered"]["data"]:
            f = fj["filtered"]["data"][0]

            future["price"] = f.get("lastPrice", "-")
            future["dprice"] = f.get("change", "-")
            future["oi"] = f.get("openInterest", "-")
            future["doi"] = f.get("changeinOpenInterest", "-")
            future["oi_pct"] = f.get("pchangeinOpenInterest", "-")

            lots = f.get("changeinOpenInterest", 0) // LOT_SIZE["NIFTY"]
            future["lots"] = spike_label(lots)

            if lots > 0 and f.get("change", 0) < 0:
                future["trend"] = "Short Build-up"
                future["trend2"] = "Future Sellers Active"

        # -------- FORMAT OUTPUT ----------
        message = create_message(option, future, spot, "30-Dec-2025")

        # -------- SEND ALERT ONLY IF NEW --------
        if message != last_alert:
            wa_send(message)
            last_alert = message

        print("OK")

        time.sleep(4)  # Safe interval for NSE

    except Exception as e:
        print("MAIN LOOP ERROR:", e)
        time.sleep(3)
        continue
