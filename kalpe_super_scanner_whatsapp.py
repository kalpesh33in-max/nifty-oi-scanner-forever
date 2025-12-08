# ------------------------------------------------------------
#   NIFTY OPTION + FUTURE OI SCANNER (24/7 SAFE VERSION)
#   WhatsApp 2-COLUMN FORMAT (FINAL | NO MORE CHANGES)
# ------------------------------------------------------------

import os, time, json, requests, pytz, traceback
from datetime import datetime
from math import ceil

# ------------------------------------------------------------
#  TELEGRAM / WHATSAPP API CONFIG
# ------------------------------------------------------------
WA_API = os.getenv("WA_API")          # WhatsApp Cloud API URL
WA_TOKEN = os.getenv("WA_TOKEN")      # WhatsApp Bearer Token
CHAT_ID = os.getenv("CHAT_ID")        # Your WhatsApp ID

HEADERS = {
    "Authorization": f"Bearer {WA_TOKEN}",
    "Content-Type": "application/json"
}

# ------------------------------------------------------------
#  SAFE SEND FUNCTION (No Block)
# ------------------------------------------------------------
def wa_send(msg):
    try:
        data = {"messaging_product": "whatsapp", "to": CHAT_ID, "text": {"body": msg}}
        r = requests.post(WA_API, headers=HEADERS, json=data, timeout=8)
        print("WA SENT:", r.text)
        return True
    except Exception as e:
        print("WA ERROR:", e)
        return False

# ------------------------------------------------------------
#  SAFE NSE FETCH — ULTRA-LOW FREQUENCY + AUTO WAIT
# ------------------------------------------------------------
def get_json(url):
    """Fetch NSE JSON safely without triggering 403."""
    while True:
        try:
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8
            )
            if r.status_code == 200:
                return r.json()

            if r.status_code == 403:
                print("[NSE] 403 BLOCK — WAITING 40 sec")
                time.sleep(40)
                continue

            print("[NSE] ERROR:", r.status_code)
            time.sleep(12)
        except:
            print("[NSE] Exception — Retry in 15 sec")
            time.sleep(15)

# ------------------------------------------------------------
#  FORMAT NUMBER (XX,XXX)
# ------------------------------------------------------------
def fmt(n):
    try:
        return f"{int(n):,}"
    except:
        return str(n)

# ------------------------------------------------------------
#  TWO COLUMN MESSAGE FORMAT (FINAL APPROVED FORMAT)
# ------------------------------------------------------------
def build_message(sig, op, fut, spot, fut_exp, tstamp):
    msg = f"""🟢 {sig}

────────────────────────────────────────────

📘 OPTION DATA                     | 📉 FUTURE DATA
────────────────────────────────────|───────────────────────────────────
Expiry: {op['expiry']}             | Future Price: {fut['price']}
Strike: {op['strike']}             | Fut ΔPrice: {fut['dprice']}
Price: ₹{op['price']}              | Fut OI: {fmt(fut['oi'])}
OI: {fmt(op['oi'])}                | ΔOI: {fmt(fut['doi'])}
ΔOI: {fmt(op['doi'])}              | Fut OI %: {fut['oip']}%
OI %: {op['oip']}%                 | Fut Lots: {fut['lots']}
Lots: {op['lots']}                 | Trend: {fut['trend']}
IV: {op['iv']}%                    | {fut['subtrend']}
IV ROC: {op['ivroc']}%             |
Trend: {op['trend']}               |

Spot: {spot}   •   Fut Expiry: {fut_exp}
Time: {tstamp} IST
"""
    return msg


# ------------------------------------------------------------
#  ALERT CONDITIONS
# ------------------------------------------------------------

BUY_ALERT_LEVELS = ["super spike", "extreme spike"]

WRITER_LEVELS = ["high", "super high", "extreme", "super extreme"]

# FUTURE BUY/SELL also same levels
FUTURE_LEVELS = WRITER_LEVELS

# ------------------------------------------------------------
#  MAIN LOOP
# ------------------------------------------------------------
def run():
    print("SAFE NIFTY SCANNER STARTED…")

    last_alert = {}

    while True:
        try:
            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            tstamp = now.strftime("%H:%M:%S")

            # Sleep outside market hours
            if now.hour < 9 or (now.hour == 15 and now.minute > 31) or now.hour > 15:
                wa_send("🔴 Market Closed — Scanner Sleeping")
                time.sleep(900)
                continue

            # 1 — FETCH SPOT
            spot_json = get_json("https://www.nseindia.com/api/market-status")
            spot = spot_json["marketState"][0]["last"]

            # 2 — FETCH OPTION CHAIN
            oc = get_json("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY")

            # 3 — FIND BEST SIGNAL
            alerts = extract_signals(oc)

            # 4 — FUTURE DATA
            fut = get_json("https://www.nseindia.com/api/live-analysis-oi/NIFTY")
            fut_data = parse_future(fut)

            # 5 — SEND ALERTS
            for sig in alerts:
                key = sig["key"]
                if key in last_alert:
                    continue

                msg = build_message(
                    sig["title"],
                    sig["op"],
                    fut_data,
                    spot,
                    fut_data["expiry"],
                    tstamp
                )

                wa_send(msg)
                last_alert[key] = True

            time.sleep(7)   # Ultra-safe delay

        except Exception as e:
            print("MAIN ERROR:", e)
            print(traceback.format_exc())
            time.sleep(15)


# ------------------------------------------------------------
#  PARSE OPTION SIGNAL
# ------------------------------------------------------------
def extract_signals(oc):
    out = []

    for row in oc["records"]["data"]:
        ce = row.get("CE")
        pe = row.get("PE")

        # Skip empty rows
        if not ce:
            continue

        # Compute CE/PE stats
        lot = ce["changeinOpenInterest"] // 50
        doi = ce["changeinOpenInterest"]
        oip = round((doi / ce["openInterest"]) * 100, 1) if ce["openInterest"] > 0 else 0

        # Determine level
        level = level_from_lots(abs(lot))

        # ONLY buyer: super spike, extreme spike
        if lot > 0 and level in BUY_ALERT_LEVELS:
            out.append({
                "key": f"BUY-{row['strikePrice']}",
                "title": f"🟢 {level.upper()} — CE BUYER DOMINANT",
                "op": {
                    "expiry": oc["records"]["expiryDates"][0],
                    "strike": f"NIFTY {row['strikePrice']} CE",
                    "price": ce["lastPrice"],
                    "oi": ce["openInterest"],
                    "doi": doi,
                    "oip": oip,
                    "lots": f"{abs(lot)} ({level.upper()})",
                    "iv": ce["impliedVolatility"],
                    "ivroc": 0,
                    "trend": "Buyer Dominant"
                }
            })

        # Writers: high → super extreme only
        if lot < 0 and level in WRITER_LEVELS:
            out.append({
                "key": f"WRITE-{row['strikePrice']}",
                "title": f"🔴 {level.upper()} SPIKE — CALL WRITER ACTIVE",
                "op": {
                    "expiry": oc["records"]["expiryDates"][0],
                    "strike": f"NIFTY {row['strikePrice']} CE",
                    "price": ce["lastPrice"],
                    "oi": ce["openInterest"],
                    "doi": doi,
                    "oip": oip,
                    "lots": f"{abs(lot)} ({level.upper()})",
                    "iv": ce["impliedVolatility"],
                    "ivroc": 0,
                    "trend": "Writer Dominant"
                }
            })

    return out


def level_from_lots(l):
    if l >= 200: return "super extreme"
    if l >= 150: return "extreme"
    if l >= 100: return "super high"
    if l >= 75: return "high"
    if l >= 50: return "medium"
    if l >= 25: return "small"
    return "normal"


# ------------------------------------------------------------
#  FUTURE DATA PARSER
# ------------------------------------------------------------
def parse_future(fj):
    oi = fj["filtered"]["data"][0]["oi"]
    doi = fj["filtered"]["data"][0]["changeinOpenInterest"]

    level = level_from_lots(abs(doi // 50))

    trend = "Long Build-up" if doi > 0 else "Short Build-up"
    sub = "Future Buyers Active" if doi > 0 else "Future Sellers Active"

    return {
        "price": fj["filtered"]["data"][0]["lastPrice"],
        "dprice": fj["filtered"]["data"][0]["change"],
        "oi": oi,
        "doi": doi,
        "oip": round((doi / oi) * 100, 1) if oi else 0,
        "lots": f"{abs(doi // 50)} ({level.upper()})",
        "trend": trend,
        "subtrend": sub,
        "expiry": fj["expiryDates"][0]
    }


# ------------------------------------------------------------
#  START PROGRAM
# ------------------------------------------------------------
if __name__ == "__main__":
    run()
