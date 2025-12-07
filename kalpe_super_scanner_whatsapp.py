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
    if not (ULTRA_INSTANCE and ULTRA_TOKEN and WA_TO):
        print("WA-OFF:", message.replace("\n", " ")[:200])
        return
    try:
        url = f"https://api.ultramsg.com/{ULTRA_INSTANCE}/messages/chat"
        payload = {
            "token": ULTRA_TOKEN,
            "to": WA_TO,
            "body": message,
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

    IST = pytz.timezone("Asia/Kolkata")
    session = requests.Session()

    BASE_HEADERS = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "accept": "application/json, text/plain, */*",
        "referer": "https://www.nseindia.com/option-chain",
        "connection": "keep-alive",
    }
    session.headers.update(BASE_HEADERS)

    latest = None          # (data, spot, ts)
    latest_future = None   # dict or None
    blocked = False
    last_block = 0
    lock = threading.Lock()

    _cookie_ts = 0

    # -------------------------------
    # Helper utils
    # -------------------------------
    def now_ist():
        return datetime.now(IST)

    def is_market_open():
        t = now_ist()
        return t.weekday() < 5 and dtime(9, 15) <= t.time() <= dtime(15, 30)

    def fmt_num(n):
        try:
            return f"{int(n):,}"
        except Exception:
            return str(n)

    def fmt_price(n):
        try:
            return f"{n:,.2f}"
        except Exception:
            return str(n)

    def row3(a, b, c):
        return f"{a:<32} | {b:<32} | {c}"

    # -------------------------------
    # Spike level based on LOTS
    # -------------------------------
    def spike_level(lots: int):
        """
        Return (level_name, short_label, is_big_for_buy_side)
        - lots >= 200 → SUPER EXTREME
        - lots >= 150 → EXTREME
        - lots >= 100 → SUPER HIGH
        - lots >= 75  → HIGH
        - lots >= 50  → MEDIUM
        - lots >= 25  → SMALL
        """
        if lots >= 200:
            return "SUPER EXTREME", "Super Extreme", True
        if lots >= 150:
            return "EXTREME", "Extreme", True
        if lots >= 100:
            return "SUPER HIGH", "Super High", True
        if lots >= 75:
            return "HIGH", "High", False
        if lots >= 50:
            return "MEDIUM", "Medium", False
        if lots >= 25:
            return "SMALL", "Small", False
        return None, None, False

    # -------------------------------
    # Expiry labelling (Weekly / Monthly)
    # -------------------------------
    def expiry_type(expiry, all_exp):
        """
        Return label like (Weekly), (Next Weekly), (Monthly)
        Uses list of all expiries in option-chain.
        """
        if not all_exp:
            return ""

        unique_sorted = sorted(
            set(all_exp),
            key=lambda x: datetime.strptime(x, "%d-%b-%Y")
        )

        today = now_ist().date()

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
    # TREND CLASSIFIERS
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

    def option_action(trend_label):
        """Map option trend to BUY / WRITE / NEUTRAL"""
        if trend_label in ("Buyer Dominant", "Short Covering"):
            return "BUY"
        if trend_label in ("Writer Dominant", "Long Unwinding"):
            return "WRITE"
        return "NEUTRAL"

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

    def future_side(label):
        """Map future trend to BUY / SELL / NEUTRAL"""
        if label in ("Long Build-up", "Short Cover"):
            return "BUY"
        if label in ("Short Build-up", "Long Unwinding"):
            return "SELL"
        return "NEUTRAL"

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
                "price": f.get("lastPrice", 0.0),
                "prev_price": f.get("prevClose", 0.0),
                "oi": f.get("openInterest", 0),
                "prev_oi": f.get("openInterest", 0) - f.get("changeinOpenInterest", 0),
                "expiry": f.get("expiryDate", ""),
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
            "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY",
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
    # BUILD ALERT MESSAGE (3 columns)
    # -------------------------------
    def build_alert_message(
        level_name,
        opt_side,          # "CALL" / "PUT"
        opt_action_name,   # "BUY" / "WRITE"
        strike,
        typ,               # "CE"/"PE"
        expiry,
        expiry_label,
        spot,
        price,
        oi,
        doi,
        spike_pct,
        lots,
        iv,
        ivroc,
        opt_trend_label,
        fut_info,
    ):
        """
        fut_info: dict or None
        {
          "trend": str,
          "side": "BUY"/"SELL"/"NEUTRAL",
          "price": float,
          "dprice": float,
          "oi": int,
          "doi": int,
          "oi_pct": float,
          "lots": int,
          "level_name": str or None,
          "expiry": str
        }
        """

        # Header line color: green for buy, red for write
        if opt_action_name == "BUY":
            bullet = "🟢"
            header_side = f"{opt_side} BUYER DOMINANT"
        else:
            bullet = "🔴"
            header_side = f"{opt_side} WRITER ACTIVE"

        header_title = f"{bullet} {level_name} SPIKE — {header_side}"

        # Second line small header row
        col_header = row3("🟢 EXTREME SPIKE", "📊 OPTION TREND", "📉 FUTURE DATA")
        divider = "-" * 98

        # Option block
        line_exp = row3(
            f"Expiry: {expiry} {expiry_label}",
            f"OI: {fmt_num(oi)}",
            f"Future Price: {fmt_price(fut_info['price'])}" if fut_info else "Future Price: -",
        )

        line_strike = row3(
            f"NIFTY {strike} {typ}",
            f"ΔOI: {fmt_num(doi)}",
            (
                f"Fut OI: {fmt_num(fut_info['oi'])} "
                f"(ΔOI {fmt_num(fut_info['doi'])})"
            )
            if fut_info
            else "Fut OI: -",
        )

        line_price = row3(
            f"Price: ₹{fmt_price(price)}",
            f"OI %: {spike_pct:+.1f}%",
            (
                f"OI %: {fut_info['oi_pct']:+.1f}%"
                if fut_info
                else "OI %: -"
            ),
        )

        line_lots = row3(
            f"Lots: {lots} ({level_name})",
            f"IV: {iv:.2f}%",
            (
                f"OI Lots: {fut_info['lots']} → "
                f"{fut_info['level_name'] or 'N/A'}"
                if fut_info
                else "OI Lots: -"
            ),
        )

        # Text description rows
        opt_desc = ""
        if opt_trend_label == "Buyer Dominant":
            opt_desc = "(Price ↑, OI ↑ → Call/Put buying pressure)"
        elif opt_trend_label == "Writer Dominant":
            opt_desc = "(Price ↓, OI ↑ → Writers active)"
        elif opt_trend_label == "Short Covering":
            opt_desc = "(Price ↑, OI ↓ → Short covering)"
        elif opt_trend_label == "Long Unwinding":
            opt_desc = "(Price ↓, OI ↓ → Long unwinding)"

        fut_trend_line = ""
        fut_side_line = ""
        if fut_info:
            fut_trend_label = fut_info["trend"]
            if fut_trend_label == "Long Build-up":
                fut_trend_line = "Trend: Long Build-up"
                fut_side_line = "(OI ↑, Price ↑ → Future Buyers Active)"
            elif fut_trend_label == "Short Build-up":
                fut_trend_line = "Trend: Short Build-up"
                fut_side_line = "(OI ↑, Price ↓ → Future Sellers Active)"
            elif fut_trend_label == "Short Cover":
                fut_trend_line = "Trend: Short Cover"
                fut_side_line = "(OI ↓, Price ↑ → Shorts covering)"
            elif fut_trend_label == "Long Unwinding":
                fut_trend_line = "Trend: Long Unwinding"
                fut_side_line = "(OI ↓, Price ↓ → Longs exiting)"
            else:
                fut_trend_line = f"Trend: {fut_trend_label}"
                fut_side_line = ""

        line_trend = row3(
            f"{opt_trend_label}",
            opt_desc,
            fut_trend_line,
        )

        line_trend2 = row3(
            "",
            "",
            fut_side_line,
        )

        # Spot + future expiry + time
        fut_exp = fut_info["expiry"] if fut_info else "-"
        footer1 = (
            f"Spot: {fmt_price(spot)}  •  Fut Expiry: {fut_exp}"
        )
        footer2 = f"Time: {now_ist().strftime('%H:%M:%S')} IST"

        msg_lines = [
            header_title,
            "",
            col_header,
            divider,
            line_exp,
            line_strike,
            line_price,
            line_lots,
            line_trend,
            line_trend2,
            "",
            footer1,
            footer2,
        ]
        return "\n".join(msg_lines)

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

            expiries_all = [row["expiryDate"] for row in data]

            for row in data:
                strike = row["strikePrice"]
                expiry = row["expiryDate"]

                if abs(strike - spot) > ATM_RANGE:
                    continue

                expiry_label = expiry_type(expiry, expiries_all)

                for typ in ("CE", "PE"):
                    opt = row.get(typ)
                    if not opt:
                        continue

                    key = f"{strike}_{typ}_{expiry}"

                    oi = opt["openInterest"]
                    chg = opt["changeinOpenInterest"]
                    lots = abs(chg) // NIFTY_LOT

                    iv = opt.get("impliedVolatility") or 0.0
                    price = opt.get("lastPrice") or 0.0

                    if key not in hist:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    old = hist[key]
                    old_oi, old_iv, old_price = (
                        old["oi"],
                        old["iv"],
                        old["price"],
                    )

                    # % spike vs last snapshot
                    spike_pct = ((oi - old_oi) / old_oi * 100) if old_oi else 0.0
                    ivroc = ((iv - old_iv) / old_iv * 100) if old_iv else 0.0

                    now_t = time.time()
                    if now_t - cooldown.get(key, 0) < COOLDOWN_SEC:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    # Only positive OI change is meaningful
                    if chg <= 0:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    level_name, level_short, is_big_for_buy = spike_level(lots)
                    if not level_name:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    opt_trend_label = option_trend(price, old_price, oi, old_oi)
                    opt_act = option_action(opt_trend_label)

                    # BUY vs WRITE logic
                    opt_side = "CALL" if typ == "CE" else "PUT"

                    # For BUY side options we only want big spikes (>= SUPER HIGH)
                    if opt_act == "BUY" and not is_big_for_buy:
                        hist[key] = {"oi": oi, "iv": iv, "price": price}
                        continue

                    # FUTURE INFO
                    fut_info = None
                    if latest_future:
                        f = latest_future
                        fut_tr = future_trend(f)
                        fut_side_val = future_side(fut_tr)

                        fut_price = f["price"]
                        fut_prev_price = f["prev_price"]
                        fut_dprice = fut_price - fut_prev_price

                        fut_oi = f["oi"]
                        fut_prev_oi = f["prev_oi"]
                        fut_doi = fut_oi - fut_prev_oi

                        fut_oi_pct = ((fut_oi - fut_prev_oi) / fut_prev_oi * 100) if fut_prev_oi else 0.0
                        fut_lots = abs(fut_doi) // NIFTY_LOT
                        fut_level_name, _, _ = spike_level(fut_lots)

                        fut_info = {
                            "trend": fut_tr,
                            "side": fut_side_val,
                            "price": fut_price,
                            "dprice": fut_dprice,
                            "oi": fut_oi,
                            "doi": fut_doi,
                            "oi_pct": fut_oi_pct,
                            "lots": fut_lots,
                            "level_name": fut_level_name,
                            "expiry": f["expiry"],
                        }

                    msg = build_alert_message(
                        level_name=level_name,
                        opt_side=opt_side,
                        opt_action_name=opt_act,
                        strike=strike,
                        typ=typ,
                        expiry=expiry,
                        expiry_label=expiry_label,
                        spot=spot,
                        price=price,
                        oi=oi,
                        doi=chg,
                        spike_pct=spike_pct,
                        lots=lots,
                        iv=iv,
                        ivroc=ivroc,
                        opt_trend_label=opt_trend_label,
                        fut_info=fut_info,
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
                # Outside market hours only heartbeat & NSE session refresh
                fetch_futures()
                time.sleep(60)

    # -------------------------------
    # HEARTBEAT
    # -------------------------------
    def heartbeat():
        while True:
            print("ALIVE:", now_ist())
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
