import asyncio
import websockets
import json
import os
import sys
import re
import requests
import functools
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

LOT_SIZES = {
    "BANKNIFTY": 30, "NIFTY": 75, "HDFCBANK": 550, 
    "SBIN": 750, "ICICIBANK": 700, "AXISBANK": 625
}
DEFAULT_LOT_SIZE = 75

SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2658700CE", "BANKNIFTY24FEB2658700PE", "BANKNIFTY24FEB2658600CE", "BANKNIFTY24FEB2658600PE",
    "BANKNIFTY24FEB2658500CE", "BANKNIFTY24FEB2658500PE", "BANKNIFTY24FEB2658400CE", "BANKNIFTY24FEB2658400PE",
    "BANKNIFTY24FEB2658300CE", "BANKNIFTY24FEB2658300PE", "BANKNIFTY24FEB2658200CE", "BANKNIFTY24FEB2658200PE",
    "BANKNIFTY24FEB2658800CE", "BANKNIFTY24FEB2658800PE", "BANKNIFTY24FEB2658900CE", "BANKNIFTY24FEB2658900PE",
    "BANKNIFTY24FEB2659000CE", "BANKNIFTY24FEB2659000PE", "BANKNIFTY24FEB2659100CE", "BANKNIFTY24FEB2659100PE",
    "BANKNIFTY24FEB2659200CE", "BANKNIFTY24FEB2659200PE",
    "ICICIBANK24FEB261350CE", "ICICIBANK24FEB261350PE", "ICICIBANK24FEB261340CE", "ICICIBANK24FEB261340PE",
    "ICICIBANK24FEB261330CE", "ICICIBANK24FEB261330PE", "ICICIBANK24FEB261320CE", "ICICIBANK24FEB261320PE",
    "ICICIBANK24FEB261310CE", "ICICIBANK24FEB261310PE", "ICICIBANK24FEB261300CE", "ICICIBANK24FEB261300PE",
    "ICICIBANK24FEB261360CE", "ICICIBANK24FEB261360PE", "ICICIBANK24FEB261370CE", "ICICIBANK24FEB261370PE",
    "ICICIBANK24FEB261380CE", "ICICIBANK24FEB261380PE", "ICICIBANK24FEB261390CE", "ICICIBANK24FEB261390PE",
    "ICICIBANK24FEB261400CE", "ICICIBANK24FEB261400PE",
    "SBIN24FEB261030CE", "SBIN24FEB261030PE", "SBIN24FEB261025CE", "SBIN24FEB261025PE",
    "SBIN24FEB261020CE", "SBIN24FEB261020PE", "SBIN24FEB261015CE", "SBIN24FEB261015PE",
    "SBIN24FEB261010CE", "SBIN24FEB261010PE", "SBIN24FEB261005CE", "SBIN24FEB261005PE",
    "SBIN24FEB261035CE", "SBIN24FEB261035PE", "SBIN24FEB261040CE", "SBIN24FEB261040PE",
    "SBIN24FEB261045CE", "SBIN24FEB261045PE", "SBIN24FEB261050CE", "SBIN24FEB261050PE",
    "SBIN24FEB261055CE", "SBIN24FEB261055PE",
    "HDFCBANK24FEB26930CE", "HDFCBANK24FEB26930PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "HDFCBANK24FEB26920CE", "HDFCBANK24FEB26920PE", "HDFCBANK24FEB26915CE", "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26910CE", "HDFCBANK24FEB26910PE", "HDFCBANK24FEB26905CE", "HDFCBANK24FEB26905PE",
    "HDFCBANK24FEB26935CE", "HDFCBANK24FEB26935PE", "HDFCBANK24FEB26940CE", "HDFCBANK24FEB26940PE",
    "HDFCBANK24FEB26945CE", "HDFCBANK24FEB26945PE", "HDFCBANK24FEB26950CE", "HDFCBANK24FEB26950PE",
    "HDFCBANK24FEB26955CE", "HDFCBANK24FEB26955PE",
    "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I", "SBIN-I", "AXISBANK-I"
]

# ============================== STATE & UTILITIES =============================
symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}
future_prices = {k: 0 for k in LOT_SIZES.keys()}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Error: {e}")

# =============================== CORE LOGIC ===================================
def get_strength_label(lots):
    if lots >= 400: return "🚀 BLAST 🚀"
    elif lots >= 300: return "🌟 AWESOME"
    elif lots >= 200: return "✅ VERY GOOD"
    elif lots >= 100: return "👍 GOOD"
    else: return "🆗 OK"

def classify_action(symbol, oi_chg, price_chg):
    if symbol.endswith("-I"):
        if oi_chg > 0:
            return "FUTURE BUY (LONG) 📈" if price_chg >= 0 else "FUTURE SELL (SHORT) 📉"
        else:
            return "SHORT COVERING ↗️" if price_chg >= 0 else "LONG UNWINDING ↘️"
    
    is_call = symbol.endswith("CE")
    if oi_chg > 0:
        if price_chg >= 0: return "CALL BUY 🔵" if is_call else "PUT BUY 🔴"
        else: return "CALL WRITER ✍️" if is_call else "PUT WRITER ✍️"
    else:
        if price_chg >= 0: return "SHORT COVERING (CE) ⤴️" if is_call else "SHORT COVERING (PE) ⤴️"
        else: return "LONG UNWINDING (CE) ⤵️" if is_call else "LONG UNWINDING (PE) ⤵️"

async def process_data(data):
    global symbol_data_state, future_prices
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return

    new_price, new_oi = data.get("LastTradePrice"), data.get("OpenInterest")
    if new_price is None or new_oi is None: return

    state = symbol_data_state[symbol]
    prev_oi, prev_price = state["oi"], state["price"]
    state["oi"], state["price"] = new_oi, new_price

    base_match = re.match(r'^([A-Z]+)', symbol)
    if not base_match: return
    base_symbol = base_match.group(1)

    if symbol.endswith("-I"):
        future_prices[base_symbol] = new_price
    
    if prev_oi == 0: return 

    oi_change = new_oi - prev_oi
    if oi_change == 0: return

    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    lots_affected = int(abs(oi_change) / lot_size)

    # TRIGGER: 50 lots minimum
    if lots_affected >= 50:
        strength = get_strength_label(lots_affected)
        price_change = new_price - prev_price
        action = classify_action(symbol, oi_change, price_change)
        f_price = future_prices.get(base_symbol, 0)
        
        msg = (
            f"{strength}\n"
            f"🚨 {action}\n"
            f"Symbol: {symbol}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"LOTS: {lots_affected}\n"
            f"PRICE: {new_price:.2f} ({'▲' if price_change >= 0 else '▼'})\n"
            f"FUTURE PRICE: {f_price:.2f}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"EXISTING OI: {prev_oi:,}\n"
            f"OI CHANGE  : {oi_change:+,d}\n"
            f"NEW OI     : {new_oi:,}\n"
            f"TIME: {now()}"
        )
        print(f"🚀 {strength} Alert Triggered: {symbol}")
        await send_alert(msg)

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                if not json.loads(await websocket.recv()).get("Complete"): 
                    await asyncio.sleep(10)
                    continue
                
                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": sym}))
                
                print(f"✅ Scanner Live | Trigger: 50+ Lots", flush=True)
                await send_alert("✅ Scanner Started | Monitoring for OK to BLAST signals with Existing OI.")
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult":
                        await process_data(msg_data)
        except Exception as e:
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_scanner())
gfdl_scanner.py
Displaying gfdl_scanner.py.
