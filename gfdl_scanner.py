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
    "SBIN": 750, "ICICIBANK": 700, "AXISBANK": 625, "KOTAKBANK": 400
}
DEFAULT_LOT_SIZE = 75

SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2658900CE", "BANKNIFTY24FEB2658900PE", "BANKNIFTY24FEB2658800CE", "BANKNIFTY24FEB2658800PE",
    "BANKNIFTY24FEB2658700CE", "BANKNIFTY24FEB2658700PE", "BANKNIFTY24FEB2658600CE", "BANKNIFTY24FEB2658600PE",
    "BANKNIFTY24FEB2658500CE", "BANKNIFTY24FEB2658500PE",
    "HDFCBANK24FEB26930CE", "HDFCBANK24FEB26930PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "SBIN24FEB261040CE", "SBIN24FEB261040PE", "ICICIBANK24FEB261350CE", "ICICIBANK24FEB261350PE",
    "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I", "SBIN-I", "AXISBANK-I", "KOTAKBANK-I"
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
