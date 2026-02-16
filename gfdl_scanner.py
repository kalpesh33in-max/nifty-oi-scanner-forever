import asyncio
import websockets
import json
import os
import re
import requests
import functools
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 75, "HDFCBANK": 550, "ICICIBANK": 700}
DEFAULT_LOT_SIZE = 75

SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2660200CE", "BANKNIFTY24FEB2660200PE", "BANKNIFTY24FEB2660100CE", "BANKNIFTY24FEB2660100PE",
    "BANKNIFTY24FEB2660000CE", "BANKNIFTY24FEB2660000PE", "BANKNIFTY24FEB2659900CE", "BANKNIFTY24FEB2659900PE",
    "BANKNIFTY24FEB2659800CE", "BANKNIFTY24FEB2659800PE", "BANKNIFTY24FEB2659700CE", "BANKNIFTY24FEB2659700PE",
    "BANKNIFTY24FEB2659600CE", "BANKNIFTY24FEB2659600PE", "BANKNIFTY24FEB2659500CE", "BANKNIFTY24FEB2659500PE",
    "BANKNIFTY24FEB2659400CE", "BANKNIFTY24FEB2659400PE", "BANKNIFTY24FEB2659300CE", "BANKNIFTY24FEB2659300PE",
    "BANKNIFTY24FEB2659200CE", "BANKNIFTY24FEB2659200PE", "BANKNIFTY24FEB2659100CE", "BANKNIFTY24FEB2659100PE",
    "BANKNIFTY24FEB2659000CE", "BANKNIFTY24FEB2659000PE", "BANKNIFTY24FEB2660300CE", "BANKNIFTY24FEB2660300PE",
    "BANKNIFTY24FEB2660400CE", "BANKNIFTY24FEB2660400PE", "BANKNIFTY24FEB2660500CE", "BANKNIFTY24FEB2660500PE",
    "BANKNIFTY24FEB2660600CE", "BANKNIFTY24FEB2660600PE", "BANKNIFTY24FEB2660700CE", "BANKNIFTY24FEB2660700PE",
    "BANKNIFTY24FEB2660800CE", "BANKNIFTY24FEB2660800PE", "BANKNIFTY24FEB2660900CE", "BANKNIFTY24FEB2660900PE",
    "BANKNIFTY24FEB2661000CE", "BANKNIFTY24FEB2661000PE", "BANKNIFTY24FEB2661100CE", "BANKNIFTY24FEB2661100PE",
    "BANKNIFTY24FEB2661200CE", "BANKNIFTY24FEB2661200PE", "BANKNIFTY24FEB2661300CE", "BANKNIFTY24FEB2661300PE",
    "BANKNIFTY24FEB2661400CE", "BANKNIFTY24FEB2661400PE", "HDFCBANK24FEB26900CE", "HDFCBANK24FEB26900PE",
    "HDFCBANK24FEB26895CE", "HDFCBANK24FEB26895PE", "HDFCBANK24FEB26890CE", "HDFCBANK24FEB26890PE",
    "HDFCBANK24FEB26885CE", "HDFCBANK24FEB26885PE", "HDFCBANK24FEB26880CE", "HDFCBANK24FEB26880PE",
    "HDFCBANK24FEB26875CE", "HDFCBANK24FEB26875PE", "HDFCBANK24FEB26905CE", "HDFCBANK24FEB26905PE",
    "HDFCBANK24FEB26910CE", "HDFCBANK24FEB26910PE", "HDFCBANK24FEB26915CE", "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26920CE", "HDFCBANK24FEB26920PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "ICICIBANK24FEB261410CE", "ICICIBANK24FEB261410PE", "ICICIBANK24FEB261400CE", "ICICIBANK24FEB261400PE",
    "ICICIBANK24FEB261390CE", "ICICIBANK24FEB261390PE", "ICICIBANK24FEB261380CE", "ICICIBANK24FEB261380PE",
    "ICICIBANK24FEB261370CE", "ICICIBANK24FEB261370PE", "ICICIBANK24FEB261360CE", "ICICIBANK24FEB261360PE",
    "ICICIBANK24FEB261420CE", "ICICIBANK24FEB261420PE", "ICICIBANK24FEB261430CE", "ICICIBANK24FEB261430PE",
    "ICICIBANK24FEB261440CE", "ICICIBANK24FEB261440PE", "ICICIBANK24FEB261450CE", "ICICIBANK24FEB261450PE",
    "ICICIBANK24FEB261460CE", "ICICIBANK24FEB261460PE", "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I"
]

# ============================== STATE & UTILITIES =============================
symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}
active_watches = {} 
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
    else: return "✅ VERY GOOD" # Label for 200-299 lots

def classify_action(symbol, oi_chg, price_chg):
    if symbol.endswith("-I"):
        if oi_chg > 0: return "FUTURE BUY (LONG) 📈" if price_chg >= 0 else "FUTURE SELL (SHORT) 📉"
        else: return "SHORT COVERING ↗️" if price_chg >= 0 else "LONG UNWINDING ↘️"
    is_call = symbol.endswith("CE")
    if oi_chg > 0:
        if price_chg >= 0: return "CALL BUY 🔵" if is_call else "PUT BUY 🔴"
        else: return "CALL WRITER ✍️" if is_call else "PUT WRITER ✍️"
    else:
        if price_chg >= 0: return "SHORT COVERING (CE) ⤴️" if is_call else "SHORT COVERING (PE) ⤴️"
        else: return "LONG UNWINDING (CE) ⤵️" if is_call else "LONG UNWINDING (PE) ⤵️"

async def process_data(data):
    global symbol_data_state, active_watches, future_prices
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return
    
    new_price, new_oi = data.get("LastTradePrice"), data.get("OpenInterest")
    if new_price is None or new_oi is None: return

    base_match = re.match(r'^([A-Z]+)', symbol)
    if base_match:
        base_symbol = base_match.group(1)
        if symbol.endswith("-I"): future_prices[base_symbol] = new_price

    state = symbol_data_state[symbol]
    if state["oi"] == 0:
        state["oi"], state["price"] = new_oi, new_price
        return

    # UPDATED TRIGGER: Starts watch if tick is >= 200 lots
    oi_tick_diff = new_oi - state["oi"]
    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    tick_lots = int(abs(oi_tick_diff) / lot_size)

    if tick_lots >= 200 and symbol not in active_watches:
        active_watches[symbol] = {
            "start_oi": state["oi"], "start_price": state["price"],
            "end_time": datetime.now() + timedelta(minutes=2)
        }

    state["oi"], state["price"] = new_oi, new_price

    if symbol in active_watches:
        watch = active_watches[symbol]
        if datetime.now() >= watch["end_time"]:
            final_oi_change = new_oi - watch["start_oi"]
            final_lots = int(abs(final_oi_change) / lot_size)
            
            # UPDATED CONFIRMATION: Alert if net change is >= 200 lots
            if final_lots >= 200:
                strength, price_change = get_strength_label(final_lots), new_price - watch["start_price"]
                action = classify_action(symbol, final_oi_change, price_change)
                
                msg = (f"{strength}\n🚨 {action}\nSymbol: {symbol}\n━━━━━━━━━━━━━━━\nLOTS: {final_lots}\n"
                       f"PRICE: {new_price:.2f} ({'▲' if price_change >= 0 else '▼'})\nFUTURE PRICE: {future_prices.get(base_symbol, 0):.2f}\n"
                       f"━━━━━━━━━━━━━━━\nEXISTING OI: {watch['start_oi']:,}\nOI CHANGE  : {final_oi_change:+,d}\nNEW OI     : {new_oi:,}\nTIME: {now()}")
                await send_alert(msg)
            del active_watches[symbol]

async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = await websocket.recv()
                if not json.loads(auth_resp).get("Complete"): 
                    await asyncio.sleep(10); continue
                
                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": sym}))
                
                await send_alert(f"✅ Scanner Started | Monitoring {len(SYMBOLS_TO_MONITOR)} Symbols | 200-Lot Trigger.")
                print(f"✅ Scanner Live | High-Volume 200-Lot Trigger Active", flush=True)
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult": await process_data(msg_data)
        except Exception as e: 
            print(f"Connection Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__": asyncio.run(run_scanner())
