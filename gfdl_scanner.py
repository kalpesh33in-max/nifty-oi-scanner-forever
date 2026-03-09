import asyncio
import websockets
import json
import os
import re
import requests
import functools
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
 
# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
 
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = fhttps://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage
 
LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 75, "HDFCBANK": 550, "ICICIBANK": 700, "AXISBANK": 625, "SBIN": 750}
DEFAULT_LOT_SIZE = 75
 
SYMBOLS_TO_MONITOR = [
"BANKNIFTY4611155800PE","BANKNIFTY4611155800CE","BANKNIFTY4611155800CE","BANKNIFTY4611155800PE",
"BANKNIFTY4611155900PE","BANKNIFTY4611155700CE","BANKNIFTY4611155900CE","BANKNIFTY4611155700PE",
"BANKNIFTY4611156000PE","BANKNIFTY4611155600CE","BANKNIFTY4611156000CE","BANKNIFTY4611155600PE",
"BANKNIFTY4611156100PE","BANKNIFTY4611155500CE","BANKNIFTY4611156100CE","BANKNIFTY4611155500PE",
"BANKNIFTY4611156200PE","BANKNIFTY4611155400CE","BANKNIFTY4611156200CE","BANKNIFTY4611155400PE",
"BANKNIFTY4611156300PE","BANKNIFTY4611155300CE","BANKNIFTY4611156300CE","BANKNIFTY4611155300PE",
"BANKNIFTY4611156400PE","BANKNIFTY4611155200CE","BANKNIFTY4611156400CE","BANKNIFTY4611155200PE",
"BANKNIFTY4611156500PE","BANKNIFTY4611155100CE","BANKNIFTY4611156500CE","BANKNIFTY4611155100PE",
"BANKNIFTY4611156600PE","BANKNIFTY4611155000CE","BANKNIFTY4611156600CE","BANKNIFTY4611155000PE",
"BANKNIFTY4611156700PE","BANKNIFTY4611154900CE","BANKNIFTY4611156700CE","BANKNIFTY4611154900PE",
"BANKNIFTY4611156800PE","BANKNIFTY4611154800CE","BANKNIFTY4611156800CE","BANKNIFTY4611154800PE",
"BANKNIFTY4611156900PE","BANKNIFTY4611154700CE","BANKNIFTY4611156900CE","BANKNIFTY4611154700PE",
"BANKNIFTY4611157000PE","BANKNIFTY4611154600CE","BANKNIFTY4611157000CE","BANKNIFTY4611154600PE",
"BANKNIFTY4611157100PE","BANKNIFTY4611154500CE","BANKNIFTY4611157100CE","BANKNIFTY4611154500PE",
"BANKNIFTY4611157200PE","BANKNIFTY4611154400CE","BANKNIFTY4611157200CE","BANKNIFTY4611154400PE",
"BANKNIFTY4611157300PE","BANKNIFTY4611154300CE","BANKNIFTY4611157300CE","BANKNIFTY4611154300PE",
"BANKNIFTY4611157400PE","BANKNIFTY4611154200CE","BANKNIFTY4611157400CE","BANKNIFTY4611154200PE",
"BANKNIFTY4611157500PE","BANKNIFTY4611154100CE","BANKNIFTY4611157500CE","BANKNIFTY4611154100PE",
"BANKNIFTY4611157600PE","BANKNIFTY4611154000CE","BANKNIFTY4611157600CE","BANKNIFTY4611154000PE",
"BANKNIFTY4611157700PE","BANKNIFTY4611153900CE","BANKNIFTY4611157700CE","BANKNIFTY4611153900PE",
"BANKNIFTY4611157800PE","BANKNIFTY4611153800CE","BANKNIFTY4611157800CE","BANKNIFTY4611153800PE",
"BANKNIFTY4611157900PE","BANKNIFTY4611153700CE","BANKNIFTY4611157900CE","BANKNIFTY4611153700PE",
"BANKNIFTY4611158000PE","BANKNIFTY4611153600CE","BANKNIFTY4611158000CE","BANKNIFTY4611153600PE",
"BANKNIFTY4611158100PE","BANKNIFTY4611153500CE","BANKNIFTY4611158100CE","BANKNIFTY4611153500PE",
"BANKNIFTY-I"
]
 
# ============================== STATE & UTILITIES =============================
symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}
active_watches = {}
future_prices = {k: 0 for k in LOT_SIZES.keys()}
 
def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")
 
async def send_alert(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Error: {e}")
 
# =============================== CORE LOGIC ===================================
def get_strength_label(lots):
    if lots >= 400: return "🚀 BLAST 🚀"
    elif lots >= 300: return "🌟 AWESOME"
    elif lots >= 200: return "✅ VERY GOOD"
    else: return "⚡ GOOD"
 
def classify_action(symbol, oi_chg, price_chg):
    if symbol.endswith("-I"):
        if oi_chg > 0: return "FUTURE BUY (LONG) 📈" if price_chg >= 0 else "FUTURE SELL (SHORT) 📉"
        else: return "SHORT COVERING ↗️" if price_chg >= 0 else "LONG UNWINDING ↘️"
    is_call = symbol.endswith("CE")
    if oi_chg > 0:
        if price_chg >= 0: return "CALL BUY 🔵" if is_call else "PUT BUY 🔴"
        else: return "CALL WRITER ✍️" if is_call else "PUT WRITER ✍️"
    else:
        if price_chg >= 0: return "SHORT COVERING (CE) ⤴️" if is_call else "SHORT COVERING (PE) ⤴️"
        else: return "LONG UNWINDING (CE) ⤵️" if is_call else "LONG UNWINDING (PE) ⤵️"
 
async def process_data(data):
    global symbol_data_state, active_watches, future_prices
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return
 
    new_price, new_oi = data.get("LastTradePrice"), data.get("OpenInterest")
    if new_price is None or new_oi is None: return
 
    base_match = re.match(r'^([A-Z]+)', symbol)
    if base_match:
        base_symbol = base_match.group(1)
        if symbol.endswith("-I"): future_prices[base_symbol] = new_price
 
    state = symbol_data_state[symbol]
    if state["oi"] == 0:
        state["oi"], state["price"] = new_oi, new_price
        return
 
    oi_tick_diff = new_oi - state["oi"]
    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    tick_lots = int(abs(oi_tick_diff) / lot_size)
 
    if tick_lots >= 100 and symbol not in active_watches:
        active_watches[symbol] = {
            "start_oi": state["oi"], "start_price": state["price"],
            "end_time": datetime.now() + timedelta(minutes=2)
        }
 
    state["oi"], state["price"] = new_oi, new_price
 
    if symbol in active_watches:
        watch = active_watches[symbol]
        if datetime.now() >= watch["end_time"]:
            final_oi_change = new_oi - watch["start_oi"]
            final_lots = int(abs(final_oi_change) / lot_size)
 
            if final_lots >= 100:
                strength, price_change = get_strength_label(final_lots), new_price - watch["start_price"]
                action = classify_action(symbol, final_oi_change, price_change)
 
                msg = (f"{strength}\n🚨 {action}\nSymbol: {symbol}\n━━━━━━━━━━━━━━━\nLOTS: {final_lots}\n"
                       f"PRICE: {new_price:.2f} ({'▲' if price_change >= 0 else '▼'})\nFUTURE PRICE: {future_prices.get(base_symbol, 0):.2f}\n"
                       f"━━━━━━━━━━━━━━━\nEXISTING OI: {watch['start_oi']:,}\nOI CHANGE  : {final_oi_change:+,d}\nNEW OI     : {new_oi:,}\nTIME: {now()}")
                await send_alert(msg)
            del active_watches[symbol]
 
async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = await websocket.recv()
                if not json.loads(auth_resp).get("Complete"):
                    await asyncio.sleep(10)
                    continue
 
                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": sym}))
 
                await send_alert(f"✅ Scanner Started | Monitoring {len(SYMBOLS_TO_MONITOR)} Symbols | 100-Lot Trigger.")
                print(f"✅ Scanner Live | High-Volume 100-Lot Trigger Active", flush=True)
 
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult":
                        await process_data(msg_data)
 
        except Exception as e:
            print(f"Connection Error: {e}")
            await asyncio.sleep(5)
 
if __name__ == "__main__":
    asyncio.run(run_scanner())
 
