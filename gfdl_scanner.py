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
    "BANKNIFTY30MAR2660700CE", "BANKNIFTY30MAR2660700PE", "BANKNIFTY30MAR2660600CE", "BANKNIFTY30MAR2660600PE",
    "BANKNIFTY30MAR2660500CE", "BANKNIFTY30MAR2660500PE", "BANKNIFTY30MAR2660400CE", "BANKNIFTY30MAR2660400PE",
    "BANKNIFTY30MAR2660300CE", "BANKNIFTY30MAR2660300PE", "BANKNIFTY30MAR2660200CE", "BANKNIFTY30MAR2660200PE",
    "BANKNIFTY30MAR2660100CE", "BANKNIFTY30MAR2660100PE", "BANKNIFTY30MAR2660000CE", "BANKNIFTY30MAR2660000PE",
    "BANKNIFTY30MAR2659900CE", "BANKNIFTY30MAR2659900PE", "BANKNIFTY30MAR2659800CE", "BANKNIFTY30MAR2659800PE",
    "BANKNIFTY30MAR2659700CE", "BANKNIFTY30MAR2659700PE", "BANKNIFTY30MAR2659600CE", "BANKNIFTY30MAR2659600PE",
    "BANKNIFTY30MAR2659500CE", "BANKNIFTY30MAR2659500PE", "BANKNIFTY30MAR2660800CE", "BANKNIFTY30MAR2660800PE",
    "BANKNIFTY30MAR2660900CE", "BANKNIFTY30MAR2660900PE", "BANKNIFTY30MAR2661000CE", "BANKNIFTY30MAR2661000PE",
    "BANKNIFTY30MAR2661100CE", "BANKNIFTY30MAR2661100PE", "BANKNIFTY30MAR2661200CE", "BANKNIFTY30MAR2661200PE",
    "BANKNIFTY30MAR2661300CE", "BANKNIFTY30MAR2661300PE", "BANKNIFTY30MAR2661400CE", "BANKNIFTY30MAR2661400PE",
    "BANKNIFTY30MAR2661500CE", "BANKNIFTY30MAR2661500PE", "BANKNIFTY30MAR2661600CE", "BANKNIFTY30MAR2661600PE",
    "BANKNIFTY30MAR2661700CE", "BANKNIFTY30MAR2661700PE", "BANKNIFTY30MAR2661800CE", "BANKNIFTY30MAR2661800PE",
    "BANKNIFTY30MAR2661900CE", "BANKNIFTY30MAR2661900PE", "ICICIBANK30MAR261390CE", "ICICIBANK30MAR261390PE",
    "ICICIBANK30MAR261380CE", "ICICIBANK30MAR261380PE", "ICICIBANK30MAR261370CE", "ICICIBANK30MAR261370PE",
    "ICICIBANK30MAR261360CE", "ICICIBANK30MAR261360PE", "ICICIBANK30MAR261350CE", "ICICIBANK30MAR261350PE",
    "ICICIBANK30MAR261340CE", "ICICIBANK30MAR261340PE", "ICICIBANK30MAR261400CE", "ICICIBANK30MAR261400PE",
    "ICICIBANK30MAR261410CE", "ICICIBANK30MAR261410PE", "ICICIBANK30MAR261420CE", "ICICIBANK30MAR261420PE",
    "ICICIBANK30MAR261430CE", "ICICIBANK30MAR261430PE", "ICICIBANK30MAR261440CE", "ICICIBANK30MAR261440PE",
    "HDFCBANK30MAR26915CE", "HDFCBANK30MAR26915PE", "HDFCBANK30MAR26905CE", "HDFCBANK30MAR26905PE",
    "HDFCBANK30MAR26895CE", "HDFCBANK30MAR26895PE", "HDFCBANK30MAR26885CE", "HDFCBANK30MAR26885PE",
    "HDFCBANK30MAR26875CE", "HDFCBANK30MAR26875PE", "HDFCBANK30MAR26865CE", "HDFCBANK30MAR26865PE",
    "HDFCBANK30MAR26925CE", "HDFCBANK30MAR26925PE", "HDFCBANK30MAR26935CE", "HDFCBANK30MAR26935PE",
    "HDFCBANK30MAR26945CE", "HDFCBANK30MAR26945PE", "HDFCBANK30MAR26955CE", "HDFCBANK30MAR26955PE",
    "HDFCBANK30MAR26965CE", "HDFCBANK30MAR26965PE", "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I"
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
    elif lots >= 200: return "✅ VERY GOOD"
    else: return "⚡ GOOD" # Label for 100-199 lots

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

    # UPDATED TRIGGER: Starts watch if tick is >= 100 lots
    oi_tick_diff = new_oi - state["oi"]
    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    tick_lots = int(abs(oi_tick_diff) / lot_size)

    if tick_lots >= 100 and symbol not in active_watches:
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
            
            # UPDATED CONFIRMATION: Alert if net change is >= 100 lots
            if final_lots >= 100:
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
                
                await send_alert(f"✅ Scanner Started | Monitoring {len(SYMBOLS_TO_MONITOR)} Symbols | 100-Lot Trigger.")
                print(f"✅ Scanner Live | High-Volume 100-Lot Trigger Active", flush=True)
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult": await process_data(msg_data)
        except Exception as e: 
            print(f"Connection Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__": asyncio.run(run_scanner())
