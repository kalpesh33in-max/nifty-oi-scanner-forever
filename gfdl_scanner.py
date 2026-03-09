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

# Updated BankNifty Lot Size to 30
LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 25, "FINNIFTY": 25, "HDFCBANK": 550, "ICICIBANK": 700, "AXISBANK": 625, "SBIN": 750}
DEFAULT_LOT_SIZE = 30

# Symbol List: 53400 to 58000 (CE and PE) for 30MAR26 Expiry
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY30MAR2653400CE", "BANKNIFTY30MAR2653400PE",
    "BANKNIFTY30MAR2653500CE", "BANKNIFTY30MAR2653500PE",
    "BANKNIFTY30MAR2653600CE", "BANKNIFTY30MAR2653600PE",
    "BANKNIFTY30MAR2653700CE", "BANKNIFTY30MAR2653700PE",
    "BANKNIFTY30MAR2653800CE", "BANKNIFTY30MAR2653800PE",
    "BANKNIFTY30MAR2653900CE", "BANKNIFTY30MAR2653900PE",
    "BANKNIFTY30MAR2654000CE", "BANKNIFTY30MAR2654000PE",
    "BANKNIFTY30MAR2654100CE", "BANKNIFTY30MAR2654100PE",
    "BANKNIFTY30MAR2654200CE", "BANKNIFTY30MAR2654200PE",
    "BANKNIFTY30MAR2654300CE", "BANKNIFTY30MAR2654300PE",
    "BANKNIFTY30MAR2654400CE", "BANKNIFTY30MAR2654400PE",
    "BANKNIFTY30MAR2654500CE", "BANKNIFTY30MAR2654500PE",
    "BANKNIFTY30MAR2654600CE", "BANKNIFTY30MAR2654600PE",
    "BANKNIFTY30MAR2654700CE", "BANKNIFTY30MAR2654700PE",
    "BANKNIFTY30MAR2654800CE", "BANKNIFTY30MAR2654800PE",
    "BANKNIFTY30MAR2654900CE", "BANKNIFTY30MAR2654900PE",
    "BANKNIFTY30MAR2655000CE", "BANKNIFTY30MAR2655000PE",
    "BANKNIFTY30MAR2655100CE", "BANKNIFTY30MAR2655100PE",
    "BANKNIFTY30MAR2655200CE", "BANKNIFTY30MAR2655200PE",
    "BANKNIFTY30MAR2655300CE", "BANKNIFTY30MAR2655300PE",
    "BANKNIFTY30MAR2655400CE", "BANKNIFTY30MAR2655400PE",
    "BANKNIFTY30MAR2655500CE", "BANKNIFTY30MAR2655500PE",
    "BANKNIFTY30MAR2655600CE", "BANKNIFTY30MAR2655600PE",
    "BANKNIFTY30MAR2655700CE", "BANKNIFTY30MAR2655700PE",
    "BANKNIFTY30MAR2655800CE", "BANKNIFTY30MAR2655800PE",
    "BANKNIFTY30MAR2655900CE", "BANKNIFTY30MAR2655900PE",
    "BANKNIFTY30MAR2656000CE", "BANKNIFTY30MAR2656000PE",
    "BANKNIFTY30MAR2656100CE", "BANKNIFTY30MAR2656100PE",
    "BANKNIFTY30MAR2656200CE", "BANKNIFTY30MAR2656200PE",
    "BANKNIFTY30MAR2656300CE", "BANKNIFTY30MAR2656300PE",
    "BANKNIFTY30MAR2656400CE", "BANKNIFTY30MAR2656400PE",
    "BANKNIFTY30MAR2656500CE", "BANKNIFTY30MAR2656500PE",
    "BANKNIFTY30MAR2656600CE", "BANKNIFTY30MAR2656600PE",
    "BANKNIFTY30MAR2656700CE", "BANKNIFTY30MAR2656700PE",
    "BANKNIFTY30MAR2656800CE", "BANKNIFTY30MAR2656800PE",
    "BANKNIFTY30MAR2656900CE", "BANKNIFTY30MAR2656900PE",
    "BANKNIFTY30MAR2657000CE", "BANKNIFTY30MAR2657000PE",
    "BANKNIFTY30MAR2657100CE", "BANKNIFTY30MAR2657100PE",
    "BANKNIFTY30MAR2657200CE", "BANKNIFTY30MAR2657200PE",
    "BANKNIFTY30MAR2657300CE", "BANKNIFTY30MAR2657300PE",
    "BANKNIFTY30MAR2657400CE", "BANKNIFTY30MAR2657400PE",
    "BANKNIFTY30MAR2657500CE", "BANKNIFTY30MAR2657500PE",
    "BANKNIFTY30MAR2657600CE", "BANKNIFTY30MAR2657600PE",
    "BANKNIFTY30MAR2657700CE", "BANKNIFTY30MAR2657700PE",
    "BANKNIFTY30MAR2657800CE", "BANKNIFTY30MAR2657800PE",
    "BANKNIFTY30MAR2657900CE", "BANKNIFTY30MAR2657900PE",
    "BANKNIFTY30MAR2658000CE", "BANKNIFTY30MAR2658000PE",
    "BANKNIFTY-I"
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
    else: return "⚡ GOOD" 

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

    # Trigger logic: Starts watch if tick is >= 100 lots
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
            
            # Confirmation: Alert if net change is >= 100 lots after 2 mins
            if final_lots >= 100:
                strength, price_change = get_strength_label(final_lots), new_price - watch["start_price"]
                action = classify_action(symbol, final_oi_change, price_change)
                
                # Maintaining your exact output format
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
