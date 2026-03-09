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

LOT_SIZES = {"BANKNIFTY": 30}
DEFAULT_LOT_SIZE = 30

SYMBOLS_TO_MONITOR = [
"BANKNIFTY30MAR2655600CE.NFO","BANKNIFTY30MAR2655600PE.NFO",
"BANKNIFTY30MAR2655500CE.NFO","BANKNIFTY30MAR2655500PE.NFO",
"BANKNIFTY30MAR2655400CE.NFO","BANKNIFTY30MAR2655400PE.NFO",
"BANKNIFTY30MAR2655300CE.NFO","BANKNIFTY30MAR2655300PE.NFO",
"BANKNIFTY30MAR2655200CE.NFO","BANKNIFTY30MAR2655200PE.NFO",
"BANKNIFTY30MAR2655100CE.NFO","BANKNIFTY30MAR2655100PE.NFO",
"BANKNIFTY30MAR2655000CE.NFO","BANKNIFTY30MAR2655000PE.NFO",
"BANKNIFTY30MAR2654900CE.NFO","BANKNIFTY30MAR2654900PE.NFO",
"BANKNIFTY30MAR2654800CE.NFO","BANKNIFTY30MAR2654800PE.NFO",
"BANKNIFTY30MAR2654700CE.NFO","BANKNIFTY30MAR2654700PE.NFO",
"BANKNIFTY30MAR2654600CE.NFO","BANKNIFTY30MAR2654600PE.NFO",
"BANKNIFTY30MAR2654500CE.NFO","BANKNIFTY30MAR2654500PE.NFO",
"BANKNIFTY30MAR2654400CE.NFO","BANKNIFTY30MAR2654400PE.NFO",
"BANKNIFTY30MAR2654300CE.NFO","BANKNIFTY30MAR2654300PE.NFO",
"BANKNIFTY30MAR2654200CE.NFO","BANKNIFTY30MAR2654200PE.NFO",
"BANKNIFTY30MAR2654100CE.NFO","BANKNIFTY30MAR2654100PE.NFO",
"BANKNIFTY30MAR2654000CE.NFO","BANKNIFTY30MAR2654000PE.NFO",
"BANKNIFTY30MAR2653900CE.NFO","BANKNIFTY30MAR2653900PE.NFO",
"BANKNIFTY30MAR2653800CE.NFO","BANKNIFTY30MAR2653800PE.NFO",
"BANKNIFTY30MAR2653700CE.NFO","BANKNIFTY30MAR2653700PE.NFO",
"BANKNIFTY30MAR2653600CE.NFO","BANKNIFTY30MAR2653600PE.NFO",
"BANKNIFTY30MAR2653500CE.NFO","BANKNIFTY30MAR2653500PE.NFO",
"BANKNIFTY30MAR2653400CE.NFO","BANKNIFTY30MAR2653400PE.NFO",
"BANKNIFTY30MAR2653300CE.NFO","BANKNIFTY30MAR2653300PE.NFO",
"BANKNIFTY30MAR2653200CE.NFO","BANKNIFTY30MAR2653200PE.NFO",
"BANKNIFTY30MAR2653100CE.NFO","BANKNIFTY30MAR2653100PE.NFO",
"BANKNIFTY30MAR2657900CE.NFO","BANKNIFTY30MAR2657900PE.NFO",
"BANKNIFTY-I"
]

# ============================== STATE =================================
symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}
active_watches = {}
future_prices = {"BANKNIFTY":0}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))

# ============================== LOGIC =================================

def clean_symbol(symbol):
    return symbol.replace(".NFO","")

def get_strength_label(lots):
    if lots >= 400: return "🚀 BLAST 🚀"
    elif lots >= 300: return "🌟 AWESOME"
    elif lots >= 200: return "✅ VERY GOOD"
    else: return "⚡ GOOD"

def classify_action(symbol, oi_chg, price_chg):

    if symbol.endswith("-I"):
        if oi_chg > 0:
            return "FUTURE BUY 📈" if price_chg >= 0 else "FUTURE SELL 📉"
        else:
            return "SHORT COVERING ↗️" if price_chg >= 0 else "LONG UNWINDING ↘️"

    is_call = "CE" in symbol

    if oi_chg > 0:
        if price_chg >= 0:
            return "CALL BUY 🔵" if is_call else "PUT BUY 🔴"
        else:
            return "CALL WRITER ✍️" if is_call else "PUT WRITER ✍️"
    else:
        if price_chg >= 0:
            return "SHORT COVERING ⤴️"
        else:
            return "LONG UNWINDING ⤵️"

async def process_data(data):

    symbol = data.get("InstrumentIdentifier")

    if symbol not in symbol_data_state:
        return

    print(f"Tick Received: {symbol}", flush=True)

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")

    if new_price is None or new_oi is None:
        return

    base_symbol = "BANKNIFTY"

    if symbol.endswith("-I"):
        future_prices[base_symbol] = new_price
        print(f"Future price updated: {new_price}", flush=True)

    state = symbol_data_state[symbol]

    if state["oi"] == 0:
        state["oi"], state["price"] = new_oi, new_price
        return

    oi_tick_diff = new_oi - state["oi"]

    print(f"{symbol} | OI Change: {oi_tick_diff}", flush=True)

    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)

    tick_lots = int(abs(oi_tick_diff) / lot_size)

    print(f"{symbol} | Tick Lots: {tick_lots}", flush=True)

    if tick_lots >= 100 and symbol not in active_watches:

        print(f"Watch Started: {symbol}", flush=True)

        active_watches[symbol] = {
            "start_oi": state["oi"],
            "start_price": state["price"],
            "end_time": datetime.now() + timedelta(minutes=2)
        }

    state["oi"], state["price"] = new_oi, new_price

    if symbol in active_watches:

        watch = active_watches[symbol]

        if datetime.now() >= watch["end_time"]:

            final_oi_change = new_oi - watch["start_oi"]

            final_lots = int(abs(final_oi_change) / lot_size)

            print(f"Watch Ended: {symbol} | Final Lots: {final_lots}", flush=True)

            if final_lots >= 100:

                print(f"ALERT TRIGGERED {symbol}", flush=True)

                strength = get_strength_label(final_lots)

                price_change = new_price - watch["start_price"]

                action = classify_action(symbol, final_oi_change, price_change)

                msg = (
f"{strength}\n"
f"🚨 {action}\n"
f"Symbol: {clean_symbol(symbol)}\n"
f"━━━━━━━━━━━━━━━\n"
f"LOTS: {final_lots}\n"
f"PRICE: {new_price:.2f}\n"
f"FUTURE PRICE: {future_prices.get(base_symbol,0):.2f}\n"
f"━━━━━━━━━━━━━━━\n"
f"EXISTING OI: {watch['start_oi']:,}\n"
f"OI CHANGE  : {final_oi_change:+,}\n"
f"NEW OI     : {new_oi:,}\n"
f"TIME: {now()}"
)

                await send_alert(msg)

            del active_watches[symbol]

# ============================== WEBSOCKET =================================

async def run_scanner():

    while True:

        try:

            print("Connecting to WebSocket...", flush=True)

            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:

                print("Authenticating...", flush=True)

                await websocket.send(json.dumps({
                    "MessageType": "Authenticate",
                    "Password": API_KEY
                }))

                auth_resp = await websocket.recv()

                print("Authentication response:", auth_resp, flush=True)

                for sym in SYMBOLS_TO_MONITOR:

                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime",
                        "Exchange": "NFO",
                        "InstrumentIdentifier": sym
                    }))

                    print(f"Subscribed: {sym}", flush=True)

                await send_alert("✅ Scanner Started | Debug Mode")

                print("Scanner Live and Receiving Data...", flush=True)

                async for message in websocket:

                    msg_data = json.loads(message)

                    if msg_data.get("MessageType") == "RealtimeResult":
                        await process_data(msg_data)

        except Exception as e:

            print("Connection Error:", e, flush=True)

            await asyncio.sleep(5)

# ============================== START =================================

if __name__ == "__main__":
    asyncio.run(run_scanner())
