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

LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 75, "HDFCBANK": 550, "ICICIBANK": 700, "AXISBANK": 625, "SBIN": 750}
DEFAULT_LOT_SIZE = 75

SYMBOLS_TO_MONITOR = [
"BANKNIFTY4611155800PE","BANKNIFTY4611155800CE",
"BANKNIFTY4611155900PE","BANKNIFTY4611155700CE",
"BANKNIFTY4611155900CE","BANKNIFTY4611155700PE",
"BANKNIFTY4611156000PE","BANKNIFTY4611155600CE",
"BANKNIFTY4611156000CE","BANKNIFTY4611155600PE",
"BANKNIFTY4611156100PE","BANKNIFTY4611155500CE",
"BANKNIFTY4611156100CE","BANKNIFTY4611155500PE",
"BANKNIFTY4611156200PE","BANKNIFTY4611155400CE",
"BANKNIFTY4611156200CE","BANKNIFTY4611155400PE",
"BANKNIFTY4611156300PE","BANKNIFTY4611155300CE",
"BANKNIFTY4611156300CE","BANKNIFTY4611155300PE",
"BANKNIFTY4611156400PE","BANKNIFTY4611155200CE",
"BANKNIFTY4611156400CE","BANKNIFTY4611155200PE",
"BANKNIFTY4611156500PE","BANKNIFTY4611155100CE",
"BANKNIFTY4611156500CE","BANKNIFTY4611155100PE",
"BANKNIFTY4611156600PE","BANKNIFTY4611155000CE",
"BANKNIFTY4611156600CE","BANKNIFTY4611155000PE",
"BANKNIFTY4611156700PE","BANKNIFTY4611154900CE",
"BANKNIFTY4611156700CE","BANKNIFTY4611154900PE",
"BANKNIFTY4611156800PE","BANKNIFTY4611154800CE",
"BANKNIFTY4611156800CE","BANKNIFTY4611154800PE",
"BANKNIFTY4611156900PE","BANKNIFTY4611154700CE",
"BANKNIFTY4611156900CE","BANKNIFTY4611154700PE",
"BANKNIFTY4611157000PE","BANKNIFTY4611154600CE",
"BANKNIFTY4611157000CE","BANKNIFTY4611154600PE",
"BANKNIFTY4611157100PE","BANKNIFTY4611154500CE",
"BANKNIFTY4611157100CE","BANKNIFTY4611154500PE",
"BANKNIFTY4611157200PE","BANKNIFTY4611154400CE",
"BANKNIFTY4611157200CE","BANKNIFTY4611154400PE",
"BANKNIFTY4611157300PE","BANKNIFTY4611154300CE",
"BANKNIFTY4611157300CE","BANKNIFTY4611154300PE",
"BANKNIFTY4611157400PE","BANKNIFTY4611154200CE",
"BANKNIFTY4611157400CE","BANKNIFTY4611154200PE",
"BANKNIFTY4611157500PE","BANKNIFTY4611154100CE",
"BANKNIFTY4611157500CE","BANKNIFTY4611154100PE",
"BANKNIFTY4611157600PE","BANKNIFTY4611154000CE",
"BANKNIFTY4611157600CE","BANKNIFTY4611154000PE",
"BANKNIFTY4611157700PE","BANKNIFTY4611153900CE",
"BANKNIFTY4611157700CE","BANKNIFTY4611153900PE",
"BANKNIFTY4611157800PE","BANKNIFTY4611153800CE",
"BANKNIFTY4611157800CE","BANKNIFTY4611153800PE",
"BANKNIFTY4611157900PE","BANKNIFTY4611153700CE",
"BANKNIFTY4611157900CE","BANKNIFTY4611153700PE",
"BANKNIFTY4611158000PE","BANKNIFTY4611153600CE",
"BANKNIFTY4611158000CE","BANKNIFTY4611153600PE",
"BANKNIFTY4611158100PE","BANKNIFTY4611153500CE",
"BANKNIFTY4611158100CE","BANKNIFTY4611153500PE",
"BANKNIFTY-I"
]

symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}
active_watches = {}
future_prices = {"BANKNIFTY": 0}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))

async def process_data(data):

    symbol = data.get("InstrumentIdentifier")
    if symbol not in symbol_data_state:
        return

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")

    if new_price is None or new_oi is None:
        return

    state = symbol_data_state[symbol]

    if state["oi"] == 0:
        state["oi"], state["price"] = new_oi, new_price
        return

    oi_tick_diff = new_oi - state["oi"]

    lot_size = 30
    tick_lots = int(abs(oi_tick_diff) / lot_size)

    # SHORT LOG ONLY WHEN OI CHANGES
    if oi_tick_diff != 0:
        print(f"{symbol} OI Change: {oi_tick_diff} Lots: {tick_lots}", flush=True)

    state["oi"], state["price"] = new_oi, new_price


async def run_scanner():

    while True:
        try:

            async with websockets.connect(WSS_URL) as websocket:

                await websocket.send(json.dumps({
                    "MessageType": "Authenticate",
                    "Password": API_KEY
                }))

                auth_resp = await websocket.recv()

                if not json.loads(auth_resp).get("Complete"):
                    await asyncio.sleep(10)
                    continue

                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime",
                        "Exchange": "NFO",
                        "InstrumentIdentifier": sym
                    }))

                await send_alert("Scanner Started")

                async for message in websocket:
                    msg_data = json.loads(message)

                    if msg_data.get("MessageType") == "RealtimeResult":
                        await process_data(msg_data)

        except Exception as e:
            print("Connection Error:", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_scanner())
