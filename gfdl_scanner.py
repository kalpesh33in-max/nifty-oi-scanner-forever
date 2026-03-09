import asyncio
import websockets
import json
import os
import re
import requests
import functools
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ================= CONFIG =================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

LOT_SIZES = {"BANKNIFTY": 30}
DEFAULT_LOT_SIZE = 30

# ================= SYMBOL LIST =================
SYMBOLS_TO_MONITOR = [
"BANKNIFTY30MAR2655600CE","BANKNIFTY30MAR2655600PE",
"BANKNIFTY30MAR2655500CE","BANKNIFTY30MAR2655500PE",
"BANKNIFTY30MAR2655400CE","BANKNIFTY30MAR2655400PE",
"BANKNIFTY30MAR2655300CE","BANKNIFTY30MAR2655300PE",
"BANKNIFTY30MAR2655200CE","BANKNIFTY30MAR2655200PE",
"BANKNIFTY30MAR2655100CE","BANKNIFTY30MAR2655100PE",
"BANKNIFTY30MAR2655000CE","BANKNIFTY30MAR2655000PE",
"BANKNIFTY30MAR2654900CE","BANKNIFTY30MAR2654900PE",
"BANKNIFTY30MAR2654800CE","BANKNIFTY30MAR2654800PE",
"BANKNIFTY30MAR2654700CE","BANKNIFTY30MAR2654700PE",
"BANKNIFTY30MAR2654600CE","BANKNIFTY30MAR2654600PE",
"BANKNIFTY30MAR2654500CE","BANKNIFTY30MAR2654500PE",
"BANKNIFTY30MAR2654400CE","BANKNIFTY30MAR2654400PE",
"BANKNIFTY30MAR2654300CE","BANKNIFTY30MAR2654300PE",
"BANKNIFTY30MAR2654200CE","BANKNIFTY30MAR2654200PE",
"BANKNIFTY30MAR2654100CE","BANKNIFTY30MAR2654100PE",
"BANKNIFTY30MAR2654000CE","BANKNIFTY30MAR2654000PE",
"BANKNIFTY30MAR2653900CE","BANKNIFTY30MAR2653900PE",
"BANKNIFTY30MAR2653800CE","BANKNIFTY30MAR2653800PE",
"BANKNIFTY30MAR2653700CE","BANKNIFTY30MAR2653700PE",
"BANKNIFTY30MAR2653600CE","BANKNIFTY30MAR2653600PE",
"BANKNIFTY30MAR2653500CE","BANKNIFTY30MAR2653500PE",
"BANKNIFTY30MAR2653400CE","BANKNIFTY30MAR2653400PE",
"BANKNIFTY30MAR2653300CE","BANKNIFTY30MAR2653300PE",
"BANKNIFTY30MAR2653200CE","BANKNIFTY30MAR2653200PE",
"BANKNIFTY30MAR2653100CE","BANKNIFTY30MAR2653100PE",
"BANKNIFTY30MAR2655700CE","BANKNIFTY30MAR2655700PE",
"BANKNIFTY30MAR2655800CE","BANKNIFTY30MAR2655800PE",
"BANKNIFTY30MAR2655900CE","BANKNIFTY30MAR2655900PE",
"BANKNIFTY30MAR2656000CE","BANKNIFTY30MAR2656000PE",
"BANKNIFTY30MAR2656100CE","BANKNIFTY30MAR2656100PE",
"BANKNIFTY30MAR2656200CE","BANKNIFTY30MAR2656200PE",
"BANKNIFTY30MAR2656300CE","BANKNIFTY30MAR2656300PE",
"BANKNIFTY30MAR2656400CE","BANKNIFTY30MAR2656400PE",
"BANKNIFTY30MAR2656500CE","BANKNIFTY30MAR2656500PE",
"BANKNIFTY30MAR2656600CE","BANKNIFTY30MAR2656600PE",
"BANKNIFTY30MAR2656700CE","BANKNIFTY30MAR2656700PE",
"BANKNIFTY30MAR2656800CE","BANKNIFTY30MAR2656800PE",
"BANKNIFTY30MAR2656900CE","BANKNIFTY30MAR2656900PE",
"BANKNIFTY30MAR2657000CE","BANKNIFTY30MAR2657000PE",
"BANKNIFTY30MAR2657100CE","BANKNIFTY30MAR2657100PE",
"BANKNIFTY30MAR2657200CE","BANKNIFTY30MAR2657200PE",
"BANKNIFTY30MAR2657300CE","BANKNIFTY30MAR2657300PE",
"BANKNIFTY30MAR2657400CE","BANKNIFTY30MAR2657400PE",
"BANKNIFTY30MAR2657500CE","BANKNIFTY30MAR2657500PE",
"BANKNIFTY30MAR2657600CE","BANKNIFTY30MAR2657600PE",
"BANKNIFTY30MAR2657700CE","BANKNIFTY30MAR2657700PE",
"BANKNIFTY30MAR2657800CE","BANKNIFTY30MAR2657800PE",
"BANKNIFTY30MAR2657900CE","BANKNIFTY30MAR2657900PE",
"BANKNIFTY-I"
]

# ================= STATE =================
symbol_data_state = {s: {"price":0,"oi":0} for s in SYMBOLS_TO_MONITOR}
future_prices = {"BANKNIFTY":0}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg):
    loop = asyncio.get_running_loop()
    params={'chat_id':TELEGRAM_CHAT_ID,'text':msg}
    await loop.run_in_executor(None,functools.partial(requests.post,TELEGRAM_API_URL,params=params,timeout=10))

# ================= DATA PROCESS =================
async def process_data(data):

    symbol=data.get("InstrumentIdentifier")

    if symbol not in symbol_data_state:
        return

    print("Tick:",symbol,flush=True)

    price=data.get("LastTradePrice")
    oi=data.get("OpenInterest")

    if price is None or oi is None:
        return

    state=symbol_data_state[symbol]

    if state["oi"]==0:
        state["oi"]=oi
        state["price"]=price
        return

    oi_change=oi-state["oi"]

    lot_size=LOT_SIZES["BANKNIFTY"]
    lots=int(abs(oi_change)/lot_size)

    print(symbol,"OI Change:",oi_change,"Lots:",lots,flush=True)

    state["oi"]=oi
    state["price"]=price

# ================= WEBSOCKET =================
async def run_scanner():

    while True:
        try:

            print("Connecting WebSocket...",flush=True)

            async with websockets.connect(WSS_URL) as ws:

                await ws.send(json.dumps({
                    "MessageType":"Authenticate",
                    "Password":API_KEY
                }))

                print("Authenticated",flush=True)

                for sym in SYMBOLS_TO_MONITOR:

                    await ws.send(json.dumps({
                        "MessageType":"SubscribeRealtime",
                        "Exchange":"NFO",
                        "InstrumentIdentifier":sym
                    }))

                    print("Subscribed:",sym,flush=True)

                await send_alert("Scanner Started")

                async for message in ws:

                    data=json.loads(message)

                    if data.get("MessageType")=="RealtimeResult":
                        await process_data(data)

        except Exception as e:

            print("Connection error:",e)

            await asyncio.sleep(5)

if __name__=="__main__":
    asyncio.run(run_scanner())
