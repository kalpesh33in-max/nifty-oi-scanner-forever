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

# Updated to reflect standard BankNifty lot size
LOT_SIZES = {"BANKNIFTY": 15, "NIFTY": 25, "FINNIFTY": 25}
DEFAULT_LOT_SIZE = 15

# Updated Symbol List for 30MAR26 Expiry
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY30MAR2655800PE", "BANKNIFTY30MAR2655800CE",
    "BANKNIFTY30MAR2655900PE", "BANKNIFTY30MAR2655700CE",
    "BANKNIFTY30MAR2655900CE", "BANKNIFTY30MAR2655700PE",
    "BANKNIFTY30MAR2656000PE", "BANKNIFTY30MAR2655600CE",
    "BANKNIFTY30MAR2656000CE", "BANKNIFTY30MAR2655600PE",
    "BANKNIFTY30MAR2656100PE", "BANKNIFTY30MAR2655500CE",
    "BANKNIFTY30MAR2656100CE", "BANKNIFTY30MAR2655500PE",
    "BANKNIFTY30MAR2656200PE", "BANKNIFTY30MAR2655400CE",
    "BANKNIFTY30MAR2656200CE", "BANKNIFTY30MAR2655400PE",
    "BANKNIFTY30MAR2656300PE", "BANKNIFTY30MAR2655300CE",
    "BANKNIFTY30MAR2656300CE", "BANKNIFTY30MAR2655300PE",
    "BANKNIFTY30MAR2656400PE", "BANKNIFTY30MAR2655200CE",
    "BANKNIFTY30MAR2656400CE", "BANKNIFTY30MAR2655200PE",
    "BANKNIFTY30MAR2656500PE", "BANKNIFTY30MAR2655100CE",
    "BANKNIFTY30MAR2656500CE", "BANKNIFTY30MAR2655100PE",
    "BANKNIFTY30MAR2656600PE", "BANKNIFTY30MAR2655000CE",
    "BANKNIFTY30MAR2656600CE", "BANKNIFTY30MAR2655000PE",
    "BANKNIFTY30MAR2656700PE", "BANKNIFTY30MAR2654900CE",
    "BANKNIFTY30MAR2656700CE", "BANKNIFTY30MAR2654900PE",
    "BANKNIFTY30MAR2656800PE", "BANKNIFTY30MAR2654800CE",
    "BANKNIFTY30MAR2656800CE", "BANKNIFTY30MAR2654800PE",
    "BANKNIFTY30MAR2656900PE", "BANKNIFTY30MAR2654700CE",
    "BANKNIFTY30MAR2656900CE", "BANKNIFTY30MAR2654700PE",
    "BANKNIFTY30MAR2657000PE", "BANKNIFTY30MAR2654600CE",
    "BANKNIFTY30MAR2657000CE", "BANKNIFTY30MAR2654600PE",
    "BANKNIFTY30MAR2657100PE", "BANKNIFTY30MAR2654500CE",
    "BANKNIFTY30MAR2657100CE", "BANKNIFTY30MAR2654500PE",
    "BANKNIFTY30MAR2657200PE", "BANKNIFTY30MAR2654400CE",
    "BANKNIFTY30MAR2657200CE", "BANKNIFTY30MAR2654400PE",
    "BANKNIFTY30MAR2657300PE", "BANKNIFTY30MAR2654300CE",
    "BANKNIFTY30MAR2657300CE", "BANKNIFTY30MAR2654300PE",
    "BANKNIFTY30MAR2657400PE", "BANKNIFTY30MAR2654200CE",
    "BANKNIFTY30MAR2657400CE", "BANKNIFTY30MAR2654200PE",
    "BANKNIFTY30MAR2657500PE", "BANKNIFTY30MAR2654100CE",
    "BANKNIFTY30MAR2657500CE", "BANKNIFTY30MAR2654100PE",
    "BANKNIFTY30MAR2657600PE", "BANKNIFTY30MAR2654000CE",
    "BANKNIFTY30MAR2657600CE", "BANKNIFTY30MAR2654000PE",
    "BANKNIFTY30MAR2657700PE", "BANKNIFTY30MAR2653900CE",
    "BANKNIFTY30MAR2657700CE", "BANKNIFTY30MAR2653900PE",
    "BANKNIFTY30MAR2657800PE", "BANKNIFTY30MAR2653800CE",
    "BANKNIFTY30MAR2657800CE", "BANKNIFTY30MAR2653800PE",
    "BANKNIFTY30MAR2657900PE", "BANKNIFTY30MAR2653700CE",
    "BANKNIFTY30MAR2657900CE", "BANKNIFTY30MAR2653700PE",
    "BANKNIFTY30MAR2658000PE", "BANKNIFTY30MAR2653600CE",
    "BANKNIFTY30MAR2658000CE", "BANKNIFTY30MAR2653600PE",
    "BANKNIFTY30MAR2658100PE", "BANKNIFTY30MAR2653500CE",
    "BANKNIFTY30MAR2658100CE", "BANKNIFTY30MAR2653500PE",
    "BANKNIFTY-I"
]

# =============================== STATE TRACKING ===============================
last_prices = {}
last_oi = {}
active_watches = {} # {symbol: {"start_oi": val, "start_price": val, "expiry": time}}

# =============================== UTILITIES ====================================
def now(): return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

def get_lot_size(symbol):
    for key in LOT_SIZES:
        if key in symbol: return LOT_SIZES[key]
    return DEFAULT_LOT_SIZE

async def send_alert(message):
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, functools.partial(
            requests.post, TELEGRAM_API_URL, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}
        ))
    except Exception as e:
        print(f"Telegram Error: {e}")

# =============================== LOGIC ========================================
async def process_data(data):
    symbol = data.get("InstrumentIdentifier")
    if not symbol: return
    
    current_price = data.get("LastTradePrice", 0)
    current_oi = data.get("OpenInterest", 0)
    
    prev_oi = last_oi.get(symbol, 0)
    prev_price = last_prices.get(symbol, 0)
    
    last_oi[symbol] = current_oi
    last_prices[symbol] = current_price
    
    if prev_oi == 0: return

    lot_size = get_lot_size(symbol)
    oi_change_lots = (current_oi - prev_oi) / lot_size

    # 1. Trigger Watch if single tick is >= 100 Lots
    if abs(oi_change_lots) >= 100 and symbol not in active_watches:
        active_watches[symbol] = {
            "start_oi": prev_oi,
            "start_price": prev_price,
            "expiry": datetime.now() + timedelta(minutes=2)
        }

    # 2. Check Active Watches every tick
    current_time = datetime.now()
    if symbol in active_watches:
        watch = active_watches[symbol]
        if current_time >= watch["expiry"]:
            final_oi_change = current_oi - watch["start_oi"]
            final_lots = final_oi_change / lot_size
            
            # Confirm if net change is still >= 100 Lots after 2 minutes
            if abs(final_lots) >= 100:
                price_diff = current_price - watch["start_price"]
                sentiment = ""
                
                # Sentiment logic
                if final_oi_change > 0:
                    sentiment = "WRITER ✍️" if price_diff < 0 else "BUYING 🔥"
                else:
                    sentiment = "UNWINDING 📉" if price_diff < 0 else "SHORT COVERING ⤴️"

                msg = (f"🚨 HIGH VOL DETECTED: {symbol}\n"
                       f"SENTIMENT: {sentiment}\n"
                       f"━━━━━━━━━━━━━━━\n"
                       f"NET LOTS   : {final_lots:,.0f}\n"
                       f"PRICE CHG  : {price_diff:+.2f}\n"
                       f"━━━━━━━━━━━━━━━\n"
                       f"EXISTING OI: {watch['start_oi']:,}\n"
                       f"NEW OI     : {current_oi:,}\n"
                       f"TIME: {now()}")
                await send_alert(msg)
            
            del active_watches[symbol]

async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = await websocket.recv()
                if not json.loads(auth_resp).get("Complete"): 
                    print("Auth Failed"); await asyncio.sleep(10); continue
                
                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime", 
                        "Exchange": "NFO", 
                        "Unsubscribe": "false", 
                        "InstrumentIdentifier": sym
                    }))
                
                await send_alert(f"✅ Scanner Started | Monitoring {len(SYMBOLS_TO_MONITOR)} Symbols | 100-Lot Trigger.")
                print(f"✅ Scanner Live | Monitoring {len(SYMBOLS_TO_MONITOR)} symbols", flush=True)
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult": 
                        await process_data(msg_data)
        except Exception as e: 
            print(f"Connection Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_scanner())
