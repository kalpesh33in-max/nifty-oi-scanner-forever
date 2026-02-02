import asyncio
import websockets
import json
import os
import sys
import ssl
import requests
import functools
import re
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Corrected Lot Sizes for February 2026 Expiry
LOT_SIZES = {
    "BANKNIFTY": 30, 
    "NIFTY": 75, 
    "FINNIFTY": 40,
    "HDFCBANK": 550, 
    "SBIN": 750, 
    "ICICIBANK": 700, 
    "AXISBANK": 625, 
    "KOTAKBANK": 400
}
DEFAULT_LOT_SIZE = 75

SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2658900CE", "BANKNIFTY24FEB2658900PE", "BANKNIFTY24FEB2658800CE", "BANKNIFTY24FEB2658800PE",
    "BANKNIFTY24FEB2658700CE", "BANKNIFTY24FEB2658700PE", "BANKNIFTY24FEB2658600CE", "BANKNIFTY24FEB2658600PE",
    "BANKNIFTY24FEB2658500CE", "BANKNIFTY24FEB2658500PE", "BANKNIFTY24FEB2658400CE", "BANKNIFTY24FEB2658400PE",
    "BANKNIFTY24FEB2659000CE", "BANKNIFTY24FEB2659000PE", "BANKNIFTY24FEB2659100CE", "BANKNIFTY24FEB2659100PE",
    "BANKNIFTY24FEB2659200CE", "BANKNIFTY24FEB2659200PE", "BANKNIFTY24FEB2659300CE", "BANKNIFTY24FEB2659300PE",
    "BANKNIFTY24FEB2659400CE", "BANKNIFTY24FEB2659400PE",
    "HDFCBANK24FEB26930CE", "HDFCBANK24FEB26930PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "HDFCBANK24FEB26920CE", "HDFCBANK24FEB26920PE", "HDFCBANK24FEB26915CE", "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26910CE", "HDFCBANK24FEB26910PE", "HDFCBANK24FEB26905CE", "HDFCBANK24FEB26905PE",
    "HDFCBANK24FEB26935CE", "HDFCBANK24FEB26935PE", "HDFCBANK24FEB26940CE", "HDFCBANK24FEB26940PE",
    "HDFCBANK24FEB26945CE", "HDFCBANK24FEB26945PE", "HDFCBANK24FEB26950CE", "HDFCBANK24FEB26950PE",
    "SBIN24FEB261040CE", "SBIN24FEB261040PE", "SBIN24FEB261035CE", "SBIN24FEB261035PE",
    "SBIN24FEB261030CE", "SBIN24FEB261030PE", "SBIN24FEB261025CE", "SBIN24FEB261025PE",
    "ICICIBANK24FEB261350CE", "ICICIBANK24FEB261350PE", "ICICIBANK24FEB261340CE", "ICICIBANK24FEB261340PE",
    "ICICIBANK24FEB261330CE", "ICICIBANK24FEB261330PE", "ICICIBANK24FEB261320CE", "ICICIBANK24FEB261320PE",
    "BANKNIFTY-I", "HDFCBANK-I", "ICICIBANK-I", "SBIN-I", "AXISBANK-I", "KOTAKBANK-I"
]

# ============================== STATE & UTILITIES =============================
symbol_data_state = {symbol: {"price": 0, "oi": 0} for symbol in SYMBOLS_TO_MONITOR}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Error: {e}", flush=True)

# =============================== CORE LOGIC ===================================
async def process_data(data):
    global symbol_data_state
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")
    if new_price is None or new_oi is None: return

    state = symbol_data_state[symbol]
    prev_oi = state["oi"]
    prev_price = state["price"]
    
    # Update State for next comparison
    state["oi"] = new_oi
    state["price"] = new_price

    # Ignore first packet to establish a baseline
    if prev_oi == 0:
        return

    # 1. Calculate OI Change
    oi_change = new_oi - prev_oi
    if oi_change == 0: return

    # 2. Identify Lot Size
    base_match = re.match(r'^([A-Z]+)', symbol)
    base_symbol = base_match.group(1) if base_match else "UNKNOWN"
    lot_size = LOT_SIZES.get(base_symbol, DEFAULT_LOT_SIZE)
    
    # 3. Calculate Lots (Absolute value to catch buying and covering)
    lots_affected = int(abs(oi_change) / lot_size)

    # 4. TRIGGER: Only if Lots > 100
    if lots_affected >= 100:
        # Determine Directions
        oi_dir = "ADDITION 🟢" if oi_change > 0 else "EXIT/COVERING 🔴"
        price_change = new_price - prev_price
        price_dir = "UP ▲" if price_change > 0 else "DOWN ▼" if price_change < 0 else "FLAT ↔"
        
        # Build Alert Message
        alert_type = "FUTURE" if symbol.endswith("-I") else "OPTION"
        msg = (
            f"🔔 {alert_type} ALERT: {symbol}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"LOTS: {lots_affected} ({oi_dir})\n"
            f"PRICE: {new_price:.2f} ({price_dir})\n"
            f"━━━━━━━━━━━━━━━\n"
            f"EXISTING OI: {prev_oi:,}\n"
            f"OI CHANGE  : {oi_change:+,d}\n"
            f"NEW OI     : {new_oi:,}\n"
            f"PRICE CHG  : {price_change:+.2f}\n"
            f"TIME       : {now()}"
        )
        print(f"🚀 [ALERT] {symbol}: {lots_affected} lots detected.", flush=True)
        await send_alert(msg)

# ============================ MAIN SCANNER LOOP ===============================
async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                # Authentication
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = json.loads(await websocket.recv())
                if not auth_resp.get("Complete"):
                    print(f"❌ Auth Failed: {auth_resp.get('Comment')}", flush=True)
                    await asyncio.sleep(15)
                    continue
                
                # Subscription
                for sym in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime", "Exchange": "NFO",
                        "Unsubscribe": "false", "InstrumentIdentifier": sym
                    }))
                
                print(f"✅ Scanner Live at {now()}. Trigger: > 100 Lots.", flush=True)
                await send_alert("✅ GFDL Scanner is LIVE. Trigger set to > 100 Lots.")

                async for message in websocket:
                    data = json.loads(message)
                    if data.get("MessageType") == "RealtimeResult":
                        await process_data(data)

        except Exception as e:
            print(f"❌ Connection error: {e}. Reconnecting...", flush=True)
            await asyncio.sleep(10)

if __name__ == "__main__":
    if not all([API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("❌ CRITICAL: Missing Environment Variables.")
        sys.exit(1)
    asyncio.run(run_scanner())
