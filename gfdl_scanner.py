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
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN") # Needed if repo is Private

# The Raw GitHub URL for your symbol.txt
GITHUB_SYMBOL_URL = "https://raw.githubusercontent.com/kalpesh33in-max/nifty-oi-scanner-forever/main/symbol.txt"

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 25, "FINNIFTY": 25, "HDFCBANK": 550, "ICICIBANK": 700, "AXISBANK": 625, "SBIN": 750}
DEFAULT_LOT_SIZE = 30

# ============================== STATE & UTILITIES =============================
all_available_symbols = []
monitored_symbols = set()
symbol_data_state = {}
active_watches = {} 
future_prices = {k: 0 for k in LOT_SIZES.keys()}
last_atm = 0
active_ws = None

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_alert(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    try:
        await loop.run_in_executor(None, functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10))
    except Exception as e:
        print(f"⚠️ Telegram Error: {e}")

def load_symbols_from_github():
    global all_available_symbols
    headers = {}
    if GITHUB_TOKEN:
        headers['Authorization'] = f"token {GITHUB_TOKEN}"
    
    try:
        print("🔄 Fetching symbols from GitHub...")
        response = requests.get(GITHUB_SYMBOL_URL, headers=headers, timeout=15)
        if response.status_code == 200:
            text_data = response.text
            # Split by comma or newline to handle the format seen in the screenshot
            raw_symbols = re.split(r'[,\n]+', text_data)
            cleaned = []
            for sym in raw_symbols:
                sym = sym.strip().upper()
                if sym:
                    # Remove .NFO and append to cleaned list
                    sym = sym.replace(".NFO", "")
                    cleaned.append(sym)
            all_available_symbols = cleaned
            print(f"✅ Loaded {len(all_available_symbols)} symbols from GitHub.")
            return True
        else:
            print(f"❌ GitHub Load Error: HTTP {response.status_code}. (If repo is private, ensure GITHUB_TOKEN is set in environment vars)")
    except Exception as e:
        print(f"❌ GitHub Load Error: {e}")
    return False

def get_atm_range_symbols(bnf_price):
    if bnf_price == 0: return {"BANKNIFTY-I"}
    atm = round(bnf_price / 100) * 100
    # Range: ATM +- 23 strikes (2300 points above and below)
    strikes = range(atm - 2300, atm + 2400, 100)
    
    selected = {"BANKNIFTY-I"}
    strike_list = list(strikes)
    
    for sym in all_available_symbols:
        if "BANKNIFTY" in sym and sym != "BANKNIFTY-I":
            # Extract strike from symbol using regex
            match = re.search(r'(\d{5})(CE|PE)$', sym)
            if match:
                strike = int(match.group(1))
                if strike in strike_list:
                    selected.add(sym)
    return selected

async def update_subscriptions_loop():
    global monitored_symbols, last_atm, active_ws
    while True:
        try:
            # Check if websocket exists and is NOT closed
            if active_ws is not None:
                is_connected = False
                try:
                    # Try different ways to check connection state for library compatibility
                    if hasattr(active_ws, 'open'):
                        is_connected = active_ws.open
                    else:
                        is_connected = not active_ws.closed
                except Exception:
                    is_connected = True # Fallback to true if check fails

                if is_connected:
                    bnf_price = future_prices.get("BANKNIFTY", 0)
                    if bnf_price > 0:
                        current_atm = round(bnf_price / 100) * 100
                        if current_atm != last_atm:
                            new_symbols = get_atm_range_symbols(bnf_price)
                            
                            # 1. Unsubscribe from old ones moving out of range
                            to_remove = monitored_symbols - new_symbols
                            for sym in to_remove:
                                await active_ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "true", "InstrumentIdentifier": sym}))
                                if sym in symbol_data_state: del symbol_data_state[sym]
                            
                            # 2. Subscribe to new ones coming into range
                            to_add = new_symbols - monitored_symbols
                            for sym in to_add:
                                await active_ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": sym}))
                                if sym not in symbol_data_state: symbol_data_state[sym] = {"price": 0, "oi": 0}
                            
                            monitored_symbols = new_symbols
                            last_atm = current_atm
                            print(f"🎯 ATM Updated: {current_atm} | Monitoring {len(monitored_symbols)} symbols", flush=True)
        except Exception as e:
            print(f"⚠️ Sub Update Error: {e}")
        
        await asyncio.sleep(60) # Re-check ATM range every 60 seconds

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
                
                # Maintaining your exact existing output format
                msg = (f"{strength}\n🚨 {action}\nSymbol: {symbol}\n━━━━━━━━━━━━━━━\nLOTS: {final_lots}\n"
                       f"PRICE: {new_price:.2f} ({'▲' if price_change >= 0 else '▼'})\nFUTURE PRICE: {future_prices.get(base_symbol, 0):.2f}\n"
                       f"━━━━━━━━━━━━━━━\nEXISTING OI: {watch['start_oi']:,}\nOI CHANGE  : {final_oi_change:+,d}\nNEW OI     : {new_oi:,}\nTIME: {now()}")
                await send_alert(msg)
            del active_watches[symbol]

async def run_scanner():
    global active_ws
    load_symbols_from_github()
    asyncio.create_task(update_subscriptions_loop()) # Start the ATM monitor

    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                active_ws = websocket
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = await websocket.recv()
                if not json.loads(auth_resp).get("Complete"): 
                    await asyncio.sleep(10); continue
                
                # Initial subscription to Future to get price
                await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": "BANKNIFTY-I"}))
                if "BANKNIFTY-I" not in symbol_data_state: symbol_data_state["BANKNIFTY-I"] = {"price": 0, "oi": 0}
                monitored_symbols.add("BANKNIFTY-I")
                
                await send_alert("✅ Dynamic ATM Scanner Started | Awaiting Future Price...")
                print(f"✅ Scanner Live | Dynamic ATM Tracker Active", flush=True)
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult": await process_data(msg_data)
        except Exception as e: 
            print(f"Connection Error: {e}")
            active_ws = None
            await asyncio.sleep(5)

if __name__ == "__main__": asyncio.run(run_scanner())
