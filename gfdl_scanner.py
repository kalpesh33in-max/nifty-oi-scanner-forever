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
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN") 

# The Raw GitHub URL for your symbol.txt
GITHUB_SYMBOL_URL = "https://raw.githubusercontent.com/kalpesh33in-max/nifty-oi-scanner-forever/main/symbol.txt"

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Strictly BANKNIFTY per request
LOT_SIZE = 30 

# ============================== STATE & UTILITIES =============================
all_available_symbols = []
monitored_symbols = set()
symbol_data_state = {}
active_watches = {} 
future_price = 0
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
        print(f"⚠️ [{now()}] Telegram Error: {e}", flush=True)

def load_symbols_from_github():
    global all_available_symbols
    headers = {'Authorization': f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    
    try:
        print(f"🔄 [{now()}] Fetching BANKNIFTY symbols from GitHub...", flush=True)
        response = requests.get(GITHUB_SYMBOL_URL, headers=headers, timeout=15)
        if response.status_code == 200:
            raw_symbols = re.split(r'[,\n]+', response.text)
            cleaned = [s.strip().upper().replace(".NFO", "") for s in raw_symbols if "BANKNIFTY" in s.upper()]
            all_available_symbols = cleaned
            print(f"✅ [{now()}] Loaded {len(all_available_symbols)} BANKNIFTY symbols.", flush=True)
            return True
        else:
            print(f"❌ [{now()}] GitHub Load Error: HTTP {response.status_code}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] GitHub Load Error: {e}", flush=True)
    return False

def get_atm_range_symbols(bnf_price):
    if bnf_price == 0: return {"BANKNIFTY-I"}
    atm = round(bnf_price / 100) * 100
    # Range: ATM +- 23 strikes
    strikes = range(atm - 2300, atm + 2400, 100)
    
    selected = {"BANKNIFTY-I"}
    strike_list = list(strikes)
    
    for sym in all_available_symbols:
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
            # FIXED: Correct way to check if connection is open across library versions
            if active_ws and hasattr(active_ws, 'state') and str(active_ws.state).split('.')[-1] == 'OPEN':
                if future_price > 0:
                    current_atm = round(future_price / 100) * 100
                    if current_atm != last_atm:
                        new_symbols = get_atm_range_symbols(future_price)
                        
                        # Unsubscribe old symbols
                        to_remove = monitored_symbols - new_symbols
                        for sym in to_remove:
                            await active_ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "true", "InstrumentIdentifier": sym}))
                            if sym in symbol_data_state: del symbol_data_state[sym]
                        
                        # Subscribe to new symbols
                        to_add = new_symbols - monitored_symbols
                        for sym in to_add:
                            await active_ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": sym}))
                            symbol_data_state[sym] = {"price": 0, "oi": 0}
                        
                        monitored_symbols = new_symbols
                        last_atm = current_atm
                        print(f"🎯 [{now()}] ATM Updated: {current_atm} | Monitoring {len(monitored_symbols)} symbols", flush=True)
        except Exception as e:
            print(f"⚠️ [{now()}] Sub Update Error: {e}", flush=True)
        
        await asyncio.sleep(60)

# =============================== CORE LOGIC ===================================
def classify_action(symbol, oi_chg, price_chg):
    if symbol.endswith("-I"):
        if oi_chg > 0: return "FUTURE BUY (LONG) 📈" if price_chg >= 0 else "FUTURE SELL (SHORT) 📉"
        else: return "SHORT COVERING ↗️" if price_chg >= 0 else "LONG UNWINDING ↘️"
    
    is_call = symbol.endswith("CE")
    if oi_chg > 0:
        if price_chg >= 0: return "CALL BUY 🔵" if is_call else "PUT BUY 🔴"
        else: return "CALL WRITER ✍️" if is_call else "PUT WRITER ✍️"
    else:
        if price_chg >= 0: return "SHORT COVERING ⤴️"
        else: return "LONG UNWINDING ⤵️"

async def process_data(data):
    global symbol_data_state, active_watches, future_price
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state: return
    
    new_price, new_oi = data.get("LastTradePrice"), data.get("OpenInterest")
    if new_price is None or new_oi is None: return

    if symbol == "BANKNIFTY-I": 
        future_price = new_price

    state = symbol_data_state[symbol]
    if state["oi"] == 0:
        state["oi"], state["price"] = new_oi, new_price
        return

    # Trigger logic: Starts watch if tick is >= 100 lots
    oi_tick_diff = new_oi - state["oi"]
    tick_lots = int(abs(oi_tick_diff) / LOT_SIZE)

    if tick_lots >= 100 and symbol not in active_watches:
        print(f"⚡ [{now()}] Alert Watch Started: {symbol} | Tick: {tick_lots} Lots", flush=True)
        active_watches[symbol] = {
            "start_oi": state["oi"], "start_price": state["price"],
            "end_time": datetime.now() + timedelta(minutes=2)
        }

    state["oi"], state["price"] = new_oi, new_price

    if symbol in active_watches:
        watch = active_watches[symbol]
        if datetime.now() >= watch["end_time"]:
            final_oi_change = new_oi - watch["start_oi"]
            final_lots = int(abs(final_oi_change) / LOT_SIZE)
            
            if final_lots >= 100:
                strength = "🚀 BLAST 🚀" if final_lots >= 400 else "🌟 AWESOME" if final_lots >= 300 else "✅ VERY GOOD"
                price_change = new_price - watch["start_price"]
                action = classify_action(symbol, final_oi_change, price_change)
                
                msg = (f"{strength}\n🚨 {action}\nSymbol: {symbol}\n━━━━━━━━━━━━━━━\nLOTS: {final_lots}\n"
                       f"PRICE: {new_price:.2f} ({'▲' if price_change >= 0 else '▼'})\nFUTURE PRICE: {future_price:.2f}\n"
                       f"━━━━━━━━━━━━━━━\nEXISTING OI: {watch['start_oi']:,}\nOI CHANGE  : {final_oi_change:+,d}\nNEW OI     : {new_oi:,}\nTIME: {now()}")
                
                print(f"📢 [{now()}] ALERT SENT: {symbol} ({final_lots} Lots)", flush=True)
                await send_alert(msg)
            del active_watches[symbol]

async def run_scanner():
    global active_ws
    load_symbols_from_github()
    asyncio.create_task(update_subscriptions_loop()) 

    while True:
        try:
            print(f"🔄 [{now()}] Connecting to GFDL WebSocket...", flush=True)
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                active_ws = websocket
                await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                auth_resp = await websocket.recv()
                
                if not json.loads(auth_resp).get("Complete"): 
                    print(f"❌ [{now()}] Authentication Failed.", flush=True)
                    await asyncio.sleep(10); continue
                
                print(f"🔑 [{now()}] Authenticated Successfully.", flush=True)
                
                # Subscribe to Future to get price
                await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": "BANKNIFTY-I"}))
                symbol_data_state["BANKNIFTY-I"] = {"price": 0, "oi": 0}
                monitored_symbols.add("BANKNIFTY-I")
                
                print(f"✅ [{now()}] Scanner Live | Dynamic BANKNIFTY Tracker Active", flush=True)
                await send_alert("✅ BANKNIFTY Scanner Started Successfully")
                
                async for message in websocket:
                    msg_data = json.loads(message)
                    if msg_data.get("MessageType") == "RealtimeResult": 
                        await process_data(msg_data)
        except Exception as e: 
            print(f"❌ [{now()}] Connection Error: {e}", flush=True)
            active_ws = None
            await asyncio.sleep(5)

if __name__ == "__main__": 
    print(f"🚀 [{now()}] Initializing Scanner...", flush=True)
    asyncio.run(run_scanner())
