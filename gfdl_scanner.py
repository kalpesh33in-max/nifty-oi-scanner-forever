import asyncio
import websockets
import json
import time
import requests
from datetime import datetime
import re
import functools
import os
import sys
from zoneinfo import ZoneInfo

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

required_vars = {"API_KEY": API_KEY, "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN, "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID}
if any(v is None for v in required_vars.values()):
    print(f"❌ Missing environment variables.", flush=True)
    sys.exit(1)

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# --- UPDATED SYMBOL LIST (FEBRUARY EXPIRY) ---
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2660200CE", "BANKNIFTY24FEB2660200PE", "BANKNIFTY24FEB2660100CE", "BANKNIFTY24FEB2660100PE",
    "BANKNIFTY24FEB2660000CE", "BANKNIFTY24FEB2660000PE", "BANKNIFTY24FEB2659900CE", "BANKNIFTY24FEB2659900PE",
    "BANKNIFTY24FEB2659800CE", "BANKNIFTY24FEB2659800PE", "BANKNIFTY24FEB2659700CE", "BANKNIFTY24FEB2659700PE",
    "BANKNIFTY24FEB2660300CE", "BANKNIFTY24FEB2660300PE", "BANKNIFTY24FEB2660400CE", "BANKNIFTY24FEB2660400PE",
    "BANKNIFTY24FEB2660500CE", "BANKNIFTY24FEB2660500PE", "BANKNIFTY24FEB2660600CE", "BANKNIFTY24FEB2660600PE",
    "BANKNIFTY24FEB2660700CE", "BANKNIFTY24FEB2660700PE", "HDFCBANK24FEB26935CE", "HDFCBANK24FEB26935PE",
    "HDFCBANK24FEB26930CE", "HDFCBANK24FEB26930PE", "HDFCBANK24FEB26925CE", "HDFCBANK24FEB26925PE",
    "HDFCBANK24FEB26920CE", "HDFCBANK24FEB26920PE", "HDFCBANK24FEB26915CE", "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26910CE", "HDFCBANK24FEB26910PE", "HDFCBANK24FEB26940CE", "HDFCBANK24FEB26940PE",
    "HDFCBANK24FEB26945CE", "HDFCBANK24FEB26945PE", "HDFCBANK24FEB26950CE", "HDFCBANK24FEB26950PE",
    "HDFCBANK24FEB26955CE", "HDFCBANK24FEB26955PE", "HDFCBANK24FEB26960CE", "HDFCBANK24FEB26960PE",
    "SBIN24FEB261065CE", "SBIN24FEB261065PE", "SBIN24FEB261060CE", "SBIN24FEB261060PE",
    "SBIN24FEB261055CE", "SBIN24FEB261055PE", "SBIN24FEB261050CE", "SBIN24FEB261050PE",
    "SBIN24FEB261045CE", "SBIN24FEB261045PE", "SBIN24FEB261040CE", "SBIN24FEB261040PE",
    "SBIN24FEB261070CE", "SBIN24FEB261070PE", "SBIN24FEB261075CE", "SBIN24FEB261075PE",
    "SBIN24FEB261080CE", "SBIN24FEB261080PE", "SBIN24FEB261085CE", "SBIN24FEB261085PE",
    "SBIN24FEB261090CE", "SBIN24FEB261090PE", "ICICIBANK24FEB261380CE", "ICICIBANK24FEB261380PE",
    "ICICIBANK24FEB261370CE", "ICICIBANK24FEB261370PE", "ICICIBANK24FEB261360CE", "ICICIBANK24FEB261360PE",
    "ICICIBANK24FEB261350CE", "ICICIBANK24FEB261350PE", "ICICIBANK24FEB261340CE", "ICICIBANK24FEB261340PE",
    "ICICIBANK24FEB261330CE", "ICICIBANK24FEB261330PE", "ICICIBANK24FEB261390CE", "ICICIBANK24FEB261390PE",
    "ICICIBANK24FEB261400CE", "ICICIBANK24FEB261400PE", "ICICIBANK24FEB261410CE", "ICICIBANK24FEB261410PE",
    "ICICIBANK24FEB261420CE", "ICICIBANK24FEB261420PE", "ICICIBANK24FEB261430CE", "ICICIBANK24FEB261430PE",
    "SBIN24FEB26FUT", "HDFCBANK24FEB26FUT", "ICICIBANK24FEB26FUT", "BANKNIFTY24FEB26FUT",
    "NIFTY BANK", "HDFCBANK", "SBIN", "ICICIBANK"
]

LOT_SIZES = {"BANKNIFTY": 30, "HDFCBANK": 550, "ICICIBANK": 700, "SBIN": 750}
DEFAULT_LOT_SIZE = 75
OI_ROC_THRESHOLD = 2.0

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================

symbol_data_state = {symbol: {"price": 0, "price_prev": 0, "oi": 0, "oi_prev": 0} for symbol in SYMBOLS_TO_MONITOR}
future_prices = {"BANKNIFTY": 0, "HDFCBANK": 0, "ICICIBANK": 0, "SBIN": 0}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    loop = asyncio.get_running_loop()
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    blocking_call = functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10)
    try:
        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status() 
    except Exception as e:
        print(f"❌ Telegram Error: {e}", flush=True)

# ==============================================================================
# =============================== CORE LOGIC ===================================
# ==============================================================================

def get_option_moneyness(symbol, future_prices):
    if "FUT" in symbol or ".NSE" in symbol: return "N/A"
    underlying = next((u for u in future_prices if u in symbol), None)
    if not underlying: return "N/A"
    f_price = future_prices.get(underlying, 0)
    if f_price == 0: return "OTM"
    try:
        match = re.search(r'.*?(\d{2})(\d+)(CE|PE)$', symbol)
        strike = int(match.group(2))
        opt_type = match.group(3)
        atm_band = f_price * 0.001
        if abs(f_price - strike) <= atm_band: return "ATM"
        if (opt_type == 'CE' and strike < f_price) or (opt_type == 'PE' and strike > f_price): return "ITM"
    except: return "N/A"
    return "OTM"

def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices):
    price_dir = "↑" if (state['price'] - state['price_prev']) > 0 else "↓" if (state['price'] - state['price_prev']) < 0 else "↔"
    p_name = "BANKNIFTY" if "BANKNIFTY" in symbol else "HDFCBANK" if "HDFCBANK" in symbol else "ICICI" if "ICICIBANK" in symbol else "SBIN"
    f_lookup = "ICICIBANK" if p_name == "ICICI" else p_name
    f_price = future_prices.get(f_lookup, 0)
    is_fut = "FUT" in symbol
    try:
        m = re.search(r'.*?(\d{2})(\d+)(CE|PE)$', symbol) if not is_fut else re.search(r'.*?(\d{2})FUT$', symbol)
        year, strike, o_type = (m.group(1), m.group(2), m.group(3)) if not is_fut else (m.group(1), "FUT", "")
    except: year, strike, o_type = "", "N/A", ""

    return f"*{p_name} | {'FUTURE' if is_fut else 'OPTION'}*\n" \
           f"STRIKE: {strike}{o_type} {moneyness}\nACTION: {action}\nSIZE: {bucket} ({lots} lots)\n" \
           f"OI Δ: {oi_chg}\nOI RoC: {oi_roc:.2f}%\nPRICE: {price_dir}\nTIME: {now()}\n\n" \
           f"FUTURE PRICE: {f_price:.2f}\nLAST PRICE: {state['price']:.2f}"

async def process_data(data):
    global symbol_data_state, future_prices
    raw_symbol = data.get("InstrumentIdentifier", "")
    # Clean the symbol name for our internal state (remove .NFO, .NSE_IDX, etc.)
    symbol = raw_symbol.split('.')[0]
    
    new_price = data.get("LastTradePrice")
    if not symbol or symbol not in symbol_data_state or new_price is None: return

    # Update Futures/Index price for moneyness calculation
    if "FUT" in symbol or any(u == symbol for u in future_prices):
        for u in future_prices:
            if u in symbol: future_prices[u] = new_price
        if not "FUT" in symbol: return # Skip OI logic for cash/index symbols

    state = symbol_data_state[symbol]
    new_oi = data.get("OpenInterest")
    if new_oi is None: return

    state["price_prev"], state["oi_prev"] = state["price"], state["oi"]
    state["price"], state["oi"] = new_price, new_oi
    if state["oi_prev"] == 0: return

    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0: return
    
    oi_roc = (oi_chg / state["oi_prev"]) * 100
    if abs(oi_roc) > OI_ROC_THRESHOLD:
        # Lot calculation
        lot_size = next((s for n, s in LOT_SIZES.items() if n in symbol), DEFAULT_LOT_SIZE)
        lots = int(abs(oi_chg) / lot_size)
        
        is_fut = "FUT" in symbol
        alert_ok, moneyness = False, "N/A"
        
        if is_fut and lots > 50: alert_ok = True
        elif not is_fut and lots > 100:
            moneyness = get_option_moneyness(symbol, future_prices)
            if moneyness in ["ITM", "ATM"]: alert_ok = True
        
        if alert_ok:
            bucket = "EXTREME" if lots >= 200 else "HIGH" if lots >= 100 else "MEDIUM"
            action = "BUYER" if (state["price"]-state["price_prev"]) > 0 else "WRITER"
            msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices)
            await send_telegram(msg)

async def run_scanner():
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
                if not json.loads(await ws.recv()).get("Complete"): 
                    await asyncio.sleep(30); continue
                
                # Subscription logic with suffixes for the exchange
                for s in SYMBOLS_TO_MONITOR:
                    exch = "NSE" if ".NSE" in s or any(u == s for u in ["SBIN", "HDFCBANK", "ICICIBANK"]) else "NFO"
                    if s == "NIFTY BANK": exch = "NSE_IDX"
                    await ws.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": exch, "InstrumentIdentifier": s}))
                
                await send_telegram("✅ Scanner Live with FEB Expiry symbols.")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("MessageType") == "RealtimeResult": await process_data(data)
        except Exception as e:
            print(f"Connection lost: {e}"); await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(run_scanner())
