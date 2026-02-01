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

# --- Environment Variable Loading ---
# Load credentials securely from environment variables
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- Validation ---
# Ensure all required environment variables are set
required_vars = {
    "API_KEY": API_KEY,
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
}

missing_vars = [key for key, value in required_vars.items() if value is None]

if missing_vars:
    print(f"❌ Critical Error: Missing required environment variables: {', '.join(missing_vars)}", flush=True)
    print("Please set these variables in your deployment environment and restart the application.", flush=True)
    sys.exit(1) # Exit the script with a non-zero status code to indicate failure

# --- API and Connection ---
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# --- Symbol List (Options & Futures) ---
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY24FEB2659600CE.NFO",
    "BANKNIFTY24FEB2659600PE.NFO",
    "BANKNIFTY24FEB2659500CE.NFO",
    "BANKNIFTY24FEB2659500PE.NFO",
    "BANKNIFTY24FEB2659400CE.NFO",
    "BANKNIFTY24FEB2659400PE.NFO",
    "BANKNIFTY24FEB2659300CE.NFO",
    "BANKNIFTY24FEB2659300PE.NFO",
    "BANKNIFTY24FEB2659200CE.NFO",
    "BANKNIFTY24FEB2659200PE.NFO",
    "BANKNIFTY24FEB2659100CE.NFO",
    "BANKNIFTY24FEB2659100PE.NFO",
    "BANKNIFTY24FEB2659700CE.NFO",
    "BANKNIFTY24FEB2659700PE.NFO",
    "BANKNIFTY24FEB2659800CE.NFO",
    "BANKNIFTY24FEB2659800PE.NFO",
    "BANKNIFTY24FEB2659900CE.NFO",
    "BANKNIFTY24FEB2659900PE.NFO",
    "BANKNIFTY24FEB2660000CE.NFO",
    "BANKNIFTY24FEB2660000PE.NFO",
    "BANKNIFTY24FEB2660100CE.NFO",
    "BANKNIFTY24FEB2660100PE.NFO",
    "HDFCBANK24FEB26930CE.NFO",
    "HDFCBANK24FEB26930PE.NFO",
    "HDFCBANK24FEB26925CE.NFO",
    "HDFCBANK24FEB26925PE.NFO",
    "HDFCBANK24FEB26920CE.NFO",
    "HDFCBANK24FEB26920PE.NFO",
    "HDFCBANK24FEB26915CE.NFO",
    "HDFCBANK24FEB26915PE.NFO",
    "HDFCBANK24FEB26910CE.NFO",
    "HDFCBANK24FEB26910PE.NFO",
    "HDFCBANK24FEB26905CE.NFO",
    "HDFCBANK24FEB26905PE.NFO",
    "HDFCBANK24FEB26935CE.NFO",
    "HDFCBANK24FEB26935PE.NFO",
    "HDFCBANK24FEB26940CE.NFO",
    "HDFCBANK24FEB26940PE.NFO",
    "HDFCBANK24FEB26945CE.NFO",
    "HDFCBANK24FEB26945PE.NFO",
    "HDFCBANK24FEB26950CE.NFO",
    "HDFCBANK24FEB26950PE.NFO",
    "HDFCBANK24FEB26955CE.NFO",
    "HDFCBANK24FEB26955PE.NFO",
    "ICICIBANK24FEB261350CE.NFO",
    "ICICIBANK24FEB261350PE.NFO",
    "ICICIBANK24FEB261340CE.NFO",
    "ICICIBANK24FEB261340PE.NFO",
    "ICICIBANK24FEB261330CE.NFO",
    "ICICIBANK24FEB261330PE.NFO",
    "ICICIBANK24FEB261320CE.NFO",
    "ICICIBANK24FEB261320PE.NFO",
    "ICICIBANK24FEB261310CE.NFO",
    "ICICIBANK24FEB261310PE.NFO",
    "ICICIBANK24FEB261300CE.NFO",
    "ICICIBANK24FEB261300PE.NFO",
    "ICICIBANK24FEB261360CE.NFO",
    "ICICIBANK24FEB261360PE.NFO",
    "ICICIBANK24FEB261370CE.NFO",
    "ICICIBANK24FEB261370PE.NFO",
    "ICICIBANK24FEB261380CE.NFO",
    "ICICIBANK24FEB261380PE.NFO",
    "ICICIBANK24FEB261390CE.NFO",
    "ICICIBANK24FEB261390PE.NFO",
    "ICICIBANK24FEB261400CE.NFO",
    "ICICIBANK24FEB261400PE.NFO",
    "SBIN24FEB261370CE.NFO",
    "SBIN24FEB261370PE.NFO",
    "SBIN24FEB261365CE.NFO",
    "SBIN24FEB261365PE.NFO",
    "SBIN24FEB261360CE.NFO",
    "SBIN24FEB261360PE.NFO",
    "SBIN24FEB261355CE.NFO",
    "SBIN24FEB261355PE.NFO",
    "SBIN24FEB261350CE.NFO",
    "SBIN24FEB261350PE.NFO",
    "SBIN24FEB261345CE.NFO",
    "SBIN24FEB261345PE.NFO",
    "SBIN24FEB261375CE.NFO",
    "SBIN24FEB261375PE.NFO",
    "SBIN24FEB261380CE.NFO",
    "SBIN24FEB261380PE.NFO",
    "SBIN24FEB261385CE.NFO",
    "SBIN24FEB261385PE.NFO",
    "SBIN24FEB261390CE.NFO",
    "SBIN24FEB261390PE.NFO",
    "SBIN24FEB261395CE.NFO",
    "SBIN24FEB261395PE.NFO",
    "SBIN24FEB26FUT",
    "HDFCBANK24FEB26FUT",
    "ICICIBANK24FEB26FUT",
    "BANKNIFTY24FEB26FUT",

]

# --- Logic & Thresholds ---
LOT_SIZES = {
    "BANKNIFTY": 30,
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "SBIN": 750,
}
DEFAULT_LOT_SIZE = 75 # For any other symbol
OI_ROC_THRESHOLD = 2.0 # Temporarily lowered for IV testing
MOMENTUM_WINDOW = 300 # 5 minutes in seconds

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================

# State now supports a history of ticks for momentum analysis
symbol_data_state = {
    symbol: {
        # State for single-tick OI RoC alerts
        "price": 0, "price_prev": 0,
        "oi": 0, "oi_prev": 0,
        # State for 5-minute momentum alerts
        "ticks": [],  # Stores (timestamp, price, oi) tuples
        "last_trend_alert_type": None, # e.g., "STRONG_UPTREND"
        "last_trend_alert_time": 0,    # time.time() of the last alert
    } for symbol in SYMBOLS_TO_MONITOR
}

# Dictionary to hold the latest price of the underlying futures
future_price_state = {
    "BANKNIFTY": {"ticks": []},
    "HDFCBANK": {"ticks": []},
    "ICICIBANK": {"ticks": []},
    "SBIN": {"ticks": []},
}

# Dictionary to hold the latest price of the underlying spot instruments




def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    """Sends a message to the configured Telegram chat without blocking the event loop."""
    print(f"📦 [{now()}] Preparing to send Telegram message...", flush=True)
    loop = asyncio.get_running_loop()
    
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg}
    
    # Use functools.partial to prepare the blocking function with its arguments
    blocking_call = functools.partial(requests.post, TELEGRAM_API_URL, params=params, timeout=10)

    try:
        # Run the blocking call in a separate thread
        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status() 
        print(f"✅ [{now()}] Telegram message sent successfully. Response: {response.text}", flush=True)
    except requests.exceptions.RequestException as e:
        print(f"❌ [{now()}] FAILED to send Telegram message: {e}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] An unexpected error occurred while sending Telegram message: {e}", flush=True)

async def send_alert(msg: str):
    """Dispatches the alert to the configured Telegram channel."""
    await send_telegram(msg)

# ==============================================================================
# =============================== CORE LOGIC ===================================
# ==============================================================================

def lots_from_oi_change(symbol, oi_change):
    """Calculates the number of lots from a change in Open Interest."""
    lot_size = DEFAULT_LOT_SIZE
    for name, size in LOT_SIZES.items():
        if name in symbol:
            lot_size = size
            break
    if lot_size == 0: return 0
    return int(abs(oi_change) / lot_size)

def lot_bucket(lots):
    """Classifies the number of lots into qualitative buckets."""
    if lots >= 200: return "EXTREME HIGH"
    if lots >= 150: return "EXTRA HIGH"
    if lots >= 100: return "HIGH"
    if lots >= 75: return "MEDIUM"
    if lots >= 1: return "LOW"
    return "IGNORE"

def classify_option(oi_change, price_change, symbol):
    if price_change == 0:
        if oi_change > 0:
            return "HEDGING"
        elif oi_change < 0:
            return "REMOVE FROM HEDGE"
    elif oi_change > 0:
        if price_change > 0:
            return "BUYER(LONG)"
        else:  # price_change < 0
            return "WRITER(SHORT)"
    elif oi_change < 0:
        if price_change > 0:
            return "REMOVE FROM SHORT"
        else:  # price_change < 0
            return "REMOVE FROM LONG"
    
    return "Indecisive Movement"

def get_option_moneyness(symbol, future_price_state):
    """
    Checks if an option is ITM, ATM, or OTM based on the latest future price.
    Returns a string: "ITM", "ATM", "OTM", or "".
    """
    if "FUT" in symbol:
        return "" # Futures don't have moneyness in this context

    underlying_name = None
    if "HDFCBANK" in symbol: underlying_name = "HDFCBANK"
    elif "ICICIBANK" in symbol: underlying_name = "ICICIBANK"
    elif "SBIN" in symbol: underlying_name = "SBIN"
    elif "BANKNIFTY" in symbol: underlying_name = "BANKNIFTY"

    if not underlying_name or underlying_name not in future_price_state:
        return ""

    # Get the latest future price from the ticks
    future_ticks = future_price_state[underlying_name].get("ticks", [])
    if not future_ticks:
        print(f"⏳ [{now()}] {symbol}: Waiting for future price of {underlying_name} to check moneyness.", flush=True)
        return ""
    
    future_price = future_ticks[-1][1] # Latest price is the second element of the last tick
    if future_price == 0: # Ensure we have a valid price
        return ""

    try:
        # Extract strike from symbols like 'BANKNIFTY24FEB2660000CE.NFO'
        match = re.search(r'(\d+)(CE|PE)\.NFO$', symbol)
        if not match: # Fallback for symbols without .NFO
             match = re.search(r'(\d+)(CE|PE)$', symbol)
        
        strike_price = int(match.group(1))
        option_type = match.group(2)
    except (AttributeError, TypeError, ValueError):
        return ""

    # Define ATM band (0.1% of future price)
    atm_band = future_price * 0.001
    
    if abs(future_price - strike_price) <= atm_band:
        return "ATM"
    
    is_itm = (option_type == 'CE' and strike_price < future_price) or \
             (option_type == 'PE' and strike_price > future_price)

    return "ITM" if is_itm else "OTM"

def check_momentum_trends(symbol, state, future_price_state):
    """
    Analyzes 5-minute data based on "logic 2.pdf" cases.
    Returns a tuple of (trend_name, interpretation, analysis_data) if a case is matched.
    """
    ticks = state.get("ticks", [])
    if len(ticks) < 2: return None

    first_tick_time, last_tick_time = ticks[0][0], ticks[-1][0]
    window_duration = last_tick_time - first_tick_time
    if window_duration < MOMENTUM_WINDOW / 2: return None

    underlying = next((name for name in future_price_state if name in symbol), None)
    if not underlying: return None

    future_ticks = future_price_state[underlying].get("ticks", [])
    if not future_ticks: return None
    
    start_future_price_tick = next((ft for ft in future_ticks if ft[0] >= first_tick_time), None)
    if not start_future_price_tick: return None
    start_future_price = start_future_price_tick[1]
    end_future_price = future_ticks[-1][1]

    start_option_price, start_oi = ticks[0][1], ticks[0][2]
    end_option_price, end_oi = ticks[-1][1], ticks[-1][2]

    future_price_chg = end_future_price - start_future_price
    option_price_chg = end_option_price - start_option_price
    oi_chg = end_oi - start_oi

    # Check if lot size from OI change is >= 100
    lots = lots_from_oi_change(symbol, oi_chg)
    if lots < 100:
        return None

    is_call = "CE" in symbol
    is_put = "PE" in symbol
    future_up = future_price_chg > 0
    future_down = future_price_chg < 0
    future_flat = not future_up and not future_down
    price_up = option_price_chg > 0
    price_down = option_price_chg < 0
    oi_up = oi_chg > 0
    oi_down = oi_chg < 0
    
    trend_name, interpretation = None, None

    # Bullish Cases
    if is_call and future_up and price_down and oi_up:
        trend_name = "📈 Case-1: Strong Bullish (Smart money)"
        interpretation = "Call writing + Future moving up -> Resistance absorbed -> slow trending up"
    elif is_call and future_up and price_up and oi_down:
        trend_name = "🟢📈 Case-2: Breakout Bullish" # Added green circle
        interpretation = "Call writers trapped -> Fast upside move"
    elif is_put and future_up and price_down and oi_up:
        trend_name = "📈 Case-3: Support-based Bullish"
        interpretation = "Put writing -> Strong support below"
        
    # Bearish Cases
    elif is_put and future_down and price_down and oi_up:
        trend_name = "📉 Case-4: Strong Bearish (Smart money)"
        interpretation = "Put writing + downside control -> Slow grind down"
    elif is_put and future_down and price_up and oi_down:
        trend_name = "🔴📉 Case-5: Breakdown Bearish" # Added red circle
        interpretation = "Put writers trapped -> Fast fall"
    elif is_call and future_down and price_down and oi_up:
        trend_name = "📉 Case-6: Resistance-based Bearish"
        interpretation = "Call writing -> Strong resistance above"

    # Trap Conditions
    elif is_call and (future_flat or future_down) and price_up and oi_up:
        trend_name = "🚨 Bull Trap"
        interpretation = "Retail buying calls -> Avoid longs"
    elif is_put and (future_flat or future_up) and price_up and oi_up:
        trend_name = "🚨 Bear Trap"
        interpretation = "Panic buying puts -> Avoid shorts"

    if trend_name:
        analysis_data = {
            "start_time": first_tick_time, "end_time": last_tick_time, "duration": window_duration,
            "start_oi": start_oi, "end_oi": end_oi, "oi_chg": oi_chg,
            "start_option_price": start_option_price, "end_option_price": end_option_price, "option_price_chg": option_price_chg,
            "start_future_price": start_future_price, "end_future_price": end_future_price, "future_price_chg": future_price_chg,
        }
        return (trend_name, interpretation, analysis_data)
        
    return None

def format_momentum_alert(symbol, trend_name, interpretation, data, future_price_state):
    """
    Formats the 5-minute momentum alert message based on the new "logic 2.pdf" format.
    """
    product_name, strike_display, option_type, year = "UNKNOWN", "N/A", "", ""
    if "HDFCBANK" in symbol: product_name = "HDFCBANK"
    elif "ICICIBANK" in symbol: product_name = "ICICI"
    elif "SBIN" in symbol: product_name = "SBIN"
    elif "BANKNIFTY" in symbol: product_name = "BANKNIFTY"
    
    try:
        match = re.search(r'(\d+)(CE|PE)\.NFO$', symbol)
        if not match:
            match = re.search(r'(\d+)(CE|PE)$', symbol)
        strike_display, option_type = match.group(1), match.group(2)
    except Exception: pass

    lots = lots_from_oi_change(symbol, data['oi_chg'])
    try:
        oi_roc = (data['oi_chg'] / data['start_oi']) * 100
    except ZeroDivisionError:
        oi_roc = 0.0
    
    moneyness = get_option_moneyness(symbol, future_price_state)

    # --- Movement Indicators ---
    future_dir = "↑" if data['future_price_chg'] > 0 else "↓" if data['future_price_chg'] < 0 else "↔"
    option_dir = "↑" if data['option_price_chg'] > 0 else "↓" if data['option_price_chg'] < 0 else "↔"
    oi_dir = "↑" if data['oi_chg'] > 0 else "↓"

    # --- Formatting ---
    header = "- - - 5-Min Momentum Alert - - -"
    line1 = f"{product_name} | {strike_display}{option_type} ({moneyness})" if moneyness else f"{product_name} | {strike_display}{option_type}"
    line2 = f"\n{trend_name}\nInterpretation: {interpretation}\n"
    
    analysis_header = "--- Analysis Breakdown ---"
    future_line = f"Future Price:   {future_dir} ({data['future_price_chg']:+.2f})"
    option_line = f"Option Price:   {option_dir} ({data['option_price_chg']:+.2f})"
    oi_line =     f"Option OI:      {oi_dir} ({data['oi_chg']:+,.0f})"
    
    data_header = "\n--- Data Points ---"
    existing_oi_line = f"Existing OI: {data['start_oi']:,.0f}"
    oi_delta_line = f"OI Δ: {data['oi_chg']:+,.0f} ({lots} lots)"
    oi_roc_line = f"OI RoC: {oi_roc:+.2f}%"
    last_option_price_line = f"Last Option Price: {data['end_option_price']:.2f}"
    last_future_price_line = f"Last Future Price: {data['end_future_price']:.2f}"

    duration_minutes = int(data['duration'] // 60)
    duration_seconds = int(data['duration'] % 60)
    start_time_str = datetime.fromtimestamp(data['start_time'], ZoneInfo("Asia/Kolkata")).strftime('%H:%M')
    end_time_str = datetime.fromtimestamp(data['end_time'], ZoneInfo("Asia/Kolkata")).strftime('%H:%M')
    duration_line = f"Duration: {duration_minutes}m {duration_seconds}s ({start_time_str} -> {end_time_str})"
    
    footer = "- - - - - - - - - - - - - - - -"

    return "\n".join([
        header, line1, line2, analysis_header, future_line, option_line, oi_line,
        data_header, existing_oi_line, oi_delta_line, oi_roc_line, last_option_price_line,
        last_future_price_line, duration_line, footer
    ])


def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_price_state_global):
    """Formats the alert message, showing N/A for missing IV."""
    price_chg = state['price'] - state['price_prev']
    if price_chg > 0:
        price_dir = "↑"
    elif price_chg < 0:
        price_dir = "↓"
    else:
        price_dir = "↔"

    # Dynamically determine product name
    product_name = "UNKNOWN" # Default
    if "HDFCBANK" in symbol:
        product_name = "HDFCBANK"
    elif "ICICIBANK" in symbol:
        product_name = "ICICI" # As requested by user
    elif "SBIN" in symbol:
        product_name = "SBIN"
    elif "BANKNIFTY" in symbol:
        product_name = "BANKNIFTY"

    year, strike_display, option_type_display = "", "", ""
    is_future = "FUT" in symbol # Determine if it's a future

    try:
        if is_future:
            match = re.search(r'.*?(\d{2})FUT$', symbol)
            if match:
                year = match.group(1)
            strike_display = "FUT" # For internal logic, but won't be displayed in main_message
            option_type_display = ""
        else: # For options
            match = re.search(r'.*?(\d{2})(\d+)(CE|PE)$', symbol)
            if match:
                year = match.group(1)
                strike_display = match.group(2)
                option_type_display = match.group(3)
            else: # Fallback for old format without date
                match = re.search(r'(\d+)(CE|PE)$', symbol)
                strike_display = match.group(1)
                option_type_display = match.group(2)
    except Exception:
        year, strike_display, option_type_display = "", "N/A", "" # Fallback if parsing completely fails

    if is_future:
        main_message = f"""{product_name} | FUTURE
ACTION: {action}
SIZE: {bucket} ({lots} lots)
EXISTING OI: {state['oi_prev']}
OI Δ: {oi_chg}
PRICE: {price_dir}
TIME: {now()}
"""
        added_section = f"""{year} {product_name} FUT

{state['price']:.2f}
"""
        return f"{main_message}\n\n{added_section}"
    else:
        # Ensure correct underlying key for future_prices lookup
        underlying_lookup_key = product_name
        if product_name == "ICICI":
            underlying_lookup_key = "ICICIBANK"
        future_price_val = 0
        if underlying_lookup_key in future_price_state_global and future_price_state_global[underlying_lookup_key]["ticks"]:
            future_price_val = future_price_state_global[underlying_lookup_key]["ticks"][-1][1]
        line11 = f"FUTURE PRICE: {future_price_val:.2f}"
        line12 = f"LAST PRICE: {state['price']:.2f}"

        return f"{line1}\n{line2}\n{line3}\n{line4}\n{line5}\n{line6}\n{line7}\n{line8}\n{line9}\n{line10}\n{line11}\n{line12}"

# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================

async def process_data(data):
    """
    Processes a single data packet, updating future prices or sending option alerts.
    Option alerts are filtered to only include ITM/ATM strikes.
    """
    global symbol_data_state, future_price_state
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    new_price = data.get("LastTradePrice")
    if new_price is None or new_price <= 0:
        return

    # --- Handle different instrument types ---



    # If the symbol is a future, update its price history and stop
    if "FUT" in symbol:
        underlying = next((name for name in future_price_state if name in symbol), None)
        if underlying:
            current_time = time.time()
            future_state = future_price_state[underlying]
            future_state["ticks"].append((current_time, new_price))
            future_state["ticks"] = [tick for tick in future_state["ticks"] if current_time - tick[0] <= MOMENTUM_WINDOW]
        return

    # --- The rest of the function will only execute for OPTIONS ---
    state = symbol_data_state[symbol]
    new_oi = data.get("OpenInterest")

    if new_oi is None:
        return

    # --- History Management for Momentum Alerts ---
    current_time = time.time()
    if new_oi > 0: # Only store ticks with valid OI
        state["ticks"].append((current_time, new_price, new_oi))
    state["ticks"] = [tick for tick in state["ticks"] if current_time - tick[0] <= MOMENTUM_WINDOW]

    # --- Standard processing for Option contracts ---
    state["price_prev"], state["oi_prev"] = state["price"], state["oi"]
    state["price"], state["oi"] = new_price, new_oi

    if state["oi_prev"] == 0:
        print(f"ℹ️ [{now()}] {symbol}: Initializing option data state.", flush=True)
        return

    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0:
        return

    # --- Momentum Alert Logic ---
    analysis_result = check_momentum_trends(symbol, state, future_price_state)
    if analysis_result:
        trend_name, interpretation, trend_data = analysis_result
        
        current_time = time.time()
        last_alert_time = state.get("last_trend_alert_time", 0)
        last_alert_type = state.get("last_trend_alert_type", None)
        
        # Only alert if the trend type is new or if it's been more than 5 mins since the last alert of the same type
        if trend_name != last_alert_type or (current_time - last_alert_time) > MOMENTUM_WINDOW:
            print(f"📈 [{now()}] {symbol}: Momentum Trend Detected - {trend_name}. TRIGGERING ALERT.", flush=True)
            
            # Format the specific momentum alert message, now passing spot_price_state
            alert_msg = format_momentum_alert(symbol, trend_name, interpretation, trend_data, future_price_state)
            await send_alert(alert_msg)
            
            # Update state to prevent re-alerting immediately
            state["last_trend_alert_type"] = trend_name
            state["last_trend_alert_time"] = current_time
        else:
            # This trend is ongoing, but we've already alerted recently. Suppress.
            print(f"📈 [{now()}] {symbol}: Ongoing momentum trend '{trend_name}'. Alert suppressed.", flush=True)

async def run_scanner():
    """The main function to connect, authenticate, subscribe, and process data."""
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                print(f"✅ [{now()}] Connected to WebSocket. Authenticating...", flush=True)
                
                auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
                await websocket.send(json.dumps(auth_request))
                auth_response = json.loads(await websocket.recv())
                
                if not auth_response.get("Complete"):
                    print(f"❌ [{now()}] Authentication FAILED: {auth_response.get('Comment')}. Retrying in 30s.", flush=True)
                    await asyncio.sleep(30)
                    continue
                
                print(f"✅ [{now()}] Authentication successful. Subscribing to {len(SYMBOLS_TO_MONITOR)} symbols...", flush=True)

                for symbol in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime", "Exchange": "NFO",
                        "Unsubscribe": "false", "InstrumentIdentifier": symbol
                    }))
                print(f"✅ [{now()}] Subscriptions sent. Scanner is now live.", flush=True)
                await send_alert("✅ GFDL Scanner is LIVE and monitoring the market.")

                async for message in websocket:
                    try:
                        data = json.loads(message)
                        if data.get("MessageType") == "RealtimeResult":
                            await process_data(data)                        
                    except json.JSONDecodeError:
                        print(f"⚠️ [{now()}] Warning: Received a non-JSON message.", flush=True)
                    except Exception as e:
                        print(f"❌ [{now()}] Error during message processing for {message}: {e}", flush=True)

        except websockets.exceptions.ConnectionClosed as e:
            print(f"⚠️ [{now()}] WebSocket connection closed: {e}. Reconnecting in 10 seconds...", flush=True)
            await asyncio.sleep(10)
        except Exception as e:
            print(f"❌ [{now()}] An unexpected error occurred in the main loop: {e}. Reconnecting in 30 seconds...", flush=True)
            await asyncio.sleep(30)

if __name__ == "__main__":
    print("🚀 GFDL Scanner Starting...", flush=True)
    try:
        asyncio.run(run_scanner())
    except KeyboardInterrupt:
        print("\n🛑 Scanner stopped by user.", flush=True)
        # In a real async app, you'd await this, but for shutdown it's okay to fire and forget
        asyncio.run(send_alert("🛑 GFDL Scanner was stopped manually."))
    except Exception as e:
        error_message = f"💥 GFDL Scanner CRASHED with a critical error: {e}"
        print(error_message, flush=True)
        asyncio.run(send_alert(error_message))
