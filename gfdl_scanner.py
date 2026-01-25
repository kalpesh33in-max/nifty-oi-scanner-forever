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
    "BANKNIFTY24FEB2658500CE",
    "BANKNIFTY24FEB2658500PE",
    "BANKNIFTY24FEB2658400CE",
    "BANKNIFTY24FEB2658400PE",
    "BANKNIFTY24FEB2658300CE",
    "BANKNIFTY24FEB2658300PE",
    "BANKNIFTY24FEB2658200CE",
    "BANKNIFTY24FEB2658200PE",
    "BANKNIFTY24FEB2658100CE",
    "BANKNIFTY24FEB2658100PE",
    "BANKNIFTY24FEB2658000CE",
    "BANKNIFTY24FEB2658000PE",
    "BANKNIFTY24FEB2658600CE",
    "BANKNIFTY24FEB2658600PE",
    "BANKNIFTY24FEB2658700CE",
    "BANKNIFTY24FEB2658700PE",
    "BANKNIFTY24FEB2658800CE",
    "BANKNIFTY24FEB2658800PE",
    "BANKNIFTY24FEB2658900CE",
    "BANKNIFTY24FEB2658900PE",
    "BANKNIFTY24FEB2659000CE",
    "BANKNIFTY24FEB2659000PE",
    "HDFCBANK24FEB26915CE",
    "HDFCBANK24FEB26915PE",
    "HDFCBANK24FEB26910CE",
    "HDFCBANK24FEB26910PE",
    "HDFCBANK24FEB26905CE",
    "HDFCBANK24FEB26905PE",
    "HDFCBANK24FEB26900CE",
    "HDFCBANK24FEB26900PE",
    "HDFCBANK24FEB26895CE",
    "HDFCBANK24FEB26895PE",
    "HDFCBANK24FEB26890CE",
    "HDFCBANK24FEB26890PE",
    "HDFCBANK24FEB26920CE",
    "HDFCBANK24FEB26920PE",
    "HDFCBANK24FEB26925CE",
    "HDFCBANK24FEB26925PE",
    "HDFCBANK24FEB26930CE",
    "HDFCBANK24FEB26930PE",
    "HDFCBANK24FEB26935CE",
    "HDFCBANK24FEB26935PE",
    "HDFCBANK24FEB26940CE",
    "HDFCBANK24FEB26940PE",
    "ICICIBANK24FEB261340CE",
    "ICICIBANK24FEB261340PE",
    "ICICIBANK24FEB261330CE",
    "ICICIBANK24FEB261330PE",
    "ICICIBANK24FEB261320CE",
    "ICICIBANK24FEB261320PE",
    "ICICIBANK24FEB261310CE",
    "ICICIBANK24FEB261310PE",
    "ICICIBANK24FEB261300CE",
    "ICICIBANK24FEB261300PE",
    "ICICIBANK24FEB261290CE",
    "ICICIBANK24FEB261290PE",
    "ICICIBANK24FEB261350CE",
    "ICICIBANK24FEB261350PE",
    "ICICIBANK24FEB261360CE",
    "ICICIBANK24FEB261360PE",
    "ICICIBANK24FEB261370CE",
    "ICICIBANK24FEB261370PE",
    "ICICIBANK24FEB261380CE",
    "ICICIBANK24FEB261380PE",
    "ICICIBANK24FEB261390CE",
    "ICICIBANK24FEB261390PE",
    "SBIN24FEB261030CE",
    "SBIN24FEB261030PE",
    "SBIN24FEB261025CE",
    "SBIN24FEB261025PE",
    "SBIN24FEB261020CE",
    "SBIN24FEB261020PE",
    "SBIN24FEB261015CE",
    "SBIN24FEB261015PE",
    "SBIN24FEB261010CE",
    "SBIN24FEB261010PE",
    "SBIN24FEB261005CE",
    "SBIN24FEB261005PE",
    "SBIN24FEB261035CE",
    "SBIN24FEB261035PE",
    "SBIN24FEB261040CE",
    "SBIN24FEB261040PE",
    "SBIN24FEB261045CE",
    "SBIN24FEB261045PE",
    "SBIN24FEB261050CE",
    "SBIN24FEB261050PE",
    "SBIN24FEB261055CE",
    "SBIN24FEB261055PE",
    "BANKNIFTY27JAN26FUT",
    "HDFCBANK27JAN26FUT",
    "ICICIBANK27JAN26FUT",
    "SBIN27JAN26FUT",
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
MOMENTUM_WINDOW = 180 # 3 minutes in seconds

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
    Returns a string: "ITM", "ATM", or "OTM".
    """
    # This filter does not apply to future contracts themselves
    if "FUT" in symbol:
        return "N/A"

    # Identify underlying and get its future price
    underlying = None
    if "HDFCBANK" in symbol: underlying = "HDFCBANK"
    elif "ICICIBANK" in symbol: underlying = "ICICIBANK"
    elif "SBIN" in symbol: underlying = "SBIN"
    elif "BANKNIFTY" in symbol: underlying = "BANKNIFTY"

    if not underlying: 
        return "N/A" # If it's not one of our known underlyings, don't block it

    future_price = future_price_state.get(underlying)
    if not future_price or future_price == 0:
        print(f"⏳ [{now()}] {symbol}: Waiting for future price of {underlying} to check moneyness.", flush=True)
        return "OTM" # Treat as OTM if we don't have the future price yet

    # Extract strike and type
    try:
        match = re.search(r'.*?(\d{2})(\d+)(CE|PE)$', symbol)
        strike_price = int(match.group(2))
        option_type = match.group(3)
    except (AttributeError, TypeError, ValueError):
        return "N/A" # If we can't parse the option, don't block it

    # Define ATM band (0.5% of future price)
    atm_band = future_price * 0.001
    
    # Check ATM first
    if abs(future_price - strike_price) <= atm_band:
        return "ATM"
    
    # Check ITM
    is_itm = False
    if option_type == 'CE' and strike_price < future_price:
        is_itm = True
    if option_type == 'PE' and strike_price > future_price:
        is_itm = True

    if is_itm:
        return "ITM"
    else:
        # If not ATM and not ITM, it must be OTM
        print(f"ℹ️ [{now()}] {symbol}: OTM (Future: {future_price:.2f}, Strike: {strike_price}), alert suppressed.", flush=True)
        return "OTM"

def calculate_future_price_score(ticks):
    """Calculates the Future Price Score based on trend direction."""
    if len(ticks) < 10:  # Need enough data to determine a trend
        return 0, "Not Enough Data"

    prices = [tick[1] for tick in ticks]
    start_price = prices[0]
    end_price = prices[-1]
    price_change = end_price - start_price
    
    # Simple trend detection
    if price_change > 0:
        # Simplified: Treat all upward movement as "Price ↑ but choppy" for now
        return 25, "Price ↑"
    elif price_change < 0:
        # Simplified: Treat all downward movement as "Price ↓ but choppy" for now
        return -25, "Price ↓"
    else:
        return 0, "Flat"

def calculate_option_oi_score(symbol_data_state, underlying):
    """
    Calculates the Option OI Score based on aggregate call and put activity.
    """
    total_ce_oi_chg = 0
    total_pe_oi_chg = 0
    ce_price_chg_sum = 0
    pe_price_chg_sum = 0
    ce_count = 0
    pe_count = 0

    for symbol, state in symbol_data_state.items():
        if underlying not in symbol:
            continue

        if not state.get('ticks'):
            continue
        
        # Ensure the ticks are within the momentum window
        now = time.time()
        relevant_ticks = [t for t in state['ticks'] if now - t[0] <= MOMENTUM_WINDOW]
        if len(relevant_ticks) < 2:
            continue

        start_price = relevant_ticks[0][1]
        start_oi = relevant_ticks[0][2]
        end_price = relevant_ticks[-1][1]
        end_oi = relevant_ticks[-1][2]
        
        oi_chg = end_oi - start_oi
        price_chg = end_price - start_price

        if "CE" in symbol:
            total_ce_oi_chg += oi_chg
            ce_price_chg_sum += price_chg
            ce_count += 1
        elif "PE" in symbol:
            total_pe_oi_chg += oi_chg
            pe_price_chg_sum += price_chg
            pe_count += 1
    
    avg_ce_price_chg = ce_price_chg_sum / ce_count if ce_count > 0 else 0
    avg_pe_price_chg = pe_price_chg_sum / pe_count if pe_count > 0 else 0

    # Scoring logic from score.pdf
    if total_pe_oi_chg > 0 and avg_pe_price_chg < 0:
        return 20, "Put Writing"
    if total_ce_oi_chg > 0 and avg_ce_price_chg < 0:
        return -20, "Call Writing"
    if total_ce_oi_chg < 0 and avg_ce_price_chg > 0:
        return 15, "Call Short Covering"
    if total_pe_oi_chg < 0 and avg_pe_price_chg > 0:
        return -15, "Put Short Covering"
    
    # Check for both side writing
    if total_ce_oi_chg > 0 and total_pe_oi_chg > 0:
        return 0, "Both Side Writing"

    return 0, "Neutral OI Activity"

def calculate_option_price_score(symbol_data_state, underlying):
    """
    Calculates the Option Price Score based on how fast prices are moving.
    """
    ce_price_chg_pct_sum = 0
    pe_price_chg_pct_sum = 0
    ce_count = 0
    pe_count = 0
    FAST_THRESHOLD = 0.10  # 10% change is considered "fast"

    for symbol, state in symbol_data_state.items():
        if underlying not in symbol:
            continue
        
        if not state.get('ticks'):
            continue

        now = time.time()
        relevant_ticks = [t for t in state['ticks'] if now - t[0] <= MOMENTUM_WINDOW]
        if len(relevant_ticks) < 2:
            continue

        start_price = relevant_ticks[0][1]
        end_price = relevant_ticks[-1][1]

        if start_price == 0:
            continue

        price_chg_pct = (end_price - start_price) / start_price

        if "CE" in symbol:
            ce_price_chg_pct_sum += price_chg_pct
            ce_count += 1
        elif "PE" in symbol:
            pe_price_chg_pct_sum += price_chg_pct
            pe_count += 1
            
    avg_ce_price_chg_pct = ce_price_chg_pct_sum / ce_count if ce_count > 0 else 0
    avg_pe_price_chg_pct = pe_price_chg_pct_sum / pe_count if pe_count > 0 else 0

    if avg_ce_price_chg_pct > FAST_THRESHOLD:
        return 15, "CE Price ↑ Fast"
    if avg_pe_price_chg_pct > FAST_THRESHOLD:
        return -15, "PE Price ↑ Fast"
    
    # Check for premium decay
    if avg_ce_price_chg_pct < 0 and avg_pe_price_chg_pct < 0:
        return 0, "Premium Decay"

    return 0, "Neutral Price Activity"

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

    if abs(lots_from_oi_change(symbol, oi_chg)) <= 300: return None
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
        trend_name = "📈 Case-2: Breakout Bullish"
        interpretation = "Call writers trapped -> Fast upside move"
    elif is_put and future_up and price_down and oi_up:
        trend_name = "📈 Case-3: Support-based Bullish"
        interpretation = "Put writing -> Strong support below"
        
    # Bearish Cases
    elif is_put and future_down and price_down and oi_up:
        trend_name = "📉 Case-4: Strong Bearish (Smart money)"
        interpretation = "Put writing + downside control -> Slow grind down"
    elif is_put and future_down and price_up and oi_down:
        trend_name = "📉 Case-5: Breakdown Bearish"
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

def get_market_trend_score(underlying, symbol_data_state, future_price_state):
    """Calculates the total market trend score for a given underlying."""
    
    future_ticks = future_price_state.get(underlying, {}).get("ticks", [])
    
    future_price_score, future_price_desc = calculate_future_price_score(future_ticks)
    option_oi_score, option_oi_desc = calculate_option_oi_score(symbol_data_state, underlying)
    option_price_score, option_price_desc = calculate_option_price_score(symbol_data_state, underlying)

    final_score = future_price_score + option_oi_score + option_price_score
    
    score_meaning = "No Trend"
    if 45 <= final_score <= 75:
        score_meaning = "Strong Bullish"
    elif 23 <= final_score <= 44:
        score_meaning = "Weak Bullish"
    elif -22 <= final_score <= 22:
        score_meaning = "No Trend"
    elif -44 <= final_score <= -23:
        score_meaning = "Weak Bearish"
    elif -75 <= final_score <= -45:
        score_meaning = "Strong Bearish"

    return final_score, score_meaning

def format_momentum_alert(symbol, trend_name, interpretation, data, trend_score_details):
    """
    Formats the 3-minute momentum alert message, including the market trend score.
    """
    product_name, strike_display, option_type, year = "UNKNOWN", "N/A", "", ""
    if "HDFCBANK" in symbol: product_name = "HDFCBANK"
    elif "ICICIBANK" in symbol: product_name = "ICICI"
    elif "SBIN" in symbol: product_name = "SBIN"
    elif "BANKNIFTY" in symbol: product_name = "BANKNIFTY"
    
    try:
        match = re.search(r'.*?(\d{2})(\d+)(CE|PE)$', symbol)
        if match: year, strike_display, option_type = match.groups()
    except Exception: pass

    lots = lots_from_oi_change(symbol, data['oi_chg'])
    try:
        oi_roc = (data['oi_chg'] / data['start_oi']) * 100
    except ZeroDivisionError:
        oi_roc = 0.0

    # --- Unpack Score ---
    trend_score, trend_meaning = trend_score_details

    # --- Movement Indicators ---
    future_dir = "↑" if data['future_price_chg'] > 0 else "↓" if data['future_price_chg'] < 0 else "↔"
    option_dir = "↑" if data['option_price_chg'] > 0 else "↓" if data['option_price_chg'] < 0 else "↔"
    oi_dir = "↑" if data['oi_chg'] > 0 else "↓"

    # --- Formatting ---
    header = "- - - 3-Min Momentum Alert - - -"
    line1 = f"{product_name} | {strike_display}{option_type}"
    line2 = f"\n{trend_name}\nInterpretation: {interpretation}\n"
    score_line = f"Market Trend Score: {trend_score} ({trend_meaning})"
    
    analysis_header = "--- Analysis Breakdown ---"
    future_line = f"Future Price:   {future_dir} ({data['future_price_chg']:+.2f})"
    option_line = f"Option Price:   {option_dir} ({data['option_price_chg']:+.2f})"
    oi_line =     f"Option OI:      {oi_dir} ({data['oi_chg']:+,.0f})"
    
    data_header = "\n--- Data Points ---"
    oi_delta_line = f"OI Δ: {data['oi_chg']:+,.0f} ({lots} lots)"
    oi_roc_line = f"OI RoC: {oi_roc:+.2f}%"
    last_option_price_line = f"Last Option Price: {data['end_option_price']:.2f}"
    last_future_price_line = f"Last Future Price: {data['end_future_price']:.2f}"
    
    duration_minutes = int(data['duration'] // 60)
    duration_seconds = int(data['duration'] % 60)
    start_time_str = datetime.fromtimestamp(data['start_time']).strftime('%H:%M')
    end_time_str = datetime.fromtimestamp(data['end_time']).strftime('%H:%M')
    duration_line = f"Duration: {duration_minutes}m {duration_seconds}s ({start_time_str} -> {end_time_str})"
    
    footer = "- - - - - - - - - - - - - - - -"

    return "\n".join([
        header, line1, line2, score_line, analysis_header, future_line, option_line, oi_line,
        data_header, oi_delta_line, oi_roc_line, last_option_price_line,
        last_future_price_line, duration_line, footer
    ])


def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_price_state):
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
        future_price = future_price_state.get(underlying_lookup_key, 0)
        line1 = f"{product_name} | OPTION"
        line2 = f"STRIKE: {strike_display}{option_type_display} {moneyness}"
        line3 = f"ACTION: {action}"
        line4 = f"SIZE: {bucket} ({lots} lots)"
        line5 = f"EXISTING OI: {state['oi_prev']}"
        line6 = f"OI Δ: {oi_chg}"
        line7 = f"OI RoC: {oi_roc:.2f}%"
        line8 = f"PRICE: {price_dir}"
        line9 = f"TIME: {now()}"
        line10 = f"{year} {product_name} {strike_display}{option_type_display}"
        line11 = f"FUTURE PRICE: {future_price:.2f}"
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
    if new_price is None:
        return

    # If the symbol is a future, update its price history and stop processing for alerts
    if "FUT" in symbol:
        underlying = next((name for name in future_price_state if name in symbol), None)
        
        if underlying and new_price > 0: # Ensure price is valid
            current_time = time.time()
            future_state = future_price_state[underlying]
            future_state["ticks"].append((current_time, new_price))
            # Prune old data points
            future_state["ticks"] = [tick for tick in future_state["ticks"] if current_time - tick[0] <= MOMENTUM_WINDOW]
        return # Stop processing here for futures

    # The rest of the function will only execute for OPTIONS
    state = symbol_data_state[symbol]
    new_oi = data.get("OpenInterest")

    if new_oi is None:
        return

    # --- History Management for Momentum Alerts ---
    current_time = time.time()
    # Add the new data point to the history if it's valid
    if new_price > 0 and new_oi > 0:
        state["ticks"].append((current_time, new_price, new_oi))
    # Prune old data points from the history
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

    # --- Calculations ---
    price_chg = state["price"] - state["price_prev"]
    
    try:
        oi_roc = (oi_chg / state["oi_prev"]) * 100
    except ZeroDivisionError:
        oi_roc = 0.0

    # --- Standard OI Spike Alert ---
    lots = lots_from_oi_change(symbol, oi_chg)
    if lots >= 200:
        # Calculate moneyness just for the alert message, not for filtering
        moneyness = get_option_moneyness(symbol, future_price_state)
        bucket = lot_bucket(lots)
        
        print(f"📊 [{now()}] {symbol}: Spike Detected ({lots} lots). TRIGGERING ALERT.", flush=True)
        
        action = classify_option(oi_chg, price_chg, symbol)
        alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_price_state)
        await send_alert(alert_msg)

    # --- Momentum Alert Logic ---
    # This is called on every tick to check for a developing trend.
    analysis_result = check_momentum_trends(symbol, state, future_price_state)
    if analysis_result:
        trend_name, interpretation, trend_data = analysis_result
        
        current_time = time.time()
        last_alert_time = state.get("last_trend_alert_time", 0)
        last_alert_type = state.get("last_trend_alert_type", None)
        
        # Only alert if the trend type is new or if it's been more than 3 mins since the last alert of the same type
        if trend_name != last_alert_type or (current_time - last_alert_time) > MOMENTUM_WINDOW:
            print(f"📈 [{now()}] {symbol}: Momentum Trend Detected - {trend_name}. TRIGGERING ALERT.", flush=True)
            
            # --- Calculate Market Trend Score ---
            underlying = next((name for name in future_price_state if name in symbol), None)
            if underlying:
                trend_score_details = get_market_trend_score(underlying, symbol_data_state, future_price_state)
            else:
                trend_score_details = (0, "Unknown")

            # Format the specific momentum alert message
            alert_msg = format_momentum_alert(symbol, trend_name, interpretation, trend_data, trend_score_details)
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
