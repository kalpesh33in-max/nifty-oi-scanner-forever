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
    "BANKNIFTY24FEB2658900CE.NFO",
    "BANKNIFTY24FEB2658900PE.NFO",
    "BANKNIFTY24FEB2658800CE.NFO",
    "BANKNIFTY24FEB2658800PE.NFO",
    "BANKNIFTY24FEB2658700CE.NFO",
    "BANKNIFTY24FEB2658700PE.NFO",
    "BANKNIFTY24FEB2658600CE.NFO",
    "BANKNIFTY24FEB2658600PE.NFO",
    "BANKNIFTY24FEB2658500CE.NFO",
    "BANKNIFTY24FEB2658500PE.NFO",
    "BANKNIFTY24FEB2658400CE.NFO",
    "BANKNIFTY24FEB2658400PE.NFO",
    "BANKNIFTY24FEB2659000CE.NFO",
    "BANKNIFTY24FEB2659000PE.NFO",
    "BANKNIFTY24FEB2659100CE.NFO",
    "BANKNIFTY24FEB2659100PE.NFO",
    "BANKNIFTY24FEB2659200CE.NFO",
    "BANKNIFTY24FEB2659200PE.NFO",
    "BANKNIFTY24FEB2659300CE.NFO",
    "BANKNIFTY24FEB2659300PE.NFO",
    "BANKNIFTY24FEB2659400CE.NFO",
    "BANKNIFTY24FEB2659400PE.NFO",
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
    "SBIN24FEB261040CE.NFO",
    "SBIN24FEB261040PE.NFO",
    "SBIN24FEB261035CE.NFO",
    "SBIN24FEB261035PE.NFO",
    "SBIN24FEB261030CE.NFO",
    "SBIN24FEB261030PE.NFO",
    "SBIN24FEB261025CE.NFO",
    "SBIN24FEB261025PE.NFO",
    "SBIN24FEB261020CE.NFO",
    "SBIN24FEB261020PE.NFO",
    "SBIN24FEB261015CE.NFO",
    "SBIN24FEB261015PE.NFO",
    "SBIN24FEB261045CE.NFO",
    "SBIN24FEB261045PE.NFO",
    "SBIN24FEB261050CE.NFO",
    "SBIN24FEB261050PE.NFO",
    "SBIN24FEB261055CE.NFO",
    "SBIN24FEB261055PE.NFO",
    "SBIN24FEB261060CE.NFO",
    "SBIN24FEB261060PE.NFO",
    "SBIN24FEB261065CE.NFO",
    "SBIN24FEB261065PE.NFO",
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
    "ICICIBANK27JAN261430CE",
    "ICICIBANK27JAN261430PE",
    "BANKNIFTY-I",
    "HDFCBANK-I",
    "ICICIBANK-I",
    "SBIN-I",
    "AXISBANK-I",
    "KOTAKBANK-I"
]

# --- Logic & Thresholds ---
LOT_SIZES = {"AXISBANK": 625, "KOTAKBANK": 2000, "SBIN": 750, "ICICIBANK": 700,"HDFCBANK": 550, "BANKNIFTY": 30}
DEFAULT_LOT_SIZE = 75 # For any other symbol
OI_ROC_THRESHOLD = 2.0 # Temporarily lowered for IV testing

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================

# State now supports None for IV to track missing data
symbol_data_state = {
    symbol: {
        "price": 0, "price_prev": 0,
        "oi": 0, "oi_prev": 0,
    } for symbol in SYMBOLS_TO_MONITOR
}

# Dictionary to hold the latest price of the underlying futures
future_prices = {
    "BANKNIFTY": 0,
    "HDFCBANK": 0,
    "ICICIBANK": 0,
    "SBIN": 0,
}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

async def send_telegram(msg: str):
    """Sends a message to the configured Telegram chat without blocking the event loop."""
    print(f"📦 [{now()}] Preparing to send Telegram message...", flush=True)
    loop = asyncio.get_running_loop()
    
    params = {'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
    
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

def get_option_moneyness(symbol, future_prices):
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

    future_price = future_prices.get(underlying)
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

def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices):
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
OI RoC: {oi_roc:.2f}%
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
        future_price = future_prices.get(underlying_lookup_key, 0)
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
    Processes a single data packet, routing to the correct logic based on symbol type.
    """
    print(data) # Added for debugging
    global symbol_data_state, future_prices
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    # --- ROUTE 1: Logic for simple "-I" futures from gfdl_future.py ---
    if symbol.endswith("-I"):
        new_price = data.get("LastTradePrice")
        new_oi = data.get("OpenInterest")
        if new_price is None or new_oi is None:
            return

        state = symbol_data_state[symbol]
        if state.get("oi", 0) == 0: # Use .get for safety on first run
            state["price"], state["oi"] = new_price, new_oi
            print(f"🟢 [{now()}] {symbol}: First Data Received (P: {new_price}, OI: {new_oi})", flush=True)
            return

        oi_chg = new_oi - state["oi"]
        if abs(oi_chg) > 0:
            base_symbol = symbol.split("-")[0].upper()
            lot_size = LOT_SIZES.get(base_symbol, 75)
            lots = int(abs(oi_chg) / lot_size)

            if lots >= 50:
                try:
                    oi_roc = (oi_chg / state["oi"]) * 100
                except ZeroDivisionError:
                    oi_roc = 0.0
                
                direction = "🔺" if new_price > state["price"] else "🔻"
                # Use a simplified alert message for these futures
                msg = (f"🔔 *ALERT: {symbol}* {direction}\n"
                       f"OI Change: {oi_chg} ({lots} lots)\n"
                       f"OI RoC: {oi_roc:.2f}%\n"
                       f"Price: {new_price}")
                await send_alert(msg)
                print(f"🚀 Alert (Future): {symbol} Lot size >= 50 detected.", flush=True)
        
        state["price"], state["oi"] = new_price, new_oi
        return # End processing for "-I" futures here

    # --- ROUTE 2: Existing logic for Options and "...FUT" futures ---
    new_price = data.get("LastTradePrice")
    if new_price is None:
        return

    if "FUT" in symbol:
        underlying = None
        if "HDFCBANK" in symbol: underlying = "HDFCBANK"
        elif "ICICIBANK" in symbol: underlying = "ICICIBANK"
        elif "SBIN" in symbol: underlying = "SBIN"
        elif "BANKNIFTY" in symbol: underlying = "BANKNIFTY"
        
        if underlying and new_price > 0:
            future_prices[underlying] = new_price
        return

    # --- Standard processing for Option contracts ---
    state = symbol_data_state[symbol]
    new_oi = data.get("OpenInterest")

    if new_oi is None:
        return

    state["price_prev"], state["oi_prev"] = state.get("price", 0), state.get("oi", 0)
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

    # --- Alert Logic ---
    if abs(oi_roc) > OI_ROC_THRESHOLD:
        print(f"🚨 [{now()}] {symbol}: OI RoC {oi_roc:.2f}% > {OI_ROC_THRESHOLD}%. Potential Alert.", flush=True)
        
        lots = lots_from_oi_change(symbol, oi_chg)
        is_future = "FUT" in symbol
        
        alert_condition_met = False
        moneyness = "N/A" # Default for futures

        if is_future:
            if lots > 50:
                alert_condition_met = True
        else: # It's an option
            if lots > 100:
                alert_condition_met = True
                moneyness = get_option_moneyness(symbol, future_prices)
        
        if alert_condition_met:
            bucket = lot_bucket(lots)
            if bucket != "IGNORE":
                print(f"📊 [{now()}] {symbol}: {'Future' if is_future else moneyness}, lots: {lots}, Bucket: {bucket}. TRIGGERING ALERT.", flush=True)
                action = classify_option(oi_chg, price_chg, symbol)
                alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices)
                await send_alert(alert_msg)

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
