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

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- Environment Variable Loading ---
# Load credentials securely from environment variables
API_KEY = os.environ.get("API_KEY")
ULTRAMSG_INSTANCE = os.environ.get("ULTRAMSG_INSTANCE")
ULTRAMSG_TOKEN = os.environ.get("ULTRAMSG_TOKEN")
ULTRAMSG_GROUP_ID = os.environ.get("ULTRAMSG_GROUP_ID")

# --- Validation ---
# Ensure all required environment variables are set
required_vars = {
    "API_KEY": API_KEY,
    "ULTRAMSG_INSTANCE": ULTRAMSG_INSTANCE,
    "ULTRAMSG_TOKEN": ULTRAMSG_TOKEN,
    "ULTRAMSG_GROUP_ID": ULTRAMSG_GROUP_ID,
}

missing_vars = [key for key, value in required_vars.items() if value is None]

if missing_vars:
    print(f"❌ Critical Error: Missing required environment variables: {', '.join(missing_vars)}", flush=True)
    print("Please set these variables in your deployment environment and restart the application.", flush=True)
    sys.exit(1) # Exit the script with a non-zero status code to indicate failure

# --- API and Connection ---
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"

# --- WhatsApp Alerting (UltraMSG) ---
ULTRAMSG_API_URL = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"

# --- Symbol List (Options & Futures) ---
SYMBOLS_TO_MONITOR = [
    # BANKNIFTY Options
    "BANKNIFTY27JAN2659000CE", "BANKNIFTY27JAN2659000PE", "BANKNIFTY27JAN2658900CE", "BANKNIFTY27JAN2658900PE",
    "BANKNIFTY27JAN2658800CE", "BANKNIFTY27JAN2658800PE", "BANKNIFTY27JAN2658700CE", "BANKNIFTY27JAN2658700PE",
    "BANKNIFTY27JAN2658600CE", "BANKNIFTY27JAN2658600PE", "BANKNIFTY27JAN2658500CE", "BANKNIFTY27JAN2658500PE",
    "BANKNIFTY27JAN2659100CE", "BANKNIFTY27JAN2659100PE", "BANKNIFTY27JAN2659200CE", "BANKNIFTY27JAN2659200PE",
    "BANKNIFTY27JAN2659300CE", "BANKNIFTY27JAN2659300PE", "BANKNIFTY27JAN2659400CE", "BANKNIFTY27JAN2659400PE",
    "BANKNIFTY27JAN2659500CE", "BANKNIFTY27JAN2659500PE", "BANKNIFTY27JAN2658400CE", "BANKNIFTY27JAN2658400PE",
    "BANKNIFTY27JAN2658300CE", "BANKNIFTY27JAN2658300PE", "BANKNIFTY27JAN2658200CE", "BANKNIFTY27JAN2658200PE", 
    "BANKNIFTY27JAN2658100CE", "BANKNIFTY27JAN2658100PE",

    # HDFCBANK Options
    "HDFCBANK27JAN26995CE", "HDFCBANK27JAN26995PE", "HDFCBANK27JAN26990CE", "HDFCBANK27JAN26990PE",
    "HDFCBANK27JAN26985CE", "HDFCBANK27JAN26985PE", "HDFCBANK27JAN26980CE", "HDFCBANK27JAN26980PE",
    "HDFCBANK27JAN26975CE", "HDFCBANK27JAN26975PE", "HDFCBANK27JAN26970CE", "HDFCBANK27JAN26970PE",
    "HDFCBANK27JAN261000CE", "HDFCBANK27JAN261000PE", "HDFCBANK27JAN261005CE", "HDFCBANK27JAN261005PE",
    "HDFCBANK27JAN261010CE", "HDFCBANK27JAN261010PE", "HDFCBANK27JAN261015CE", "HDFCBANK27JAN261015PE",
    "HDFCBANK27JAN261020CE", "HDFCBANK27JAN261020PE",

    # ICICIBANK Options
    "ICICIBANK27JAN261350CE", "ICICIBANK27JAN261350PE", "ICICIBANK27JAN261340CE", "ICICIBANK27JAN261340PE",
    "ICICIBANK27JAN261330CE", "ICICIBANK27JAN261330PE", "ICICIBANK27JAN261320CE", "ICICIBANK27JAN261320PE",
    "ICICIBANK27JAN261310CE", "ICICIBANK27JAN261310PE", "ICICIBANK27JAN261300CE", "ICICIBANK27JAN261300PE",
    "ICICIBANK27JAN261360CE", "ICICIBANK27JAN261360PE", "ICICIBANK27JAN261370CE", "ICICIBANK27JAN261370PE",
    "ICICIBANK27JAN261380CE", "ICICIBANK27JAN261380PE", "ICICIBANK27JAN261390CE", "ICICIBANK27JAN261390PE",
    "ICICIBANK27JAN261400CE", "ICICIBANK27JAN261400PE",

    # SBIN Options
    "SBIN27JAN26960CE", "SBIN27JAN26960PE", "SBIN27JAN26955CE", "SBIN27JAN26955PE",
    "SBIN27JAN26950CE", "SBIN27JAN26950PE", "SBIN27JAN26945CE", "SBIN27JAN26945PE",
    "SBIN27JAN26940CE", "SBIN27JAN26940PE", "SBIN27JAN26935CE", "SBIN27JAN26935PE",
    "SBIN27JAN26965CE", "SBIN27JAN26965PE", "SBIN27JAN26970CE", "SBIN27JAN26970PE",
    "SBIN27JAN26975CE", "SBIN27JAN26975PE", "SBIN27JAN26980CE", "SBIN27JAN26980PE",
    "SBIN27JAN26985CE", "SBIN27JAN26985PE",
    
    # Futures
    "BANKNIFTY27JAN26FUT", "HDFCBANK27JAN26FUT", "ICICIBANK27JAN26FUT", "SBIN27JAN26FUT",
]

# --- Logic & Thresholds ---
LOT_SIZES = {
    "BANKNIFTY": 30,
    "HDFCBANK": 550,
    "ICICIBANK": 700,
    "SBIN": 750,
}
DEFAULT_LOT_SIZE = 75 # For any other symbol
OI_ROC_THRESHOLD = 3.0 # Temporarily lowered for IV testing

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================

# State now supports None for IV to track missing data
symbol_data_state = {
    symbol: {
        "price": 0, "price_prev": 0,
        "oi": 0, "oi_prev": 0,
        "iv": None, "iv_prev": None, # Initialize IV as None
    } for symbol in SYMBOLS_TO_MONITOR
}

def now():
    return datetime.now().strftime("%H:%M:%S")

async def send_whatsapp(msg: str):
    """Sends a message to the configured UltraMSG group without blocking the event loop."""
    print(f"📦 [{now()}] Preparing to send WhatsApp message...", flush=True)
    loop = asyncio.get_running_loop()
    
    params = {'token': ULTRAMSG_TOKEN, 'to': ULTRAMSG_GROUP_ID, 'body': msg, 'priority': 10}
    
    # Use functools.partial to prepare the blocking function with its arguments
    blocking_call = functools.partial(requests.post, ULTRAMSG_API_URL, params=params, timeout=10)

    try:
        # Run the blocking call in a separate thread
        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status() 
        print(f"✅ [{now()}] WhatsApp message sent successfully. Response: {response.text}", flush=True)
    except requests.exceptions.RequestException as e:
        print(f"❌ [{now()}] FAILED to send WhatsApp message: {e}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] An unexpected error occurred while sending WhatsApp message: {e}", flush=True)

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

def classify_option(oi_change, price_change, iv_change, symbol):
    is_call, is_put = "CE" in symbol, "PE" in symbol
    if oi_change > 0:
        if (is_call and price_change < 0) or (is_put and price_change > 0):
            return "Fresh Writing (High Conviction)" if iv_change < 0 else "Forced Writing / Hedging"
        elif (is_call and price_change > 0) or (is_put and price_change < 0):
            return "Strong Buying" if iv_change > 0 else "Speculative Buying"
    elif oi_change < 0:
        if (is_call and price_change > 0) or (is_put and price_change < 0):
            return "Unwinding / Position Exit" if iv_change > 0 else "Profit Booking (Writers)"
        elif (is_call and price_change < 0) or (is_put and price_change > 0):
            return "Long Liquidation" if iv_change > 0 else "Profit Booking (Buyers)"
    return "Indecisive Movement"

def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, iv_roc):
    """Formats the alert message, showing N/A for missing IV."""
    price_dir = "↑" if (state['price'] - state['price_prev']) > 0 else "↓"
    
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

    try:
        # For options, extract strike and type
        if "FUT" not in symbol:
             match = re.search(r'(\d+)(CE|PE)$', symbol)
             strike, option_type = match.groups()
        else: # For futures
             strike, option_type = "FUT", ""
    except (AttributeError, TypeError):
        # Fallback for any other format
        strike, option_type = "N/A", ""

    # Display 'N/A' if IV data is not available
    iv_display = f"{state['iv']:.2f}" if state['iv'] is not None else "N/A"
    iv_roc_display = f"{iv_roc:.2f}%" if state['iv'] is not None else "N/A"

    main_message = f"""
{product_name} | {'OPTION' if 'FUT' not in symbol else 'FUTURE'}
STRIKE: {strike}{option_type}
ACTION: {action}
SIZE: {bucket} ({lots} lots)
EXISTING OI: {state['oi_prev']}
OI Δ: {oi_chg}
OI RoC: {oi_roc:.2f}%
PRICE: {price_dir}
IV: {iv_display}
IV RoC: {iv_roc_display}
TIME: {now()}
"""

    added_section = f"""
{product_name} {strike}{option_type}

{state['price']:.2f}
"""

    return f"{main_message}\n\n{added_section}"

# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================

async def process_data(data):
    """
    Processes a single data packet, sending an alert instantly if the threshold is met.
    Handles missing IV data by displaying 'N/A'.
    """
    global symbol_data_state
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    state = symbol_data_state[symbol]

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")
    new_iv = data.get("ImpliedVolatility") # Can be None

    if new_price is None or new_oi is None:
        return

    state["price_prev"], state["oi_prev"], state["iv_prev"] = state["price"], state["oi"], state["iv"]
    state["price"], state["oi"], state["iv"] = new_price, new_oi, new_iv

    if state["oi_prev"] == 0:
        print(f"ℹ️ [{now()}] {symbol}: Initializing option data state.", flush=True)
        return

    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0:
        return

    # --- Calculations ---
    price_chg = state["price"] - state["price_prev"]
    
    # Calculate IV change and RoC, ensuring they are always numbers
    iv_chg = 0
    iv_roc = 0.0
    if state["iv"] is not None and state["iv_prev"] is not None:
        iv_chg = state["iv"] - state["iv_prev"]
        if state["iv_prev"] != 0:
            try:
                iv_roc = (iv_chg / state["iv_prev"]) * 100
            except ZeroDivisionError:
                iv_roc = 0.0
    
    # Calculate OI RoC
    try:
        oi_roc = (oi_chg / state["oi_prev"]) * 100
    except ZeroDivisionError:
        oi_roc = 0.0

    # --- Instant Alert Logic ---
    if abs(oi_roc) > OI_ROC_THRESHOLD:
        print(f"🚨 [{now()}] {symbol}: OI RoC {oi_roc:.2f}% > {OI_ROC_THRESHOLD}%. TRIGGERING ALERT.", flush=True)
        
        action = classify_option(oi_chg, price_chg, iv_chg, symbol)
        lots = lots_from_oi_change(symbol, oi_chg)
        bucket = lot_bucket(lots)
        print(f"📊 [{now()}] {symbol}: Calculated lots: {lots}, Bucket: {bucket}", flush=True)
        
        if bucket != "IGNORE":
            alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, iv_roc)
            await send_whatsapp(alert_msg)

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
                await send_whatsapp("✅ GFDL Scanner is LIVE and monitoring the market.")

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
        asyncio.run(send_whatsapp("🛑 GFDL Scanner was stopped manually."))
    except Exception as e:
        error_message = f"💥 GFDL Scanner CRASHED with a critical error: {e}"
        print(error_message, flush=True)
        asyncio.run(send_whatsapp(error_message))
