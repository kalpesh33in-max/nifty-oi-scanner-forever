import asyncio
import websockets
import json
import time
import requests
import sys
from datetime import datetime
import re
import functools
import os

# =============================================================================
# ============================== CONFIGURATION ================================
# =============================================================================

# --- API and Connection ---
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
API_KEY = os.environ.get("API_KEY")

# --- WhatsApp Alerting (UltraMSG) ---
ULTRAMSG_INSTANCE = os.environ.get("ULTRAMSG_INSTANCE")
ULTRAMSG_TOKEN = os.environ.get("ULTRAMSG_TOKEN")
ULTRAMSG_GROUP_ID = os.environ.get("ULTRAMSG_GROUP_ID")
ULTRAMSG_API_URL = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"

# --- Symbol List (Options Only) ---
SYMBOLS_TO_MONITOR = [
    "NIFTY30DEC2526200CE", "NIFTY30DEC2526200PE",
    "NIFTY30DEC2526150CE", "NIFTY30DEC2526150PE",
    "NIFTY30DEC2526100CE", "NIFTY30DEC2526100PE",
    "NIFTY30DEC2526050CE", "NIFTY30DEC2526050PE",
    "NIFTY30DEC2526000CE", "NIFTY30DEC2526000PE",
    "NIFTY30DEC2525950CE", "NIFTY30DEC2525950PE",
    "NIFTY30DEC2526250CE", "NIFTY30DEC2526250PE",
    "NIFTY30DEC2526300CE", "NIFTY30DEC2526300PE",
    "NIFTY30DEC2526350CE", "NIFTY30DEC2526350PE",
    "NIFTY30DEC2526400CE", "NIFTY30DEC2526400PE",
    "NIFTY30DEC2526450CE", "NIFTY30DEC2526450PE",
    "BANKNIFTY30DEC2559300CE", "BANKNIFTY30DEC2559300PE",
    "BANKNIFTY30DEC2559200CE", "BANKNIFTY30DEC2559200PE",
    "BANKNIFTY30DEC2559100CE", "BANKNIFTY30DEC2559100PE",
    "BANKNIFTY30DEC2559000CE", "BANKNIFTY30DEC2559000PE",
    "BANKNIFTY30DEC2558900CE", "BANKNIFTY30DEC2558900PE",
    "BANKNIFTY30DEC2558800CE", "BANKNIFTY30DEC2558800PE",
    "BANKNIFTY30DEC2559400CE", "BANKNIFTY30DEC2559400PE",
    "BANKNIFTY30DEC255950CE", "BANKNIFTY30DEC255950PE",
    "BANKNIFTY30DEC2559600CE", "BANKNIFTY30DEC2559600PE",
    "BANKNIFTY30DEC2559700CE", "BANKNIFTY30DEC2559700PE",
    "BANKNIFTY30DEC2559800CE", "BANKNIFTY30DEC2559800PE",
    "HDFCBANK30DEC25995CE", "HDFCBANK30DEC25995PE",
    "HDFCBANK30DEC25990CE", "HDFCBANK30DEC25990PE",
    "HDFCBANK30DEC25985CE", "HDFCBANK30DEC25985PE",
    "HDFCBANK30DEC25980CE", "HDFCBANK30DEC25980PE",
    "HDFCBANK30DEC25975CE", "HDFCBANK30DEC25975PE",
    "HDFCBANK30DEC25970CE", "HDFCBANK30DEC25970PE",
    "HDFCBANK30DEC251000CE", "HDFCBANK30DEC251000PE",
    "HDFCBANK30DEC251005CE", "HDFCBANK30DEC251005PE",
    "HDFCBANK30DEC251010CE", "HDFCBANK30DEC251010PE",
    "HDFCBANK30DEC251015CE", "HDFCBANK30DEC251015PE",
    "HDFCBANK30DEC251020CE", "HDFCBANK30DEC251020PE",
    "RELIANCE30DEC251570CE", "RELIANCE30DEC251570PE",
    "RELIANCE30DEC251560CE", "RELIANCE30DEC251560PE",
    "RELIANCE30DEC251550CE", "RELIANCE30DEC251550PE",
    "RELIANCE30DEC251540CE", "RELIANCE30DEC251540PE",
    "RELIANCE30DEC251530CE", "RELIANCE30DEC251530PE",
    "RELIANCE30DEC251520CE", "RELIANCE30DEC251520PE",
    "RELIANCE30DEC251580CE", "RELIANCE30DEC251580PE",
    "RELIANCE30DEC251590CE", "RELIANCE30DEC251590PE",
    "RELIANCE30DEC251600CE", "RELIANCE30DEC251600PE",
    "RELIANCE30DEC251610CE", "RELIANCE30DEC251610PE",
    "RELIANCE30DEC251620CE", "RELIANCE30DEC251620PE",
]

# --- Logic & Thresholds ---
BANKNIFTY_LOT = 35
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
    print(f"📦 [{now()}] Preparing to send WhatsApp message...")
    loop = asyncio.get_running_loop()
    
    params = {'token': ULTRAMSG_TOKEN, 'to': ULTRAMSG_GROUP_ID, 'body': msg, 'priority': 10}
    
    # Use functools.partial to prepare the blocking function with its arguments
    blocking_call = functools.partial(requests.post, ULTRAMSG_API_URL, params=params, timeout=10)

    try:
        # Run the blocking call in a separate thread
        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status() 
        print(f"✅ [{now()}] WhatsApp message sent successfully. Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ [{now()}] FAILED to send WhatsApp message: {e}")
    except Exception as e:
        print(f"❌ [{now()}] An unexpected error occurred while sending WhatsApp message: {e}")
# ==============================================================================
# =============================== CORE LOGIC ===================================
# ==============================================================================

def lots_from_oi_change(symbol, oi_change):
    """Calculates the number of lots from a change in Open Interest."""
    lot_size = 75 # Default for NIFTY
    if "BANKNIFTY" in symbol:
        lot_size = BANKNIFTY_LOT # 35
    elif "RELIANCE" in symbol:
        lot_size = 500
    elif "HDFCBANK" in symbol:
        lot_size = 550
    
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
    product_name = "BANKNIFTY"

    try:
        match = re.search(r'(\d+)(CE|PE)$', symbol)
        strike, option_type = match.groups()
    except (AttributeError, TypeError):
        strike, option_type = "N/A", ""

    # Display 'N/A' if IV data is not available
    iv_display = f"{state['iv']:.2f}" if state['iv'] is not None else "N/A"
    iv_roc_display = f"{iv_roc:.2f}%" if state['iv'] is not None else "N/A"

    main_message = f"""
{product_name} | OPTION
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
{product_name} {strike} {option_type}

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
        print(f"ℹ️ [{now()}] {symbol}: Initializing option data state.")
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
        print(f"🚨 [{now()}] {symbol}: OI RoC {oi_roc:.2f}% > {OI_ROC_THRESHOLD}%. TRIGGERING ALERT.")
        
        action = classify_option(oi_chg, price_chg, iv_chg, symbol)
        lots = lots_from_oi_change(symbol, oi_chg)
        bucket = lot_bucket(lots)
        print(f"📊 [{now()}] {symbol}: Calculated lots: {lots}, Bucket: {bucket}")
        
        if bucket != "IGNORE":
            alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, iv_roc)
            await send_whatsapp(alert_msg)

async def run_scanner():
    """The main function to connect, authenticate, subscribe, and process data."""
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                print(f"✅ [{now()}] Connected to WebSocket. Authenticating...")
                
                auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
                await websocket.send(json.dumps(auth_request))
                auth_response = json.loads(await websocket.recv())
                
                if not auth_response.get("Complete"):
                    print(f"❌ [{now()}] Authentication FAILED: {auth_response.get('Comment')}. Retrying in 30s.")
                    await asyncio.sleep(30)
                    continue
                
                print(f"✅ [{now()}] Authentication successful. Subscribing to {len(SYMBOLS_TO_MONITOR)} symbols...")

                for symbol in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime", "Exchange": "NFO",
                        "Unsubscribe": "false", "InstrumentIdentifier": symbol
                    }))
                print(f"✅ [{now()}] Subscriptions sent. Scanner is now live.")
                await send_whatsapp("✅ GFDL Scanner is LIVE and monitoring the market.")

                async for message in websocket:
                    try:
                        data = json.loads(message)
                        if data.get("MessageType") == "RealtimeResult":
                            await process_data(data)                        
                    except json.JSONDecodeError:
                        print(f"⚠️ [{now()}] Warning: Received a non-JSON message.")
                    except Exception as e:
                        print(f"❌ [{now()}] Error during message processing for {message}: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"⚠️ [{now()}] WebSocket connection closed: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"❌ [{now()}] An unexpected error occurred in the main loop: {e}. Reconnecting in 30 seconds...")
            await asyncio.sleep(30)

if __name__ == "__main__":
    print("GFDL Scanner Starting...")
    # In a real async app, you'd await this, but for shutdown it's okay to fire and forget
    asyncio.run(send_whatsapp("GFDL Scanner Starting..."))
    try:
        asyncio.run(run_scanner())
    except KeyboardInterrupt:
        print("\n🛑 Scanner stopped by user.")
        # In a real async app, you'd await this, but for shutdown it's okay to fire and forget
        asyncio.run(send_whatsapp("🛑 GFDL Scanner was stopped manually."))
    except Exception as e:
        error_message = f"💥 GFDL Scanner CRASHED with a critical error: {e}"
        print(error_message)
        asyncio.run(send_whatsapp(error_message))
