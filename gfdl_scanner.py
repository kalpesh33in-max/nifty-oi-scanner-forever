import asyncio
import websockets
import json
import time
import requests
from datetime import datetime
import re
import os

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- API and Connection ---
WSS_URL = "wss://test.lisuns.com:4576/"
API_KEY = os.environ.get("API_KEY")

# --- WhatsApp Alerting (UltraMSG) ---
ULTRAMSG_INSTANCE = os.environ.get("ULTRAMSG_INSTANCE")
ULTRAMSG_TOKEN = os.environ.get("ULTRAMSG_TOKEN")
ULTRAMSG_GROUP_ID = os.environ.get("ULTRAMSG_GROUP_ID")
ULTRAMSG_API_URL = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"

# Check for missing environment variables
if not all([API_KEY, ULTRAMSG_INSTANCE, ULTRAMSG_TOKEN, ULTRAMSG_GROUP_ID]):
    print("FATAL ERROR: One or more environment variables are not set.")
    print("Please set API_KEY, ULTRAMSG_INSTANCE, ULTRAMSG_TOKEN, and ULTRAMSG_GROUP_ID.")
    exit()

# Debug flags
# Set to True to print Echo (heartbeat) messages. You can also enable
# at runtime by passing `--debug-echo` on the command line.
DEBUG_ECHO = False

# --- Symbol List ---
# List of all symbols to monitor
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
    "BANKNIFTY30DEC2559500CE", "BANKNIFTY30DEC2559500PE",
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
    "BANKNIFTY30DEC25FUT", "NIFTY30DEC25FUT",
    "RELIANCE30DEC25FUT", "HDFCBANK30DEC25FUT",
]

# --- Logic & Thresholds ---
LOT_SIZES_MAP = {
    "NIFTY": 75,
    "BANKNIFTY": 35,
    "RELIANCE": 500,
    "HDFCBANK": 550,
}
GENERIC_LOT_SIZE = 1 # Fallback for symbols not in LOT_SIZES_MAP

WRITER_THRESHOLDS = {
    "LOW": 50, "MEDIUM": 75, "HIGH": 100, "EXTRA HIGH": 150, "EXTREME HIGH": 200,
}
BUYER_ALERT_LEVELS = {"HIGH", "EXTRA HIGH", "EXTREME HIGH"}

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================

# Dictionary to hold the current and previous state for each symbol
# Structure: { "symbol_name": { "price": 0, "price_prev": 0, "oi": 0, "oi_prev": 0 } }
symbol_data_state = {symbol: {"price": 0, "price_prev": 0, "oi": 0, "oi_prev": 0} for symbol in SYMBOLS_TO_MONITOR}

def now():
    return datetime.now().strftime("%H:%M:%S")

def send_whatsapp(msg: str):
    """Sends a message to the configured UltraMSG group."""
    params = {
        'token': ULTRAMSG_TOKEN,
        'to': ULTRAMSG_GROUP_ID,
        'body': msg,
        'priority': 10
    }
    try:
        response = requests.post(ULTRAMSG_API_URL, params=params, timeout=10)
        response.raise_for_status() 
        print(f"WA: Message sent. Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"WA ERROR: {e}")
    except Exception as e:
        print(f"WA UNEXPECTED ERROR: {e}")

# ==============================================================================
# =============================== CORE LOGIC ===================================
# ==============================================================================

def lots_from_oi_change(symbol, oi_change):
    lot_size = GENERIC_LOT_SIZE # Default
    # Extract product name from symbol (e.g., NIFTY, BANKNIFTY, RELIANCE)
    product = ""
    if "NIFTY" in symbol:
        product = "NIFTY"
    elif "BANKNIFTY" in symbol:
        product = "BANKNIFTY"
    elif "RELIANCE" in symbol:
        product = "RELIANCE"
    elif "HDFCBANK" in symbol:
        product = "HDFCBANK"
    
    if product in LOT_SIZES_MAP:
        lot_size = LOT_SIZES_MAP[product]
        
    if lot_size == 0: return 0
    return int(abs(oi_change) / lot_size)

def lot_bucket(lots):
    if lots >= 200: return "EXTREME HIGH"
    if lots >= 150: return "EXTRA HIGH"
    if lots >= 100: return "HIGH"
    if lots >= 75: return "MEDIUM"
    if lots >= 50: return "LOW"
    return "IGNORE"

def classify_option(oi_change, price_change, symbol):
    """
    Classifies option activity based on OI and Price changes.
    """
    is_call = "CE" in symbol
    
    if oi_change > 0:
        if (is_call and price_change > 0) or (not is_call and price_change < 0):
            return "Fresh Buying"
        elif (is_call and price_change < 0) or (not is_call and price_change > 0):
            return "Fresh Writing"
    elif oi_change < 0:
        if (is_call and price_change > 0) or (not is_call and price_change < 0):
            return "Short Covering"
        elif (is_call and price_change < 0) or (not is_call and price_change > 0):
            return "Long Unwinding"

    return "Indecisive Movement"

def classify_future(oi_change, price_change):
    if oi_change > 0 and price_change > 0: return "FUTURE BUYING"
    if oi_change > 0 and price_change < 0: return "FUTURE SHORT BUILDUP"
    if oi_change < 0 and price_change > 0: return "FUTURE SHORT COVER"
    if oi_change < 0 and price_change < 0: return "FUTURE LONG EXIT"
    return "NO CLEAR FUTURE ACTION"

def option_alert(symbol, action, bucket, lots, existing_oi, oi_chg, oi_roc, price_dir, price):
    """
    Formats the alert message for options.
    """
    # Extract Strike and Type from symbol name for better alerts
    try:
        product = "BANKNIFTY" if "BANKNIFTY" in symbol else "NIFTY"
        
        if "CE" in symbol:
            strike = symbol.split("CE")[0][-5:]
            option_type = "CE"
        elif "PE" in symbol:
            strike = symbol.split("PE")[0][-5:]
            option_type = "PE"
        else:
            strike = "N/A"
            option_type = "N/A"
    except Exception:
        strike = "N/A"
        option_type = "N/A"
        product = symbol

    # Extract Expiry Date
    expiry_date = "N/A"
    try:
        if product != "N/A":
            # Search for a pattern like "DDMMMYY" where MMM is a month abbreviation
            # Start search after the product name
            match = re.search(r'(\d{2}[A-Z]{3}\d{2})', symbol[len(product):])
            if match:
                expiry_date = match.group(1)
    except Exception:
        expiry_date = "N/A" # Ensure expiry_date is defined on exception

    main_message = f"""
{product} | OPTION
EXPIRY: {expiry_date}
STRIKE: {strike}{option_type}
ACTION: {action}
SIZE: {bucket} ({lots} lots)
EXISTING OI: {existing_oi}
OI Δ: {oi_chg}
OI RoC: {oi_roc:.2f}%
PRICE: {price_dir} {price:.2f}
TIME: {now()}
""".strip()

    # As per user request, add a new section with symbol parts (including expiry)
    added_section = f"""
{product} {expiry_date} {strike} {option_type}
""".strip()

    return f"{main_message}\n\n{added_section}"

def future_alert(symbol, action, bucket, lots, oi_chg, oi_roc):
    product = symbol.split('.')[0]
    return f"""
{product} | FUTURE
ACTION: {action}
SIZE: {bucket} ({lots} lots)
OI Δ: {oi_chg}
OI RoC: {oi_roc:.2f}%
TIME: {now()}
"""

def combo_alert(option_msg, future_msg):
    return f"""
🔥 STRONG COMBO CONFIRMATION 🔥

{option_msg}

{future_msg}
"""

# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================

async def process_data(data):
    """Processes a single data packet from the WebSocket."""
    global symbol_data_state
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return # Not a symbol we are monitoring

    # Update state
    state = symbol_data_state[symbol]
    state["price_prev"] = state["price"]
    state["oi_prev"] = state["oi"]
    state["price"] = data.get("LastTradePrice", state["price"])
    state["oi"] = data.get("OpenInterest", state["oi"])

    # Wait for at least one previous tick to have data
    if state["price_prev"] == 0 or state["oi_prev"] == 0:
        print(f"[{now()}] Initializing data for {symbol}...")
        return

    # Calculate changes
    price_chg = state["price"] - state["price_prev"]
    oi_chg = state["oi"] - state["oi_prev"]

    # If no change, do nothing
    if price_chg == 0 and oi_chg == 0:
        return

    oi_roc = (oi_chg / state["oi_prev"] * 100) if state["oi_prev"] else 0
    lots = lots_from_oi_change(symbol, oi_chg)
    bucket = lot_bucket(lots)

    if bucket == "IGNORE":
        return

    is_future = "FUT" in symbol
    action = ""

    if is_future:
        # For futures, we only process the action for combo alerts, no individual WhatsApp messages.
        action = classify_future(oi_chg, price_chg)
        print(f"INFO: Future activity detected: {action} on {symbol}")
    else: # It's an option
        action = classify_option(oi_chg, price_chg, symbol)
        price_dir = "↑" if price_chg > 0 else "↓"
        alert_msg = option_alert(
            symbol, action, bucket, lots, 
            state["oi_prev"], oi_chg, oi_roc, price_dir, state["price"]
        )
        
        # Determine the current OI RoC threshold based on the instrument
        current_roc_threshold = 3.0 # Default threshold for most instruments

        if "NIFTY" in symbol:
            current_roc_threshold = 5.0
        # BANKNIFTY, RELIANCE, HDFCBANK will use the default 3% threshold.

        # Send WhatsApp alert ONLY if OI RoC is high (above current_roc_threshold)
        if oi_roc > current_roc_threshold:
            print(f"ALERT: Triggered for {symbol}. OI ROC: {oi_roc:.2f}%. Sending WhatsApp.")
            send_whatsapp(alert_msg)

        # --- Combo Alert Logic ---
        # This part remains active as combo alerts are requested.
        fut_symbol = ""
        if "BANKNIFTY" in symbol: fut_symbol = "BANKNIFTY30DEC25FUT"
        elif "NIFTY" in symbol: fut_symbol = "NIFTY30DEC25FUT"

        if fut_symbol and fut_symbol in symbol_data_state and symbol_data_state[fut_symbol]["oi_prev"] > 0:
            fut_state = symbol_data_state[fut_symbol]
            fut_oi_chg = fut_state["oi"] - fut_state["oi_prev"]
            fut_price_chg = fut_state["price"] - fut_state["price_prev"]
            fut_action = classify_future(fut_oi_chg, fut_price_chg)
            
            # Check for confirming future action (e.g., Buyer In + Future Buying)
            if (action == "Strong Buying" and fut_action == "FUTURE BUYING") or \
               (action == "Fresh Writing (High Conviction)" and "SHORT" in fut_action):
                fut_lots = lots_from_oi_change(fut_symbol, fut_oi_chg)
                fut_bucket = lot_bucket(fut_lots)
                if fut_bucket != "IGNORE":
                    fut_oi_roc = (fut_oi_chg / fut_state["oi_prev"] * 100)
                    future_msg = future_alert(fut_symbol, fut_action, fut_bucket, fut_lots, fut_oi_chg, fut_oi_roc)
                    combo_msg = combo_alert(alert_msg, future_msg)
                    print(f"ALERT: COMBO detected on {symbol} (Sending WhatsApp)")
                    send_whatsapp(combo_msg)

async def run_scanner():
    """The main function to connect, authenticate, subscribe, and process data."""
    while True: # Main loop for reconnection
        try:
            async with websockets.connect(WSS_URL) as websocket:
                print(f"[{now()}] Connected to WebSocket.")
                
                # 1. Authenticate
                auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
                await websocket.send(json.dumps(auth_request))
                auth_response_str = await websocket.recv()
                auth_response_json = json.loads(auth_response_str)
                
                if not auth_response_json.get("Complete"):
                    print(f"[{now()}] Authentication failed: {auth_response_str}. Retrying in 30s.")
                    await asyncio.sleep(30)
                    continue
                
                print(f"[{now()}] Authentication successful: {auth_response_str}")

                # 2. Subscribe to all symbols
                for symbol in SYMBOLS_TO_MONITOR:
                    # Using SubscribeRealtime as confirmed from docs
                    subscribe_request = {
                        "MessageType": "SubscribeRealtime",
                        "Exchange": "NFO",
                        "Unsubscribe": "false",
                        "InstrumentIdentifier": symbol
                    }
                    await websocket.send(json.dumps(subscribe_request))
                print(f"[{now()}] Sent subscription requests for {len(SYMBOLS_TO_MONITOR)} symbols.")

                # 3. Process incoming messages
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        msg_type = data.get("MessageType")

                        if msg_type == "RealtimeResult":
                            await process_data(data)
                        elif msg_type == "Echo":
                            if DEBUG_ECHO:
                                print(f"[{now()}] Echo heartbeat: {message}")
                            continue  # ignore heartbeats by default
                        else:
                            print(f"[{now()}] Received diagnostic message: {message}")
                    except json.JSONDecodeError:
                        print(f"[{now()}] Received non-JSON message: {message}")
                    except Exception as e:
                        print(f"[{now()}] Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[{now()}] WebSocket connection closed: {e}. Reconnecting in 10s.")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"[{now()}] An unexpected error occurred: {e}. Reconnecting in 30s.")
            await asyncio.sleep(30)

if __name__ == "__main__":
    print("GFDL Scanner Starting...")
    send_whatsapp("GFDL Scanner Starting...")
    try:
        asyncio.run(run_scanner())
    except KeyboardInterrupt:
        print("\nScanner stopped by user.")
        send_whatsapp("GFDL Scanner Stopped by user.")
    except Exception as e:
        print(f"A critical error occurred: {e}")
        send_whatsapp(f"GFDL Scanner CRASHED: {e}")
