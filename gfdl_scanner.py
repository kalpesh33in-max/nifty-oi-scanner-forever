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

# --- Symbol List ---
# List of all symbols to monitor
SYMBOLS_TO_MONITOR = [
    "NIFTY30DEC2526200CE.NFO",
    "NIFTY30DEC2526200PE.NFO",
    "NIFTY30DEC2526150CE.NFO",
    "NIFTY30DEC2526150PE.NFO",
    "NIFTY30DEC2526100CE.NFO",
    "NIFTY30DEC2526100PE.NFO",
    "NIFTY30DEC2526050CE.NFO",
    "NIFTY30DEC2526050PE.NFO",
    "NIFTY30DEC2526000CE.NFO",
    "NIFTY30DEC2526000PE.NFO",
    "NIFTY30DEC2525950CE.NFO",
    "NIFTY30DEC2525950PE.NFO",
    "NIFTY30DEC2526250CE.NFO",
    "NIFTY30DEC2526250PE.NFO",
    "NIFTY30DEC2526300CE.NFO",
    "NIFTY30DEC2526300PE.NFO",
    "NIFTY30DEC2526350CE.NFO",
    "NIFTY30DEC2526350PE.NFO",
    "NIFTY30DEC2526400CE.NFO",
    "NIFTY30DEC2526400PE.NFO",
    "NIFTY30DEC2526450CE.NFO",
    "NIFTY30DEC2526450PE.NFO",
    "BANKNIFTY30DEC2559300CE.NFO",
    "BANKNIFTY30DEC2559300PE.NFO",
    "BANKNIFTY30DEC2559200CE.NFO",
    "BANKNIFTY30DEC2559200PE.NFO",
    "BANKNIFTY30DEC2559100CE.NFO",
    "BANKNIFTY30DEC2559100PE.NFO",
    "BANKNIFTY30DEC2559000CE.NFO",
    "BANKNIFTY30DEC2559000PE.NFO",
    "BANKNIFTY30DEC2558900CE.NFO",
    "BANKNIFTY30DEC2558900PE.NFO",
    "BANKNIFTY30DEC2558800CE.NFO",
    "BANKNIFTY30DEC2558800PE.NFO",
    "BANKNIFTY30DEC2559400CE.NFO",
    "BANKNIFTY30DEC2559400PE.NFO",
    "BANKNIFTY30DEC2559500CE.NFO",
    "BANKNIFTY30DEC2559500PE.NFO",
    "BANKNIFTY30DEC2559600CE.NFO",
    "BANKNIFTY30DEC2559600PE.NFO",
    "BANKNIFTY30DEC2559700CE.NFO",
    "BANKNIFTY30DEC2559700PE.NFO",
    "BANKNIFTY30DEC2559800CE.NFO",
    "BANKNIFTY30DEC2559800PE.NFO",
    "HDFCBANK30DEC25995CE.NFO",
    "HDFCBANK30DEC25995PE.NFO",
    "HDFCBANK30DEC25990CE.NFO",
    "HDFCBANK30DEC25990PE.NFO",
    "HDFCBANK30DEC25985CE.NFO",
    "HDFCBANK30DEC25985PE.NFO",
    "HDFCBANK30DEC25980CE.NFO",
    "HDFCBANK30DEC25980PE.NFO",
    "HDFCBANK30DEC25975CE.NFO",
    "HDFCBANK30DEC25975PE.NFO",
    "HDFCBANK30DEC25970CE.NFO",
    "HDFCBANK30DEC25970PE.NFO",
    "HDFCBANK30DEC251000CE.NFO",
    "HDFCBANK30DEC251000PE.NFO",
    "HDFCBANK30DEC251005CE.NFO",
    "HDFCBANK30DEC251005PE.NFO",
    "HDFCBANK30DEC251010CE.NFO",
    "HDFCBANK30DEC251010PE.NFO",
    "HDFCBANK30DEC251015CE.NFO",
    "HDFCBANK30DEC251015PE.NFO",
    "HDFCBANK30DEC251020CE.NFO",
    "HDFCBANK30DEC251020PE.NFO",
    "RELIANCE30DEC251570CE.NFO",
    "RELIANCE30DEC251570PE.NFO",
    "RELIANCE30DEC251560CE.NFO",
    "RELIANCE30DEC251560PE.NFO",
    "RELIANCE30DEC251550CE.NFO",
    "RELIANCE30DEC251550PE.NFO",
    "RELIANCE30DEC251540CE.NFO",
    "RELIANCE30DEC251540PE.NFO",
    "RELIANCE30DEC251530CE.NFO",
    "RELIANCE30DEC251530PE.NFO",
    "RELIANCE30DEC251520CE.NFO",
    "RELIANCE30DEC251520PE.NFO",
    "RELIANCE30DEC251580CE.NFO",
    "RELIANCE30DEC251580PE.NFO",
    "RELIANCE30DEC251590CE.NFO",
    "RELIANCE30DEC251590PE.NFO",
    "RELIANCE30DEC251600CE.NFO",
    "RELIANCE30DEC251600PE.NFO",
    "RELIANCE30DEC251610CE.NFO",
    "RELIANCE30DEC251610PE.NFO",
    "RELIANCE30DEC251620CE.NFO",
    "RELIANCE30DEC251620PE.NFO",
    "BANKNIFTY30DEC25FUT.NFO",
    "NIFTY30DEC25FUT.NFO",
    "RELIANCE30DEC25FUT.NFO",
    "HDFCBANK30DEC25FUT.NFO",
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
symbol_data_state = {symbol: {"price": 0, "price_prev": 0, "oi": 0, "oi_prev": 0, "iv": 0, "iv_prev": 0} for symbol in SYMBOLS_TO_MONITOR}

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

def classify_option(oi_change, price_change, iv_change, symbol):
    """
    Classifies option activity based on OI, Price, and IV changes.
    """
    is_call = "CE" in symbol
    is_put = "PE" in symbol

    # --- WRITER-CENTRIC ANALYSIS (Based on user definitions) ---

    # OI is increasing
    if oi_change > 0:
        # Price is moving in favor of writers (Call price down, Put price up)
        if (is_call and price_change < 0) or (is_put and price_change > 0):
            if iv_change < 0:
                return "Fresh high Writing"
            else: # iv_change >= 0
                return "hedging with writing"
        # Price is moving against writers (Call price up, Put price down) -> This is BUYER activity
        elif (is_call and price_change > 0) or (is_put and price_change < 0):
            if iv_change > 0:
                return "Strong Buying"
            else: # iv_change <= 0
                return "Speculative Buying"

    # OI is decreasing
    elif oi_change < 0:
        # Price is moving against writers (Call price up, Put price down) -> Writers are closing
        if (is_call and price_change > 0) or (is_put and price_change < 0):
            if iv_change > 0:
                return "unwinding means exit position"
            else: # iv_change <= 0
                return "writer do profit booking"
        # Price is moving in favor of writers (Call price down, Put price up) -> Buyers are exiting
        elif (is_call and price_change < 0) or (is_put and price_change > 0):
            if iv_change > 0:
                return "Long Liquidation"
            else: # iv_change <= 0
                return "Profit Booking (Buyers)"

    return "Indecisive Movement"

def classify_future(oi_change, price_change):
    if oi_change > 0 and price_change > 0: return "FUTURE BUYING"
    if oi_change > 0 and price_change < 0: return "FUTURE SHORT BUILDUP"
    if oi_change < 0 and price_change > 0: return "FUTURE SHORT COVER"
    if oi_change < 0 and price_change < 0: return "FUTURE LONG EXIT"
    return "NO CLEAR FUTURE ACTION"

def option_alert(symbol, action, bucket, lots, existing_oi, oi_chg, oi_roc, price_dir, price, iv, iv_roc):
    """
    Formats the alert message for options.
    MODIFIED: This version includes IV and Existing OI.
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
IV: {iv:.2f}
IV RoC: {iv_roc:.2f}%
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
    state["iv_prev"] = state["iv"]
    state["price"] = data.get("LastTradePrice", state["price"])
    state["oi"] = data.get("OpenInterest", state["oi"])
    state["iv"] = data.get("ImpliedVolatility", state["iv"])

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

    # --- OI RoC Threshold Logic (for both options and futures) ---
    oi_roc = (oi_chg / state["oi_prev"] * 100) if state["oi_prev"] else 0

    current_roc_threshold = 3.0  # Default threshold for BANKNIFTY, RELIANCE, HDFCBANK
    if "NIFTY" in symbol:
        current_roc_threshold = 5.0

    # If OI RoC is not significant, do nothing and return early
    if abs(oi_roc) < current_roc_threshold:
        return
        
    # --- Now, handle future vs. option specific logic ---
    is_future = "FUT" in symbol
    lots = lots_from_oi_change(symbol, oi_chg)
    bucket = lot_bucket(lots)

    if bucket == "IGNORE":
        return

    if is_future:
        action = classify_future(oi_chg, price_chg)
        alert_msg = future_alert(symbol, action, bucket, lots, oi_chg, oi_roc)
        print(f"ALERT: Future Triggered for {symbol}. OI ROC: {oi_roc:.2f}%. Sending WhatsApp.")
        send_whatsapp(alert_msg)
    else: # It's an option
        iv_chg = state["iv"] - state["iv_prev"]
        iv_roc = (iv_chg / state["iv_prev"] * 100) if state["iv_prev"] else 0
        action = classify_option(oi_chg, price_chg, iv_chg, symbol)
        price_dir = "↑" if price_chg > 0 else "↓"
        alert_msg = option_alert(
            symbol, action, bucket, lots, 
            state["oi_prev"], oi_chg, oi_roc, price_dir, state["price"],
            state["iv"], iv_roc
        )
        
        print(f"ALERT: Option Triggered for {symbol}. OI ROC: {oi_roc:.2f}%. Sending WhatsApp.")
        send_whatsapp(alert_msg)

        # --- Combo Alert Logic ---
        # This part remains active as combo alerts are requested.
        fut_symbol = ""
        if "BANKNIFTY" in symbol: fut_symbol = "BANKNIFTY30DEC25FUT.NFO"
        elif "NIFTY" in symbol: fut_symbol = "NIFTY30DEC25FUT.NFO"
        elif "RELIANCE" in symbol: fut_symbol = "RELIANCE30DEC25FUT.NFO"
        elif "HDFCBANK" in symbol: fut_symbol = "HDFCBANK30DEC25FUT.NFO"
        
        if fut_symbol and fut_symbol in symbol_data_state and symbol_data_state[fut_symbol]["oi_prev"] > 0:
            fut_state = symbol_data_state[fut_symbol]
            fut_oi_chg = fut_state["oi"] - fut_state["oi_prev"]
            fut_price_chg = fut_state["price"] - fut_state["price_prev"]
            fut_action = classify_future(fut_oi_chg, fut_price_chg)
            
            # Check for confirming future action (e.g., Buyer In + Future Buying)
            if (action == "Strong Buying" and fut_action == "FUTURE BUYING") or \
               (action == "Fresh high Writing" and "SHORT" in fut_action):
                fut_lots = lots_from_oi_change(fut_symbol, fut_oi_chg)
                fut_bucket = lot_bucket(fut_lots)
                if fut_bucket != "IGNORE":
                    fut_oi_roc = (fut_oi_chg / fut_state["oi_prev"] * 100) if fut_state["oi_prev"] else 0
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
                            pass # Ignore heartbeats
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
