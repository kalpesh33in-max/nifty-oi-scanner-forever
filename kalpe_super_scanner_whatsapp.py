import asyncio
import websockets
import json
import time
import requests
from datetime import datetime
import re
import functools

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- API and Connection ---
WSS_URL = "wss://test.lisuns.com:4576/"
API_KEY = "414c338b-2ba1-4a1c-b15a-b9bab043728a"

# --- WhatsApp Alerting (UltraMSG) ---
ULTRAMSG_INSTANCE = "instance154723"
ULTRAMSG_TOKEN = "ue8gmu3bwu0bvqh8"
ULTRAMSG_GROUP_ID = "120363403351030118@g.us"
ULTRAMSG_API_URL = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"

# --- Symbol List (Options Only) ---
SYMBOLS_TO_MONITOR = [
    "BANKNIFTY30DEC2559000CE", "BANKNIFTY30DEC2559000PE",
    "BANKNIFTY30DEC2558900CE", "BANKNIFTY30DEC2558900PE",
    "BANKNIFTY30DEC2558800CE", "BANKNIFTY30DEC2558800PE",
    "BANKNIFTY30DEC2558700CE", "BANKNIFTY30DEC2558700PE",
    "BANKNIFTY30DEC2558600CE", "BANKNIFTY30DEC2558600PE",
    "BANKNIFTY30DEC2558500CE", "BANKNIFTY30DEC2558500PE",
    "BANKNIFTY30DEC2558400CE", "BANKNIFTY30DEC2558400PE",
    "BANKNIFTY30DEC2558300CE", "BANKNIFTY30DEC2558300PE",
    "BANKNIFTY30DEC2558200CE", "BANKNIFTY30DEC2558200PE",
    "BANKNIFTY30DEC2558100CE", "BANKNIFTY30DEC2558100PE",
    "BANKNIFTY30DEC2558000CE", "BANKNIFTY30DEC2558000PE",
    "BANKNIFTY30DEC2557900CE", "BANKNIFTY30DEC2557900PE",
    "BANKNIFTY30DEC2557800CE", "BANKNIFTY30DEC2557800PE",
    "BANKNIFTY30DEC2559100CE", "BANKNIFTY30DEC2559100PE",
    "BANKNIFTY30DEC2559200CE", "BANKNIFTY30DEC2559200PE",
    "BANKNIFTY30DEC2559300CE", "BANKNIFTY30DEC2559300PE",
    "BANKNIFTY30DEC2559400CE", "BANKNIFTY30DEC2559400PE",
    "BANKNIFTY30DEC2559500CE", "BANKNIFTY30DEC2559500PE",
    "BANKNIFTY30DEC2559600CE", "BANKNIFTY30DEC2559600PE",
    "BANKNIFTY30DEC2559700CE", "BANKNIFTY30DEC2559700PE",
    "BANKNIFTY30DEC2559800CE", "BANKNIFTY30DEC2559800PE",
    "BANKNIFTY30DEC2559900CE", "BANKNIFTY30DEC2559900PE",
    "BANKNIFTY30DEC2560000CE", "BANKNIFTY30DEC2560000PE",
    "BANKNIFTY30DEC2560100CE", "BANKNIFTY30DEC2560100PE",
    "BANKNIFTY30DEC2560200CE", "BANKNIFTY30DEC2560200PE",
    "BANKNIFTY30DEC2560300CE", "BANKNIFTY30DEC2560300PE",
    "BANKNIFTY30DEC2560400CE", "BANKNIFTY30DEC2560400PE",
    "BANKNIFTY30DEC2560500CE", "BANKNIFTY30DEC2560500PE",
    "BANKNIFTY30DEC2560600CE", "BANKNIFTY30DEC2560600PE",
    "BANKNIFTY30DEC2560700CE", "BANKNIFTY30DEC2560700PE",
    "BANKNIFTY30DEC2560800CE", "BANKNIFTY30DEC2560800PE",
    "BANKNIFTY30DEC2560900CE", "BANKNIFTY30DEC2560900PE"
]
Tokens_to_monitor=["51414"]
TOKEN_TO_SYMBOL = {
    "51414": "BANKNIFTY30DEC2559000CE"  # example – map ALL tokens properly
}
def split_option_symbol(symbol: str):
    """
    BANKNIFTY30DEC2559000CE →
    Product      : BANKNIFTY
    Expiry       : 30DEC2025
    StrikePrice  : 59000
    OptionType   : CE
    """

    match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d{2})(\d+)(CE|PE)$", symbol)
    if not match:
        raise ValueError(f"Invalid option symbol format: {symbol}")

    product, exp_ddmon, exp_yy, strike, opt_type = match.groups()

    return {
        "Product": product,
        "Expiry": f"{exp_ddmon}20{exp_yy}",
        "StrikePrice": int(strike),
        "OptionType": opt_type
    }

# --- Logic & Thresholds ---
BANKNIFTY_LOT = 35
OI_ROC_THRESHOLD = 5.0   # ALERT ONLY IF OI RoC >= 5%

# ==============================================================================
# =============================== STATE & UTILITIES ============================
# ==============================================================================
# symbol_data_state = {}
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
    if "BANKNIFTY" in symbol:
        lot_size = BANKNIFTY_LOT
    else:
        lot_size = 75
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
    print(main_message)
    return f"{main_message}\n\n{added_section}"
    
# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================


async def process_data(data):
    # print(f"📨 [{now()}] Processing data: {data}")
    global symbol_data_state

    msg_type = data.get("MessageType")

    # ================= PRICE + OI =================
    if msg_type == "RealtimeResult":
        symbol = data.get("InstrumentIdentifier")
        if symbol not in symbol_data_state:
            return

        state = symbol_data_state[symbol]

        price = data.get("LastTradePrice")
        oi = data.get("OpenInterest")
        ts = data.get("LastTradeTime")

        if price is None or oi is None or ts is None:
            return

        state["price_prev"] = state.get("price")
        state["oi_prev"] = state.get("oi")

        state["price"] = price
        state["oi"] = oi
        state["price_ts"] = ts

    # ================= IV / GREEKS =================
    elif msg_type == "RealtimeGreeksResult":
        token = str(data.get("Token"))
        symbol = TOKEN_TO_SYMBOL.get(token)

        if not symbol or symbol not in symbol_data_state:
            return

        state = symbol_data_state[symbol]

        iv = data.get("IV")
        ts = data.get("Timestamp")

        if iv is None or ts is None:
            return

        state["iv_prev"] = state.get("iv")
        state["iv"] = iv
        state["iv_ts"] = ts

    else:
        return

    # ================= SYNC CHECK =================
    # ================= SYNC CHECK =================
    state = symbol_data_state[symbol]

    required_keys = ["price", "oi", "iv", "price_ts", "iv_ts"]
    if not all(k in state and state[k] is not None for k in required_keys):
        return

    # Allow lag between price & greeks
    MAX_SYNC_GAP = 5
    if abs(state["price_ts"] - state["iv_ts"]) > MAX_SYNC_GAP:
        return

    # Skip until previous values exist
    if state["oi_prev"] in (None, 0) or state["iv_prev"] is None:
        return

    # Avoid duplicate alerts
    if state.get("last_alert_ts") == state["price_ts"]:
        return

    # ================= CALCULATIONS =================
    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0:
        return

    price_chg = state["price"] - state["price_prev"]
    iv_chg = state["iv"] - state["iv_prev"]

    oi_roc = (oi_chg / state["oi_prev"]) * 100 if state["oi_prev"] else 0
    iv_roc = (iv_chg / state["iv_prev"]) * 100 if state["iv_prev"] else 0

    if abs(oi_roc) < OI_ROC_THRESHOLD:
        return

    # ================= ALERT =================
    action = classify_option(oi_chg, price_chg, iv_chg, symbol)
    lots = lots_from_oi_change(symbol, oi_chg)
    bucket = lot_bucket(lots)

    if bucket == "IGNORE":
        return

    alert_msg = format_alert_message(
        symbol, action, bucket, lots,
        state, oi_chg, oi_roc, iv_roc
    )

    state["last_alert_ts"] = state["price_ts"]

    await send_whatsapp(alert_msg)

# async def run_scanner():
#     """The main function to connect, authenticate, subscribe, and process data."""
#     while True:
#         try:
#             async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
#                 print(f"✅ [{now()}] Connected to WebSocket. Authenticating...")
                
#                 auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
#                 await websocket.send(json.dumps(auth_request))
#                 auth_response = json.loads(await websocket.recv())
                
#                 if not auth_response.get("Complete"):
#                     print(f"❌ [{now()}] Authentication FAILED: {auth_response.get('Comment')}. Retrying in 30s.")
#                     await asyncio.sleep(30)
#                     continue
                
#                 print(f"✅ [{now()}] Authentication successful. Subscribing to {len(SYMBOLS_TO_MONITOR)} symbols...")

#                 for symbol in SYMBOLS_TO_MONITOR:
#                     await websocket.send(json.dumps({
#                         "MessageType": "SubscribeRealtime", "Exchange": "NFO",
#                         "Unsubscribe": "false", "InstrumentIdentifier": symbol
#                     }))
#                 for symbol in Tokens_to_monitor:
#                     await websocket.send(json.dumps({
#                         "MessageType": "SubscribeRealtimeGreeks", "Exchange": "NFO",
#                         "Token": symbol
#                     }))
#                 print(f"✅ [{now()}] Subscriptions sent. Scanner is now live.")
#                 await send_whatsapp("✅ GFDL Scanner is LIVE and monitoring the market.")

#                 async for message in websocket:
#                     try:
#                         data = json.loads(message)
#                         if data.get("MessageType") == "RealtimeResult":
#                             await process_data(data)
#                         if data.get("MessageType") == "RealtimeGreeksResult":
#                             await process_data(data)
#                     except json.JSONDecodeError:
#                         print(f"⚠️ [{now()}] Warning: Received a non-JSON message.")
#                     except Exception as e:
#                         print(f"❌ [{now()}] Error during message processing for {message}: {e}")

#         except websockets.exceptions.ConnectionClosed as e:
#             print(f"⚠️ [{now()}] WebSocket connection closed: {e}. Reconnecting in 10 seconds...")
#             await asyncio.sleep(10)
#         except Exception as e:
#             print(f"❌ [{now()}] An unexpected error occurred in the main loop: {e}. Reconnecting in 30 seconds...")
#             await asyncio.sleep(30)
async def fetch_instruments(websocket):
    
    global Tokens_to_monitor, TOKEN_TO_SYMBOL

    Tokens_to_monitor.clear()
    TOKEN_TO_SYMBOL.clear()

    print(f"🔍 [{now()}] Resolving tokens from manual symbols...")

    for symbol in SYMBOLS_TO_MONITOR:
        try:
            parts = split_option_symbol(symbol)
        except ValueError as e:
            print(e)
            continue

        req = {
            "MessageType": "GetInstruments",
            "Exchange": "NFO",
            "Product": parts["Product"],
            "Expiry": parts["Expiry"],
            "OptionType": parts["OptionType"],
            "StrikePrice": parts["StrikePrice"],
            "DetailedInfo": "true"
        }

        await websocket.send(json.dumps(req))

        while True:
            resp = json.loads(await websocket.recv())
            # print(f"📨 [{now()}] Received instrument response: {resp}")

            if resp.get("MessageType") != "InstrumentsResult":
                continue

            for inst in resp.get("Result", []):
                trade_symbol = inst.get("TradeSymbol")
                if trade_symbol == symbol:
                    token = str(inst["TokenNumber"])
                    Tokens_to_monitor.append(token)
                    TOKEN_TO_SYMBOL[token] = symbol
                    print(f"✅ Token mapped: {symbol} → {token}")
            break

    print(f"✅ [{now()}] Token mapping completed: {len(Tokens_to_monitor)} symbols")
    
async def run_scanner():
    global symbol_data_state, TOKEN_TO_SYMBOL, Tokens_to_monitor

    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=20) as websocket:
                print(f"✅ [{now()}] Connected. Authenticating...")

                await websocket.send(json.dumps({
                    "MessageType": "Authenticate",
                    "Password": API_KEY
                }))

                auth = json.loads(await websocket.recv())
                if not auth.get("Complete"):
                    await asyncio.sleep(30)
                    continue

                print("✅ Authentication successful")

                # 🔹 ONLY FETCH TOKENS
                await fetch_instruments(websocket)

                # 🔹 SUBSCRIBE PRICE + OI
                for symbol in SYMBOLS_TO_MONITOR:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtime",
                        "Exchange": "NFO",
                        "InstrumentIdentifier": symbol,
                        "Unsubscribe": "false"
                    }))

                # 🔹 SUBSCRIBE GREEKS
                for token in Tokens_to_monitor:
                    await websocket.send(json.dumps({
                        "MessageType": "SubscribeRealtimeGreeks",
                        "Exchange": "NFO",
                        "Token": token
                    }))

                print(f"🚀 [{now()}] Scanner LIVE (Manual symbols)")
                await send_whatsapp("✅ GFDL Scanner LIVE (Manual Symbols)")

                async for message in websocket:
                    data = json.loads(message)
                    if data.get("MessageType") in (
                        "RealtimeResult",
                        "RealtimeGreeksResult"
                    ):
                        await process_data(data)
                

        except websockets.exceptions.ConnectionClosed as e:
            print(f"⚠️ [{now()}] WebSocket connection closed: {e}. Reconnecting in 10 seconds...")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"❌ [{now()}] An unexpected error occurred in the main loop: {e}. Reconnecting in 30 seconds...")
            await asyncio.sleep(30)
if __name__ == "__main__":
    print("🚀 GFDL Scanner Starting...")
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
