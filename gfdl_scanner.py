import asyncio
import websockets
import json
import time
import requests
import pandas as pd
from datetime import datetime, date
import re
import functools
import os
import sys
from zoneinfo import ZoneInfo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- Email Configuration (for daily reports) ---
# IMPORTANT: Use environment variables in production for security
EMAIL_HOST = os.environ.get("EMAIL_HOST") # e.g., 'smtp.gmail.com'
EMAIL_PORT = os.environ.get("EMAIL_PORT", 587) # e.g., 587 for TLS
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT")

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
LOT_THRESHOLDS = {
    "BANKNIFTY": 100,
    "HDFCBANK": 50,
    "ICICIBANK": 50,
    "SBIN": 50,
}

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

# --- Globals for Daily Reporting ---
daily_alerts = []
report_sent_today = False


# Dictionary to hold the latest price of the underlying futures
future_prices = {
    "BANKNIFTY": 0,
    "HDFCBANK": 0,
    "ICICIBANK": 0,
    "SBIN": 0,
}

def now():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

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

def lot_bucket(lots, symbol):
    """Classifies the number of lots into qualitative buckets based on the symbol."""
    if "BANKNIFTY" in symbol:
        if lots >= 175: return "VERY HIGH"
        if lots >= 125: return "HIGH"
        if lots >= 100: return "LOW"
    else:  # For other symbols like HDFCBANK, ICICIBANK, SBIN
        if lots >= 100: return "VERY HIGH"
        if lots >= 75: return "HIGH"
        if lots >= 50: return "LOW"
    
    return "IGNORE" # Should not be reached if alert thresholds are met

def classify_option(oi_change, price_change, iv_change, symbol):
    is_call, is_put = "CE" in symbol, "PE" in symbol
    if oi_change > 0:  # OI Increased
        # Writing: Price neutral or drops for Calls (or up/neutral for Puts)
        if (is_call and price_change <= 0) or (is_put and price_change >= 0):
            return "Fresh Writing (High Conviction)" if iv_change < 0 else "Forced Writing / Hedging"
        # Buying: Price increases for Calls (or drops for Puts)
        else: # (is_call and price_change > 0) or (is_put and price_change < 0)
            return "Strong Buying" if iv_change > 0 else "Speculative Buying"
    elif oi_change < 0:  # OI Decreased
        # Unwinding/Profit Booking (Writers): Price neutral or increases for Calls (or drops/neutral for Puts)
        if (is_call and price_change >= 0) or (is_put and price_change <= 0):
            return "Unwinding / Position Exit" if iv_change > 0 else "Profit Booking (Writers)"
        # Long Liquidation/Profit Booking (Buyers): Price decreases for Calls (or increases for Puts)
        else: # (is_call and price_change < 0) or (is_put and price_change > 0)
            return "Long Liquidation" if iv_change > 0 else "Profit Booking (Buyers)"
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
    atm_band = future_price * 0.005
    
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

def format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, iv_roc, moneyness):
    """Formats the alert message, showing N/A for missing IV."""
    price_chg_val = state['price'] - state['price_prev']
    price_dir = "↑" if price_chg_val > 0 else ("↓" if price_chg_val < 0 else "↔")
    
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

    # Construct the strike line conditionally
    strike_moneyness_line = ""
    if not is_future:
        strike_moneyness_line = f"STRIKE: {strike_display}{option_type_display} {moneyness}"

    main_message = f"""
{product_name} | {'FUTURE' if is_future else 'OPTION'}
{strike_moneyness_line}
ACTION: {action}
SIZE: {bucket} ({lots} lots)
EXISTING OI: {state['oi_prev']}
OI Δ: {oi_chg}
OI RoC: {oi_roc:.2f}%
PRICE: {price_dir}
TIME: {now()}
"""

    added_section = f"""
{year} {product_name} {'FUT' if is_future else f'{strike_display}{option_type_display}'}

{state['price']:.2f}
"""

    return f"{main_message}\n\n{added_section}"

# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================

async def process_data(data):
    """
    Processes a single data packet, updating future prices or sending option alerts.
    Option alerts are filtered to only include ITM/ATM strikes.
    """
    global symbol_data_state, future_prices
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    new_price = data.get("LastTradePrice")
    if new_price is None:
        return

    # If the symbol is a future, update its price and stop processing
    if "FUT" in symbol:
        underlying = None
        if "HDFCBANK" in symbol: underlying = "HDFCBANK"
        elif "ICICIBANK" in symbol: underlying = "ICICIBANK"
        elif "SBIN" in symbol: underlying = "SBIN"
        elif "BANKNIFTY" in symbol: underlying = "BANKNIFTY"
        
        if underlying and new_price > 0: # Ensure price is valid
            future_prices[underlying] = new_price
        return

    # --- Standard processing for Option contracts ---
    state = symbol_data_state[symbol]
    new_oi = data.get("OpenInterest")
    new_iv = data.get("ImpliedVolatility")

    if new_oi is None:
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
    
    iv_chg = 0
    iv_roc = 0.0
    if state["iv"] is not None and state["iv_prev"] is not None and state["iv_prev"] != 0:
        try:
            iv_chg = state["iv"] - state["iv_prev"]
            iv_roc = (iv_chg / state["iv_prev"]) * 100
        except ZeroDivisionError:
            iv_roc = 0.0
    
    try:
        oi_roc = (oi_chg / state["oi_prev"]) * 100
    except ZeroDivisionError:
        oi_roc = 0.0

    # --- Alert Logic ---
    lots = lots_from_oi_change(symbol, oi_chg)

    # Determine the correct lot size threshold for the symbol
    alert_threshold = 0
    for name, threshold in LOT_THRESHOLDS.items():
        if name in symbol:
            alert_threshold = threshold
            break
    
    if alert_threshold > 0 and lots >= alert_threshold:
        print(f"🚨 [{now()}] {symbol}: Lot size {lots} >= {alert_threshold}. Potential Alert.", flush=True)
        
        bucket = lot_bucket(lots, symbol)
        
        # This check is technically redundant now if LOT_THRESHOLDS only contains valid symbols,
        # but it's good for safety.
        if bucket != "IGNORE":
            moneyness = get_option_moneyness(symbol, future_prices)
            if moneyness in ["ITM", "ATM"]:
                print(f"📊 [{now()}] {symbol}: {moneyness}, lots: {lots}, Bucket: {bucket}. TRIGGERING ALERT.", flush=True)
                action = classify_option(oi_chg, price_chg, iv_chg, symbol)
                
                # --- Data Capture for Excel Report ---
                try:
                    product_name = next(name for name in LOT_SIZES if name in symbol)
                    match = re.search(r'(\d+)(CE|PE)$', symbol)
                    strike, option_type = (match.group(1), match.group(2)) if match else ("FUT", "")
                except (StopIteration, AttributeError):
                    product_name, strike, option_type = symbol, "N/A", ""

                price_chg_val = state['price'] - state['price_prev']
                price_dir = "↑" if price_chg_val > 0 else ("↓" if price_chg_val < 0 else "↔")
                
                alert_data = {
                    "time": now(),
                    "symbol": product_name,
                    "strike": strike,
                    "ce/pe": option_type,
                    "action": action,
                    "lot size": lots,
                    "EXISTING OI": state['oi_prev'],
                    "OI Δ": oi_chg,
                    "OI RoC": f"{oi_roc:.2f}%",
                    "price": price_dir,
                    "strike price": state['price']
                }
                daily_alerts.append(alert_data)
                # --- End Data Capture ---

                alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, iv_roc, moneyness)
                await send_whatsapp(alert_msg)

async def run_scanner():
    """The main function to connect, authenticate, subscribe, and process data."""
    global report_sent_today, daily_alerts
    
    last_check_time = time.time()
    
    while True:
        try:
            # --- WebSocket Connection Logic ---
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
                        # Non-blocking check for daily tasks (runs approx every 60s)
                        if time.time() - last_check_time > 60:
                            now_time = datetime.now(ZoneInfo("Asia/Kolkata"))
                            if now_time.hour == 0 and report_sent_today:
                                print(f"🌅 [{now()}] New day. Resetting daily report flag.", flush=True)
                                report_sent_today = False
                                daily_alerts = []
                            
                            if now_time.hour == 15 and now_time.minute >= 45 and not report_sent_today:
                                print(f"📅 [{now()}] Market closed. Time to generate and send daily report.", flush=True)
                                await generate_and_email_report()
                                report_sent_today = True
                            last_check_time = time.time()

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

async def generate_and_email_report():
    """Generates an Excel report from daily_alerts and emails it."""
    if not daily_alerts:
        print(f"ℹ️ [{now()}] No alerts were generated today. Skipping report.", flush=True)
        await send_whatsapp("ℹ️ GFDL Scanner recorded no significant alerts today. No report was generated.")
        return

    print(f"📝 [{now()}] Generating Excel report with {len(daily_alerts)} alerts...", flush=True)
    
    # --- Create Excel File in Memory ---
    try:
        df = pd.DataFrame(daily_alerts)
        filename = f"GFDL_Scanner_Report_{date.today().strftime('%Y-%m-%d')}.xlsx"
        
        # Use a temporary directory to save the file
        temp_dir = os.path.join(os.getcwd(), 'temp_reports')
        os.makedirs(temp_dir, exist_ok=True)
        filepath = os.path.join(temp_dir, filename)
        
        df.to_excel(filepath, index=False)
        print(f"✅ [{now()}] Successfully created Excel report: {filepath}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] Failed to create Excel file: {e}", flush=True)
        return

    # --- Send Email with Attachment ---
    if not all([EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASS, EMAIL_RECIPIENT]):
        print(f"⚠️ [{now()}] Email configuration is incomplete. Cannot send report. Please set EMAIL variables.", flush=True)
        await send_whatsapp(f"⚠️ Daily report was generated ({filename}) but cannot be sent. Email configuration is missing.")
        return

    print(f"✉️ [{now()}] Preparing to email report to {EMAIL_RECIPIENT}...", flush=True)
    
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = f"GFDL Scanner Daily Report - {date.today().strftime('%Y-%m-%d')}"

        body = f"Attached is the GFDL Scanner report for {date.today()}.\nTotal alerts: {len(daily_alerts)}"
        msg.attach(MIMEText(body, 'plain'))

        with open(filepath, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename= {filename}")
        msg.attach(part)

        # Blocking email call running in executor
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, send_email_blocking, msg)
        
        print(f"✅ [{now()}] Email sent successfully!", flush=True)
        await send_whatsapp(f"✅ Daily report with {len(daily_alerts)} alerts has been sent to {EMAIL_RECIPIENT}.")
        os.remove(filepath) # Clean up the file after sending
    except Exception as e:
        print(f"❌ [{now()}] Failed to send email: {e}", flush=True)
        await send_whatsapp(f"❌ Failed to email the daily report. Please check the logs. Error: {e}")

def send_email_blocking(msg):
    """Blocking function to send an email."""
    import smtplib
    server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    server.starttls()
    server.login(EMAIL_USER, EMAIL_PASS)
    text = msg.as_string()
    server.sendmail(EMAIL_USER, EMAIL_RECIPIENT, text)
    server.quit()


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
