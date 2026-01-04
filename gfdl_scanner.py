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
import copy


# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- Analysis Interval ---
ANALYSIS_INTERVAL_SECONDS = 15

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
ULTRAMSG_API_URL_CHAT = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"
ULTRAMSG_API_URL_DOCUMENT = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/document"
ULTRAMSG_API_URL_UPLOAD = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/media/upload"

# --- Symbol List (Options & Futures) ---
SYMBOLS_TO_MONITOR = [
    # BANKNIFTY Options
    "BANKNIFTY27JAN2660100CE", "BANKNIFTY27JAN2660100PE", "BANKNIFTY27JAN2660000CE", "BANKNIFTY27JAN2660000PE",
    "BANKNIFTY27JAN2659900CE", "BANKNIFTY27JAN2659900PE", "BANKNIFTY27JAN2659800CE", "BANKNIFTY27JAN2659800PE",
    "BANKNIFTY27JAN2659700CE", "BANKNIFTY27JAN2659700PE", "BANKNIFTY27JAN2659600CE", "BANKNIFTY27JAN2659600PE",
    "BANKNIFTY27JAN2660200CE", "BANKNIFTY27JAN2660200PE", "BANKNIFTY27JAN2660300CE", "BANKNIFTY27JAN2660300PE",
    "BANKNIFTY27JAN2660400CE", "BANKNIFTY27JAN2660400PE", "BANKNIFTY27JAN2660500CE", "BANKNIFTY27JAN2660500PE",
    "BANKNIFTY27JAN2660600CE", "BANKNIFTY27JAN2660600PE",

    # HDFCBANK Options
    "HDFCBANK27JAN261000CE", "HDFCBANK27JAN261000PE", "HDFCBANK27JAN26995CE", "HDFCBANK27JAN26995PE",
    "HDFCBANK27JAN26990CE", "HDFCBANK27JAN26990PE", "HDFCBANK27JAN261005CE", "HDFCBANK27JAN261005PE",
    "HDFCBANK27JAN261010CE", "HDFCBANK27JAN261010PE", "HDFCBANK27JAN26985CE", "HDFCBANK27JAN26985PE",
    "HDFCBANK27JAN261015CE", "HDFCBANK27JAN261015PE",

    # ICICIBANK Options
    "ICICIBANK27JAN261350CE", "ICICIBANK27JAN261350PE", "ICICIBANK27JAN261340CE", "ICICIBANK27JAN261340PE",
    "ICICIBANK27JAN261330CE", "ICICIBANK27JAN261330PE", "ICICIBANK27JAN261360CE", "ICICIBANK27JAN261360PE",
    "ICICIBANK27JAN261370CE", "ICICIBANK27JAN261370PE", "ICICIBANK27JAN261320CE", "ICICIBANK27JAN261320PE",
    "ICICIBANK27JAN261380CE", "ICICIBANK27JAN261380PE",

    # SBIN Options
    "SBIN27JAN261000CE", "SBIN27JAN261000PE", "SBIN27JAN26995CE", "SBIN27JAN26995PE",
    "SBIN27JAN26990CE", "SBIN27JAN26990PE", "SBIN27JAN261005CE", "SBIN27JAN261005PE",
    "SBIN27JAN261010CE", "SBIN27JAN261010PE", "SBIN27JAN26985CE", "SBIN27JAN26985PE",
    "SBIN27JAN261015CE", "SBIN27JAN261015PE",
    
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

# Live state, updated on every tick
symbol_data_state = {
    symbol: { "price": 0, "oi": 0 } for symbol in SYMBOLS_TO_MONITOR
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
    """Sends a text message to the configured UltraMSG group without blocking."""
    print(f"📦 [{now()}] Preparing to send WhatsApp text message...", flush=True)
    loop = asyncio.get_running_loop()
    
    params = {'token': ULTRAMSG_TOKEN, 'to': ULTRAMSG_GROUP_ID, 'body': msg, 'priority': 10}
    
    blocking_call = functools.partial(requests.post, ULTRAMSG_API_URL_CHAT, params=params, timeout=10)

    try:
        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status() 
        print(f"✅ [{now()}] WhatsApp text message sent successfully. Response: {response.text}", flush=True)
    except requests.exceptions.RequestException as e:
        print(f"❌ [{now()}] FAILED to send WhatsApp text message: {e}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] An unexpected error occurred while sending WhatsApp text: {e}", flush=True)

async def send_whatsapp_with_attachment(filepath: str, caption: str):
    """
    Sends a file attachment to the configured UltraMSG group.
    Returns True on success, False on failure.
    """
    print(f"📎 [{now()}] Preparing to send WhatsApp attachment: {filepath}", flush=True)
    if not os.path.exists(filepath):
        print(f"❌ [{now()}] File not found for attachment: {filepath}", flush=True)
        return False

    # First, upload the file to UltraMSG's CDN
    uploaded_url = await upload_file_to_ultramsg_cdn(filepath)
    if not uploaded_url:
        print(f"❌ [{now()}] Failed to upload file to UltraMSG CDN. Cannot send attachment.", flush=True)
        return False

    loop = asyncio.get_running_loop()
    
    # Now, send the document using the uploaded URL
    payload = {
        'token': ULTRAMSG_TOKEN,
        'to': ULTRAMSG_GROUP_ID,
        'document': uploaded_url, # Use the URL from the upload
        'caption': caption,
        'filename': os.path.basename(filepath)
    }
    
    response = None 
    try:
        # No 'files' parameter needed here, as the document is sent as a URL
        blocking_call = functools.partial(
            requests.post,
            ULTRAMSG_API_URL_DOCUMENT,
            params=payload,
            timeout=30
        )

        response = await loop.run_in_executor(None, blocking_call)
        response.raise_for_status()
        
        response_json = response.json()
        
        if response_json.get('error'):
            error_msg = response_json['error']
            print(f"❌ [{now()}] WhatsApp attachment FAILED. API Error: {error_msg}. Response: {response.text}", flush=True)
            return False

        if response_json.get('sent') == 'true' or response_json.get('sent') is True:
             print(f"✅ [{now()}] WhatsApp attachment sent successfully. Response: {response.text}", flush=True)
             return True
        else:
             print(f"❌ [{now()}] WhatsApp attachment FAILED. Unknown API Response: {response.text}", flush=True)
             return False

    except requests.exceptions.RequestException as e:
        print(f"❌ [{now()}] FAILED to send WhatsApp attachment due to a network error: {e}", flush=True)
        return False
    except json.JSONDecodeError:
        print(f"❌ [{now()}] FAILED to parse API response. Not valid JSON. Raw Response: {response.text if response else 'N/A'}", flush=True)
        return False
    except Exception as e:
        print(f"❌ [{now()}] An unexpected error occurred while sending WhatsApp attachment: {e}", flush=True)
        return False        
        async def upload_file_to_ultramsg_cdn(filepath: str) -> str | None:
            """
            Uploads a local file to the UltraMSG CDN and returns the public URL.
            Returns None on failure.
            """
            print(f"☁️ [{now()}] Attempting to upload {filepath} to UltraMSG CDN...", flush=True)
            if not os.path.exists(filepath):
                print(f"❌ [{now()}] Upload FAILED: File not found at {filepath}", flush=True)
                return None
        
            loop = asyncio.get_running_loop()
            
            payload = {'token': ULTRAMSG_TOKEN}
            
            response = None
            try:
                with open(filepath, "rb") as f:
                    files = {'file': f} # UltraMSG expects 'file' for media uploads
                    
                    blocking_call = functools.partial(
                        requests.post,
                        ULTRAMSG_API_URL_UPLOAD,
                        params=payload,
                        files=files,
                        timeout=60 # Increased timeout for uploads
                    )
                    response = await loop.run_in_executor(None, blocking_call)
                    response.raise_for_status()
        
                    response_json = response.json()
        
                    if response_json.get('error'):
                        error_msg = response_json['error']
                        print(f"❌ [{now()}] Upload FAILED. API Error: {error_msg}. Response: {response.text}", flush=True)
                        return None
                    
                    uploaded_url = response_json.get('url')
                    if uploaded_url:
                        print(f"✅ [{now()}] File uploaded successfully. URL: {uploaded_url}", flush=True)
                        return uploaded_url
                    else:
                        print(f"❌ [{now()}] Upload FAILED. No URL in response. Response: {response.text}", flush=True)
                        return None
        
            except requests.exceptions.RequestException as e:
                print(f"❌ [{now()}] Upload FAILED due to a network error: {e}", flush=True)
                return None
            except json.JSONDecodeError:
                print(f"❌ [{now()}] Upload FAILED. Could not parse API response as JSON. Raw Response: {response.text if response else 'N/A'}", flush=True)
                return None
            except Exception as e:
                print(f"❌ [{now()}] An unexpected error occurred during upload: {e}", flush=True)
                return None
        
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


def get_option_action_classification(option_oi_chg: float, option_price_chg: float) -> str:
    """
    Classifies market activity based on option OI/Price changes, using simplified logic.
    """
    option_oi_increased = option_oi_chg > 0
    option_oi_decreased = option_oi_chg < 0
    option_price_increased = option_price_chg > 0
    option_price_decreased = option_price_chg < 0

    # 1. OI ↑ + Price ↑ = BUYERS DOMINANT
    if option_oi_increased and option_price_increased:
        return "BUYERS DOMINANT"
    # 2. OI ↑ + Price ↓ = WRITERS DOMINANT
    elif option_oi_increased and option_price_decreased:
        return "WRITERS DOMINANT"
    # 3. OI ↓ + Price ↑ = SHORT COVERING
    elif option_oi_decreased and option_price_increased:
        return "SHORT COVERING"
    # 4. OI ↓ + Price ↓ = LONG UNWINDING
    elif option_oi_decreased and option_price_decreased:
        return "LONG UNWINDING"
    
    return "Indecisive Option Movement" # Fallback if no specific condition met


def get_option_moneyness(symbol, future_prices):
    """
    Checks if an option is ITM, ATM, or OTM based on the latest future price.
    Returns a string: "ITM", "ATM", or "OTM".
    """
    if "FUT" in symbol:
        return "N/A"

    underlying = next((name for name in LOT_SIZES if name in symbol), None)
    if not underlying:
        return "N/A" 

    future_price = future_prices.get(underlying)
    if not future_price or future_price == 0:
        print(f"⏳ [{now()}] {symbol}: Waiting for future price of {underlying} to check moneyness.", flush=True)
        return "OTM"

    try:
        match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)$', symbol)
        strike_price = int(match.group(1))
        option_type = match.group(2)
    except (AttributeError, TypeError, ValueError):
        return "N/A"

    atm_band = future_price * 0.005
    if abs(future_price - strike_price) <= atm_band:
        return "ATM"
    
    is_itm = (option_type == 'CE' and strike_price < future_price) or \
             (option_type == 'PE' and strike_price > future_price)

    if is_itm:
        return "ITM"
    else:
        print(f"ℹ️ [{now()}] {symbol}: OTM (Future: {future_price:.2f}, Strike: {strike_price}), alert suppressed.", flush=True)
        return "OTM"


def format_alert_message(symbol, action, bucket, lots, live_state, past_state, oi_chg, oi_roc, moneyness):
    """Formats the alert message based on the 15-second interval changes."""
    option_price_chg = live_state['price'] - past_state['price']
    option_price_dir = "↑" if option_price_chg > 0 else ("↓" if option_price_chg < 0 else "↔")
    
    product_name = next((name for name in LOT_SIZES if name in symbol), "UNKNOWN")

    strike_display, option_type_display = "", ""
    try:
        match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)$', symbol)
        if match:
            strike_display = match.group(1)
            option_type_display = match.group(2)
    except Exception:
        strike_display, option_type_display = "N/A", ""

    main_message = f"""
{product_name} | OPTION
STRIKE: {strike_display}{option_type_display} {moneyness}
ACTION: {action}
LOT size:- ({lots})
EXISTING OI: {past_state['oi']}
OI Δ: {oi_chg}
OI RoC: {oi_roc:.2f}%
OPTION PRICE: {live_state['price']:.2f} {option_price_dir}
TIME: {now()}
"""
    return main_message

# ==============================================================================
# ======================== INTERVAL-BASED ANALYSIS =============================
# ==============================================================================

async def analyze_interval_changes(snapshot_state):
    """
    Compares the live state against a past snapshot to find significant changes
    over the defined interval and sends alerts.
    """
    print(f"🔬 [{now()}] Analyzing {ANALYSIS_INTERVAL_SECONDS}-second interval...", flush=True)
    global symbol_data_state, future_prices, daily_alerts

    for symbol in SYMBOLS_TO_MONITOR:
        # We are no longer monitoring futures for alerts, only options
        if "FUT" in symbol:
            continue

        live_state = symbol_data_state[symbol]
        past_state = snapshot_state[symbol]

        if past_state.get("oi", 0) == 0 or live_state.get("oi", 0) == 0:
            continue

        oi_chg = live_state["oi"] - past_state["oi"]
        if oi_chg == 0:
            continue
        
        lots = lots_from_oi_change(symbol, oi_chg)
        alert_threshold = next((t for name, t in LOT_THRESHOLDS.items() if name in symbol), 0)

        # Only proceed if lot size meets the threshold
        if alert_threshold > 0 and lots >= alert_threshold:
            bucket = lot_bucket(lots, symbol)
            moneyness = get_option_moneyness(symbol, future_prices)
            
            # Only proceed if bucket is not IGNORE and option is ITM or ATM
            if bucket != "IGNORE" and moneyness in ["ITM", "ATM"]:
                option_price_chg = live_state["price"] - past_state["price"]
                try:
                    oi_roc = (oi_chg / past_state["oi"]) * 100
                except ZeroDivisionError:
                    oi_roc = 0.0

                # NEW CRITERION: Only send alert if OI RoC is > 3%
                if oi_roc > 3.0: # Using 3.0 for float comparison
                    print(f"📊 [{now()}] {symbol}: {moneyness}, lots: {lots}, Bucket: {bucket}, OI RoC: {oi_roc:.2f}%. TRIGGERING ALERT.", flush=True)
                    
                    action = get_option_action_classification(oi_chg, option_price_chg)
                    
                    product_name = next((name for name in LOT_SIZES if name in symbol), "N/A")
                    match = re.search(r'(\d+)(CE|PE)$', symbol)
                    strike, option_type = (match.group(1), match.group(2)) if match else ("N/A", "")

                    alert_data = {
                        "time": now(), "symbol": product_name, "strike": strike,
                        "ce/pe": option_type, "action": action, "lot size": lots,
                        "EXISTING OI": past_state['oi'], "OI Δ": oi_chg,
                        "OI RoC": f"{oi_roc:.2f}%", "price": "↑" if option_price_chg > 0 else "↓" if option_price_chg < 0 else "↔",
                        "strike price": live_state['price']
                    }
                    daily_alerts.append(alert_data)
                    
                    alert_msg = format_alert_message(symbol, action, bucket, lots, live_state, past_state, oi_chg, oi_roc, moneyness)
                    await send_whatsapp(alert_msg)

# ==============================================================================
# ============================ MAIN SCANNER & WEBSOCKET ========================
# ==============================================================================

async def process_data(data):
    """Processes a single data packet just to update the live symbol state."""
    global symbol_data_state, future_prices
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")
    
    if new_price is None or new_oi is None:
        return

    symbol_data_state[symbol]["price"] = new_price
    symbol_data_state[symbol]["oi"] = new_oi

    if "FUT" in symbol:
        underlying = next((name for name in LOT_SIZES if name in symbol), None)
        if underlying and new_price > 0:
            future_prices[underlying] = new_price

async def run_scanner():
    """The main function to connect, authenticate, subscribe, and process data."""
    global report_sent_today, daily_alerts, symbol_data_state
    
    snapshot_state = copy.deepcopy(symbol_data_state)
    last_analysis_time = time.time()
    
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
                        current_time = time.time()

                        if current_time - last_analysis_time > ANALYSIS_INTERVAL_SECONDS:
                            await analyze_interval_changes(snapshot_state)
                            snapshot_state = copy.deepcopy(symbol_data_state)
                            last_analysis_time = current_time

                        now_time = datetime.now(ZoneInfo("Asia/Kolkata"))
                        if now_time.hour == 0 and report_sent_today:
                            print(f"🌅 [{now()}] New day. Resetting daily report flag.", flush=True)
                            report_sent_today = False
                            daily_alerts = []
                        
                        if now_time.hour == 15 and now_time.minute >= 45 and not report_sent_today:
                            print(f"📅 [{now()}] Market closed. Time to generate and send daily report.", flush=True)
                            await generate_and_email_report()
                            report_sent_today = True

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
    """
    Generates an Excel report from daily_alerts and sends it directly via WhatsApp.
    Email functionality is removed to prioritize WhatsApp delivery.
    If WhatsApp fails, the file remains for manual retrieval.
    """
    if not daily_alerts:
        print(f"ℹ️ [{now()}] No alerts were generated today. Skipping report.", flush=True)
        await send_whatsapp("ℹ️ GFDL Scanner recorded no significant alerts today. No report was generated.")
        return

    print(f"📝 [{now()}] Generating Excel report with {len(daily_alerts)} alerts...", flush=True)
    
    filepath = None
    whatsapp_sent_success = False # Flag to track WhatsApp sending status
    try:
        df = pd.DataFrame(daily_alerts)
        filename = f"GFDL_Scanner_Report_{date.today().strftime('%Y-%m-%d')}.xlsx"
        
        temp_dir = os.path.join(os.getcwd(), 'temp_reports')
        os.makedirs(temp_dir, exist_ok=True)
        filepath = os.path.join(temp_dir, filename)
        
        df.to_excel(filepath, index=False)
        print(f"✅ [{now()}] Successfully created Excel report: {filepath}", flush=True)
    except Exception as e:
        print(f"❌ [{now()}] Failed to create Excel file: {e}", flush=True)
        await send_whatsapp(f"❌ Critical error: Failed to generate the daily Excel report. Error: {e}")
        return

    # --- Send Report directly via WhatsApp attachment ---
    print(f"📲 [{now()}] Sending report directly via WhatsApp attachment...", flush=True)
    caption = f"Daily Report ({date.today().strftime('%Y-%m-%d')})\nTotal Alerts: {len(daily_alerts)}"
    whatsapp_sent_success = await send_whatsapp_with_attachment(filepath, caption)

    if whatsapp_sent_success:
        await send_whatsapp(f"✅ Daily report with {len(daily_alerts)} alerts has been sent to this group.")
    else:
        await send_whatsapp(f"❌ CRITICAL: Failed to send the daily report via WhatsApp. Please check the logs. The report file is stored at: {filepath}")

    # --- Cleanup ---
    # Clean up the file only if it was successfully sent via WhatsApp.
    if whatsapp_sent_success:
        try:
            os.remove(filepath)
            print(f"🗑️ [{now()}] Cleaned up temporary report file: {filepath}", flush=True)
        except OSError as e:
            print(f"⚠️ [{now()}] Warning: Failed to remove temporary report file {filepath}. Error: {e}", flush=True)

# Email sending logic is entirely removed.

def send_email_blocking(msg):
    """Blocking function to send an email."""
    server = smtplib.SMTP(EMAIL_HOST, int(EMAIL_PORT))
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
        asyncio.run(send_whatsapp("🛑 GFDL Scanner was stopped manually."))
    except Exception as e:
        error_message = f"💥 GFDL Scanner CRASHED with a critical error: {e}"
        print(error_message, flush=True)
        asyncio.run(send_whatsapp(error_message))
