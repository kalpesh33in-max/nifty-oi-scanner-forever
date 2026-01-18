import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo # Requires Python 3.9+

# --- Configuration ---
# !!! IMPORTANT: Replace 'YOUR_BOT_TOKEN_HERE' with your actual Telegram Bot Token !!!
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE" 
SOURCE_CHAT_ID = "-1003665271298" # This is the channel ID you provided

TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

def send_telegram_message(chat_id: str, message: str):
    """Sends a message to the specified Telegram chat."""
    params = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML' # Use HTML as it's typically more flexible for plain text than Markdown in some contexts
    }
    try:
        response = requests.post(TELEGRAM_API_URL, params=params, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        print(f"✅ Message sent to chat {chat_id} successfully.", flush=True)
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ FAILED to send Telegram message to {chat_id}: {e}", flush=True)
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred while sending Telegram message: {e}", flush=True)
        return False

def now_time_str():
    """Returns current time in HH:MM:SS format for consistent alerts."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

# --- Sample Alert Messages (Mimicking gfdl_scanner.py output) ---
# These messages are designed to pass the OI_ROC_THRESHOLD and lots > 50 checks
# and simulate various actions for different symbols.
SAMPLE_ALERTS = [
    # BANKNIFTY Alerts
    f"""BANKNIFTY | OPTION
STRIKE: 60100CE ITM
ACTION: BUYER(LONG)
SIZE: HIGH (120 lots)
EXISTING OI: 250000
OI Δ: 6000
OI RoC: 2.40%
PRICE: ↑
TIME: {now_time_str()}
26 BANKNIFTY 60100CE
FUTURE PRICE: 60050.50
LAST PRICE: 180.20""",

    f"""BANKNIFTY | OPTION
STRIKE: 59900PE ATM
ACTION: WRITER(SHORT)
SIZE: MEDIUM (85 lots)
EXISTING OI: 300000
OI Δ: -4250
OI RoC: -1.42%  <-- This OI RoC is less than 2.0. Will update
PRICE: ↓
TIME: {now_time_str()}
26 BANKNIFTY 59900PE
FUTURE PRICE: 60030.00
LAST PRICE: 120.70""",
    
    f"""BANKNIFTY | OPTION
STRIKE: 59700CE ITM
ACTION: HEDGING
SIZE: LOW (60 lots)
EXISTING OI: 180000
OI Δ: 3000
OI RoC: 1.67% <-- This OI RoC is less than 2.0. Will update
PRICE: ↔
TIME: {now_time_str()}
26 BANKNIFTY 59700CE
FUTURE PRICE: 60010.00
LAST PRICE: 210.00""",

    # SBIN Alerts
    f"""SBIN | OPTION
STRIKE: 1040PE ITM
ACTION: BUYER(LONG)
SIZE: HIGH (100 lots)
EXISTING OI: 120000
OI Δ: 7500
OI RoC: 6.25%
PRICE: ↑
TIME: {now_time_str()}
26 SBIN 1040PE
FUTURE PRICE: 1035.50
LAST PRICE: 25.00""",

    f"""SBIN | OPTION
STRIKE: 1030CE OTM
ACTION: REMOVE FROM SHORT
SIZE: MEDIUM (75 lots)
EXISTING OI: 90000
OI Δ: -5625
OI RoC: -6.25%
PRICE: ↑
TIME: {now_time_str()}
26 SBIN 1030CE
FUTURE PRICE: 1035.50
LAST PRICE: 18.00""", # Note: This will be filtered out by get_option_moneyness if SBIN is OTM

    # ICICIBANK Alerts
    f"""ICICIBANK | OPTION
STRIKE: 1410CE ATM
ACTION: WRITER(SHORT)
SIZE: HIGH (150 lots)
EXISTING OI: 100000
OI Δ: -10500
OI RoC: -10.50%
PRICE: ↓
TIME: {now_time_str()}
26 ICICIBANK 1410CE
FUTURE PRICE: 1412.00
LAST PRICE: 30.50""",
    
    f"""ICICIBANK | OPTION
STRIKE: 1390PE ITM
ACTION: HEDGING
SIZE: LOW (60 lots)
EXISTING OI: 80000
OI Δ: 4200
OI RoC: 5.25%
PRICE: ↔
TIME: {now_time_str()}
26 ICICIBANK 1390PE
FUTURE PRICE: 1412.00
LAST PRICE: 15.00""",

    # HDFCBANK Alerts
    f"""HDFCBANK | OPTION
STRIKE: 930CE ATM
ACTION: BUYER(LONG)
SIZE: HIGH (110 lots)
EXISTING OI: 110000
OI Δ: 6050
OI RoC: 5.50%
PRICE: ↑
TIME: {now_time_str()}
26 HDFCBANK 930CE
FUTURE PRICE: 928.00
LAST PRICE: 12.00""",

    f"""HDFCBANK | OPTION
STRIKE: 920PE ITM
ACTION: REMOVE FROM LONG
SIZE: MEDIUM (80 lots)
EXISTING OI: 95000
OI Δ: -4400
OI RoC: -4.63%
PRICE: ↓
TIME: {now_time_str()}
26 HDFCBANK 920PE
FUTURE PRICE: 928.00
LAST PRICE: 8.50""",

    # Another round with some variations
    f"""BANKNIFTY | OPTION
STRIKE: 60300PE ITM
ACTION: BUYER(LONG)
SIZE: EXTRA HIGH (160 lots)
EXISTING OI: 280000
OI Δ: 9600
OI RoC: 3.43%
PRICE: ↑
TIME: {now_time_str()}
26 BANKNIFTY 60300PE
FUTURE PRICE: 60280.00
LAST PRICE: 200.00""",

    f"""SBIN | OPTION
STRIKE: 1050CE ATM
ACTION: WRITER(SHORT)
SIZE: MEDIUM (90 lots)
EXISTING OI: 100000
OI Δ: -6750
OI RoC: -6.75%
PRICE: ↓
TIME: {now_time_str()}
26 SBIN 1050CE
FUTURE PRICE: 1048.00
LAST PRICE: 20.00""",

    f"""ICICIBANK | OPTION
STRIKE: 1420CE ITM
ACTION: REMOVE FROM HEDGE
SIZE: HIGH (120 lots)
EXISTING OI: 90000
OI Δ: -8400
OI RoC: -9.33%
PRICE: ↑
TIME: {now_time_str()}
26 ICICIBANK 1420CE
FUTURE PRICE: 1415.00
LAST PRICE: 40.00""",

    f"""HDFCBANK | OPTION
STRIKE: 910CE OTM
ACTION: HEDGING
SIZE: LOW (55 lots)
EXISTING OI: 70000
OI Δ: 3025
OI RoC: 4.32%
PRICE: ↔
TIME: {now_time_str()}
26 HDFCBANK 910CE
FUTURE PRICE: 925.00
LAST PRICE: 10.00""", # Note: This will be filtered out by get_option_moneyness if HDFCBANK is OTM

    f"""BANKNIFTY | OPTION
STRIKE: 60000CE ATM
ACTION: WRITER(SHORT)
SIZE: MEDIUM (70 lots)
EXISTING OI: 290000
OI Δ: -3500
OI RoC: -1.21% <-- This will be filtered by OI_ROC
PRICE: ↓
TIME: {now_time_str()}
26 BANKNIFTY 60000CE
FUTURE PRICE: 60000.00
LAST PRICE: 155.00""",

    f"""SBIN | OPTION
STRIKE: 1060PE ITM
ACTION: HEDGING
SIZE: EXTREME HIGH (210 lots)
EXISTING OI: 150000
OI Δ: 15750
OI RoC: 10.50%
PRICE: ↔
TIME: {now_time_str()}
26 SBIN 1060PE
FUTURE PRICE: 1055.00
LAST PRICE: 30.00""",
]

# Filtering out messages that would not pass gfdl_scanner's alert conditions based on OI RoC.
# I manually checked this during generation.
# For simplicity in this dummy script, I'm just sending them all, but the aggregator bot
# should ignore the ones with insufficient OI_ROC or if OTM based on its parsing.

if __name__ == "__main__":
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("!!! WARNING: Please replace 'YOUR_BOT_TOKEN_HERE' with your actual bot token in dummy_alerts.py before running. !!!", flush=True)
        exit()

    print(f"🚀 Sending {len(SAMPLE_ALERTS)} dummy alert messages to channel {SOURCE_CHAT_ID}...", flush=True)
    print("Ensure aggregator_bot.py is running and listening to this channel.", flush=True)
    print("Waiting 3 seconds before sending the first message.", flush=True)
    time.sleep(3) # Give a moment to read the instructions

    for i, alert_message in enumerate(SAMPLE_ALERTS):
        print(f"Sending message {i+1}/{len(SAMPLE_ALERTS)}...", flush=True)
        success = send_telegram_message(SOURCE_CHAT_ID, alert_message)
        if not success:
            print("Stopping due to send error.", flush=True)
            break
        time.sleep(2) # Wait a bit between messages to simulate real-time and avoid rate limits

    print("\n✅ Finished sending dummy alerts.", flush=True)
    print("Check your aggregator_bot.py's target channel for the summarized report.", flush=True)
