import asyncio
import websockets
import json
import os
import re
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# ============================== CONFIGURATION =================================
API_KEY = os.environ.get("API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Raw GitHub URL for symbol.txt
GITHUB_SYMBOL_URL = "https://raw.githubusercontent.com/kalpesh33in-max/nifty-oi-scanner-forever/main/symbol.txt"

WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Updated Lot Sizes
LOT_SIZES = {"BANKNIFTY": 30, "NIFTY": 25, "FINNIFTY": 25}
OI_SURGE_THRESHOLD = 200  # Number of lots for an alert

# ============================== STATE & UTILITIES =============================
all_available_symbols = []
monitored_symbols = set()
symbol_data_state = {}
future_prices = {"BANKNIFTY": 0}
active_ws = None

async def send_alert(message):
    print(f"ALERT: {message}")
    try:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(TELEGRAM_API_URL, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram Error: {e}")

async def fetch_symbols_from_github():
    global all_available_symbols
    try:
        print("Fetching symbol list from GitHub...")
        response = requests.get(GITHUB_SYMBOL_URL, timeout=10)
        if response.status_code == 200:
            # Clean up formatting: split by comma, strip spaces and newlines
            raw_symbols = response.text.replace('\n', '').split(',')
            all_available_symbols = [s.strip() for s in raw_symbols if s.strip()]
            print(f"Successfully loaded {len(all_available_symbols)} symbols.")
        else:
            print(f"Failed to fetch symbols. Status: {response.status_code}")
    except Exception as e:
        print(f"Error loading symbols: {e}")

# ============================== CORE LOGIC ====================================

async def process_data(msg_data):
    symbol = msg_data.get("Symbol")
    price = msg_data.get("LastPrice", 0)
    oi = msg_data.get("OpenInterest", 0)

    # 1. Catch Future Price to drive ATM logic
    if symbol == "BANKNIFTY-I":
        future_prices["BANKNIFTY"] = price
        return

    # 2. Process Option Data
    if symbol not in symbol_data_state:
        symbol_data_state[symbol] = {"oi": oi, "price": price}
        return

    state = symbol_data_state[symbol]
    old_oi = state["oi"]
    
    # Logic for OI Change
    oi_diff = oi - old_oi
    lot_size = LOT_SIZES.get("BANKNIFTY", 30)
    
    if abs(oi_diff) >= (OI_SURGE_THRESHOLD * lot_size):
        direction = "📈 OI SURGE" if oi_diff > 0 else "📉 OI DROP"
        msg = (f"<b>{direction}</b>\n"
               f"Symbol: {symbol}\n"
               f"Price: {price}\n"
               f"OI Change: {oi_diff // lot_size} Lots")
        await send_alert(msg)
        state["oi"] = oi  # Update base to avoid repeat alerts for same surge

async def update_subscriptions_loop():
    global monitored_symbols, active_ws
    while True:
        await asyncio.sleep(15)  # Check for new ATM every 15 seconds
        
        bn_price = future_prices.get("BANKNIFTY", 0)
        if bn_price == 0 or not all_available_symbols or not active_ws:
            continue

        # Calculate ATM and Range (+/- 10 strikes)
        atm_strike = round(bn_price / 100) * 100
        selected_strikes = [atm_strike + (i * 100) for i in range(-10, 11)]
        
        new_watches = set()
        for strike in selected_strikes:
            # Match symbols containing the strike and
