import requests
import json
import time

# Configuration
SYMBOL = "NIFTY"
URL = "https://www.nseindia.com/api/option-chain-indices?symbol=" + SYMBOL
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9"
}

def fetch_oi_data():
    try:
        session = requests.Session()
        # Hit the home page first to get cookies (Required by NSE)
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
        response = session.get(URL, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: Status Code {response.status_code}")
            return None
    except Exception as e:
        print(f"Connection Failed: {e}")
        return None

def scan_markets():
    data = fetch_oi_data()
    if not data:
        return

    # Extracting the list of option data records
    records = data.get('records', {}).get('data', [])
    
    # --- THIS IS THE SECTION THAT CRASHED IN YOUR SCREENSHOT ---
    # Line 102: The 'for' loop
    for entry in records:
        # Line 103: MUST BE INDENTED (4 spaces)
        # Match symbols containing the strike and calculate PCR/OI
        strike_price = entry.get('strikePrice')
        expiry_date = entry.get('expiryDate')
        
        ce_data = entry.get('CE', {})
        pe_data = entry.get('PE', {})
        
        ce_oi = ce_data.get('openInterest', 0)
        pe_oi = pe_data.get('openInterest', 0)
        
        # Simple Logic: Identify high OI strikes
        if ce_oi > 50000 or pe_oi > 50000:
            print(f"Strike: {strike_price} | Expiry: {expiry_date}")
            print(f"  CE OI: {ce_oi} | PE OI: {pe_oi}")
    # --- END OF FIXED SECTION ---

if __name__ == "__main__":
    print("Starting Nifty OI Scanner...")
    while True:
        scan_markets()
        print("Scan complete. Waiting 60 seconds...")
        time.sleep(60) # Scan every minute
