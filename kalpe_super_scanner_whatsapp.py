import os
import time
import requests

INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
TOKEN = os.getenv("ULTRAMSG_TOKEN")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")

def send_whatsapp(msg):
    url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/chat"
    payload = {
        "token": TOKEN,
        "to": WHATSAPP_TO,
        "body": msg
    }
    try:
        r = requests.post(url, data=payload)
        print("WA SENT:", r.text)
    except Exception as e:
        print("WA ERROR:", e)

# ----------- YOUR SCANNER LOGIC -------------
def run_scanner():
    while True:
        # Replace with your actual scanner conditions
        send_whatsapp("🚀 TEST: Scanner Running in Group (WhatsApp)")
        time.sleep(20)  # send message every 20 seconds so Railway stays alive


if __name__ == "__main__":
    send_whatsapp("🎉 Scanner Started Successfully (GROUP)")
    run_scanner()
