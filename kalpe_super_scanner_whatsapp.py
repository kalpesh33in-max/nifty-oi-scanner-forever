import requests
import os
import time

INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
TOKEN = os.getenv("ULTRAMSG_TOKEN")
GROUP_ID = os.getenv("ULTRAMSG_GROUP_ID")     # 120363403351030118@g.us

def send_whatsapp_message(message):
    url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/chat"

    payload = {
        "token": TOKEN,
        "to": GROUP_ID,
        "body": message
    }

    try:
        r = requests.post(url, json=payload)
        print("WA SENT:", r.json())
    except Exception as e:
        print("WA ERROR:", e)


# Test message on start
send_whatsapp_message("⚠️ TEST: WhatsApp scanner connected successfully!")

# Keep container alive
while True:
    time.sleep(60)
