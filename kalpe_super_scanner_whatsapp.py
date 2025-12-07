import requests
import os
import time

INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
TOKEN = os.getenv("ULTRAMSG_TOKEN")
GROUP_ID = os.getenv("ULTRAMSG_GROUP_ID")


def send_whatsapp_message(message):
    url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/group"

    payload = {
        "token": TOKEN,
        "groupId": GROUP_ID,
        "body": message,
    }

    try:
        r = requests.post(url, json=payload)
        print("WA SENT:", r.json())
    except Exception as e:
        print("WA ERROR:", e)


# Send test message
send_whatsapp_message("⚠️ TEST: WhatsApp scanner connected successfully!")


# 🚨 IMPORTANT: Keep container alive forever
while True:
    time.sleep(60)
