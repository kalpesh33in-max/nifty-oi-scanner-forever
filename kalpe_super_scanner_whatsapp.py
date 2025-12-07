import requests
import os

INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
TOKEN = os.getenv("ULTRAMSG_TOKEN")
GROUP_ID = os.getenv("WHATSAPP_TO")   # example: 120363408331503118@g.us

def send_whatsapp(msg):
    url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/group"

    payload = {
        "token": TOKEN,
        "to": GROUP_ID,
        "body": msg
    }

    try:
        r = requests.post(url, data=payload)
        print("WA SENT:", r.text)
    except Exception as e:
        print("WA ERROR:", e)
