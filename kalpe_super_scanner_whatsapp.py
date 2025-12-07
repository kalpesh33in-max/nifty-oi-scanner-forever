import requests
import os

INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
TOKEN = os.getenv("ULTRAMSG_TOKEN")
GROUP_ID = os.getenv("ULTRAMSG_GROUP_ID")

def send_whatsapp_message(message):
    url = f"https://api.ultramsg.com/{INSTANCE_ID}/messages/group"

    payload = {
        "token": TOKEN,
        "groupId": GROUP_ID,
        "body": message
    }

    try:
        r = requests.post(url, json=payload)
        print("WA SENT:", r.json())
    except Exception as e:
        print("WA ERROR:", e)

# --------------------------
# MAIN ENTRY POINT
# --------------------------
if __name__ == "__main__":
    send_whatsapp_message("⚠️ TEST: WhatsApp scanner connected successfully!")

    # Keep container alive forever
    import time
    while True:
        time.sleep(60)
