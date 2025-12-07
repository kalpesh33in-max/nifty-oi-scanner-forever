import time
import requests
import os
from kalpe_super_scanner_whatsapp import send_whatsapp_message

INSTANCE_ID = os.environ.get("ULTRAMSG_INSTANCE_ID")
TOKEN = os.environ.get("ULTRAMSG_TOKEN")
GROUP_ID = os.environ.get("ULTRAMSG_GROUP_ID")

print("Instance ID =", INSTANCE_ID)
print("Token =", TOKEN)
print("Group ID =", GROUP_ID)

if not INSTANCE_ID or not TOKEN or not GROUP_ID:
    print("❌ ERROR: Missing environment variables")
    exit()

# ---------------------- SCANNER LOOP ----------------------
while True:
    try:
        message = "TEST: Scanner Running Successfully ✔"

        send_whatsapp_message(message)
        print("Message sent, sleeping...")

        time.sleep(20)  # send every 20 seconds

    except Exception as e:
        print("Main Loop ERROR:", e)
        time.sleep(5)
