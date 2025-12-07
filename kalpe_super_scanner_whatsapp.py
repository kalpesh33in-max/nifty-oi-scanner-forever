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
