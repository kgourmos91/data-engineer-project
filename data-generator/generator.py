import random
import time
from datetime import datetime

import requests

API_ENDPOINT = "http://localhost:3000/data"

def generate_synthetic_record():
    return {
        "transaction_id": random.randint(100000, 999999),
        "customer_id": random.randint(1, 1000),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.now().isoformat()
    }

def send_record(record):
    try:
        response = requests.post(API_ENDPOINT, json=record)
        print(f"✅ Sent: {record} | Status: {response.status_code}")
    except Exception as e:
        print("❌ Error sending data:", e)

if __name__ == "__main__":
    while True:
        record = generate_synthetic_record()
        send_record(record)
        time.sleep(2)
