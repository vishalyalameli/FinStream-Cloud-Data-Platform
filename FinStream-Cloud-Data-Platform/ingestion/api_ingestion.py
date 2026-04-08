from kafka import KafkaProducer
import json
import time
import requests
from utils.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC

# =========================
# KAFKA PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

# =========================
# API CONFIG
# =========================
API_URL = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "per_page": 10
}

# =========================
# MAIN LOOP
# =========================
while True:
    try:
        print("\n📡 Fetching data from API...")

        response = requests.get(API_URL, params=PARAMS, timeout=10)

        # ✅ Check status
        if response.status_code != 200:
            print(f"❌ API Error: {response.status_code}")
            time.sleep(30)
            continue

        data = response.json()

        # ✅ Validate response type
        if not isinstance(data, list):
            print("❌ Invalid API response:", data)
            time.sleep(30)
            continue

        for coin in data:

            # ✅ Skip invalid records
            if not isinstance(coin, dict):
                print("⚠️ Skipping invalid data:", coin)
                continue

            transaction = {
                "id": coin.get("id"),
                "symbol": coin.get("symbol"),
                "price": float(coin.get("current_price", 0)),
                "market_cap": float(coin.get("market_cap", 0)),
                "timestamp": time.time()
            }

            try:
                producer.send(KAFKA_TOPIC, transaction)
                print("✅ Sent:", transaction)

            except Exception as kafka_error:
                print("❌ Kafka Error:", kafka_error)

        producer.flush()
        print("🚀 Batch sent successfully")

        time.sleep(10)

    except requests.exceptions.RequestException as api_error:
        print("❌ API Request Error:", api_error)
        time.sleep(30)

    except Exception as e:
        print("❌ Unexpected Error:", e)
        time.sleep(30)