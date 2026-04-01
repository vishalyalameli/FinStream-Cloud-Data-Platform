import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url = "https://api.coingecko.com/api/v3/coins/markets"

while True:
    try:
        response = requests.get(url, params={"vs_currency": "usd" ,"per_page": 10})
        data = response.json()

        # 🔥 FIX: check data type
        if not isinstance(data, list):
            print("Invalid API response:", data)
            time.sleep(30)
            continue

        for coin in data:
            transaction = {
                "id": coin.get("id"),
                "symbol": coin.get("symbol"),
                "price": coin.get("current_price"),
                "market_cap": coin.get("market_cap"),
                "timestamp": time.time()
            }

            producer.send("transactions", transaction)
            print("Sent:", transaction)

        time.sleep(60)

    except Exception as e:
        print("Error:", e)