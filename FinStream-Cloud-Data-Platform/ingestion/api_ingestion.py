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
        response = requests.get(url, params={"vs_currency": "usd"})
        data = response.json()

        for coin in data:
            transaction = {
                "id": coin["id"],
                "symbol": coin["symbol"],
                "price": coin["current_price"],
                "market_cap": coin["market_cap"],
                "timestamp": time.time()
            }

            producer.send("transactions", transaction)
            print("Sent:", transaction)

        time.sleep(10)  # fetch every 10 sec

    except Exception as e:
        print("Error:", e)