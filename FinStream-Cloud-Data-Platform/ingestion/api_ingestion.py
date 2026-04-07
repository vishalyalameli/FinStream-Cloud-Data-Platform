from kafka import KafkaProducer
import json, time, requests
from utils.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url = "https://api.coingecko.com/api/v3/coins/markets"

while True:
    response = requests.get(url, params={"vs_currency": "usd", "per_page": 10})
    data = response.json()

    for coin in data:
        transaction = {
            "id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "price": coin.get("current_price"),
            "market_cap": coin.get("market_cap"),
            "timestamp": time.time()
        }

        producer.send(KAFKA_TOPIC, transaction)
        print("Sent:", transaction)

    producer.flush()
    time.sleep(10)