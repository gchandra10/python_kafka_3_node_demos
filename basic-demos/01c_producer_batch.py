import json
import time
import uuid
import requests
from kafka import KafkaProducer

TOPIC_NAME = "gctopic"
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
JOKE_API_URL = "https://official-joke-api.appspot.com/random_joke"

def fetch_joke():
    response = requests.get(JOKE_API_URL, timeout=10)
    response.raise_for_status()
    return response.json()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.bytes,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    batch_size=8192,
    linger_ms=500
)

try:
    while True:
        futures = []
        for _ in range(10):
            joke = fetch_joke()
            key = uuid.uuid4()
            futures.append(producer.send(TOPIC_NAME, key=key, value=joke))

        for future in futures:
            metadata = future.get(timeout=10)
            print(
                f"topic={metadata.topic} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"
            )

        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping batch producer.")
finally:
    producer.flush()
    producer.close()