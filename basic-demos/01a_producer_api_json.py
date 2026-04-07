import argparse
import json
import time
import requests
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]
JOKE_API_URL = "https://official-joke-api.appspot.com/random_joke"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="gctopic")
    parser.add_argument("--interval", type=int, default=2)
    return parser.parse_args()

def fetch_joke():
    try:
        response = requests.get(JOKE_API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching joke: {e}")
        return None

def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=1000,
        linger_ms=100,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000
    )

def main():
    args = parse_args()
    producer = build_producer()

    try:
        while True:
            joke = fetch_joke()

            if joke is None:
                print("No joke fetched.")
                time.sleep(args.interval)
                continue

            future = producer.send(
                args.topic,
                key=str(joke.get("id")).encode("utf-8"),
                value=joke
            )
            metadata = future.get(timeout=10)

            print(
                f"Produced joke id={joke.get('id')} "
                f"to topic={metadata.topic}, "
                f"partition={metadata.partition}, "
                f"offset={metadata.offset}"
            )

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nTerminating the producer.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()