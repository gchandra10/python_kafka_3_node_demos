import argparse
import json
import time
import uuid
import requests
from kafka import KafkaProducer

TOPIC_NAME = "gctopic"
BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]
JOKE_API_URL = "https://official-joke-api.appspot.com/random_joke"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["random", "same-key", "explicit"], default="random")
    parser.add_argument("--partition", type=int, default=0)
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
        key_serializer=lambda k: k if isinstance(k, bytes) else k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=1000,
        linger_ms=500,
        batch_size=8192,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000
    )

def main():
    args = parse_args()
    producer = build_producer()

    try:
        while True:
            joke = fetch_joke()
            if not joke:
                print("No joke to send.")
                time.sleep(args.interval)
                continue

            if args.mode == "random":
                key = str(uuid.uuid4())
                future = producer.send(TOPIC_NAME, key=key, value=joke)

            elif args.mode == "same-key":
                key = "jokeCategory"
                future = producer.send(TOPIC_NAME, key=key, value=joke)

            else:
                key = str(uuid.uuid4())
                future = producer.send(TOPIC_NAME, key=key, value=joke, partition=args.partition)

            metadata = future.get(timeout=10)

            print(
                f"mode={args.mode} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset} "
                f"key={key} "
                f"id={joke.get('id')}"
            )

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nTerminating producer.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()