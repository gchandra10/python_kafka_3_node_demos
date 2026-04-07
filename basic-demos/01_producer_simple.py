import sys
from kafka import KafkaProducer

TOPIC_NAME = "gctopic"
BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]

def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=10000
    )

def main():
    if len(sys.argv) < 2:
        print("Usage: uv run python publisher.py <message>")
        sys.exit(1)

    message = sys.argv[1]

    producer = build_producer()

    try:
        future = producer.send(TOPIC_NAME, value=message)
        metadata = future.get(timeout=10)
        producer.flush()

        print(
            f"Delivered message='{message}' "
            f"to topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}"
        )
    except Exception as e:
        print(f"Error producing message: {e}")
        sys.exit(2)
    finally:
        producer.close()

if __name__ == "__main__":
    main()