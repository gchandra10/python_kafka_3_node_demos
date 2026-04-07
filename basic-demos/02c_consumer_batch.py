import argparse
import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer

DEFAULT_TOPICS = ["gctopic"]
DEFAULT_BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka batch consumer (auto-commit)")
    parser.add_argument("--group", default="gcgroup18")
    parser.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS)
    parser.add_argument("--bootstrap-servers", nargs="+", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--poll-ms", type=int, default=5000)
    parser.add_argument("--max-records", type=int, default=500)
    parser.add_argument("--offset-reset", choices=["earliest", "latest"], default="earliest")
    return parser.parse_args()

def decode_key(key):
    if key is None:
        return "None"
    try:
        return str(uuid.UUID(bytes=key))
    except Exception:
        try:
            return key.decode("utf-8")
        except Exception:
            return repr(key)

def format_timestamp(message):
    ts = datetime.fromtimestamp(message.timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
    ts_type = "CreateTime" if message.timestamp_type == 0 else "LogAppendTime"
    return ts, ts_type

def build_consumer(args):
    return KafkaConsumer(
        *args.topics,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group,
        auto_offset_reset=args.offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=args.poll_ms
    )

def main():
    args = parse_args()
    print(f"Starting batch consumer (auto-commit) group={args.group}")

    consumer = build_consumer(args)

    try:
        while True:
            batch = consumer.poll(timeout_ms=args.poll_ms, max_records=args.max_records)

            total_records = sum(len(v) for v in batch.values())
            if total_records == 0:
                continue

            print(f"\nFetched batch size: {total_records}")

            for tp, messages in batch.items():
                for message in messages:
                    message_key = decode_key(message.key)
                    message_ts, message_ts_type = format_timestamp(message)

                    try:
                        payload = json.loads(message.value)
                    except Exception:
                        payload = message.value

                    print("-" * 100)
                    print(
                        f"Mode: batch-auto\n"
                        f"Topic: {message.topic}\n"
                        f"Partition: {message.partition}\n"
                        f"Offset: {message.offset}\n"
                        f"Key: {message_key}\n"
                        f"TS: {message_ts}, {message_ts_type}\n"
                        f"Message: {payload}"
                    )

    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()