import argparse
import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer

DEFAULT_TOPICS = ["gctopic", "gctopic_m"]
DEFAULT_BOOTSTRAP_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka consumer with auto-commit")
    parser.add_argument(
        "--group",
        default="gcgroup1",
        help="Consumer group id"
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=DEFAULT_TOPICS,
        help="One or more topic names"
    )
    parser.add_argument(
        "--bootstrap-servers",
        nargs="+",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--offset-reset",
        choices=["earliest", "latest"],
        default="earliest",
        help="Offset reset policy"
    )
    parser.add_argument(
        "--poll-ms",
        type=int,
        default=1000,
        help="Poll timeout in milliseconds"
    )
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

def parse_value(value):
    try:
        return json.loads(value)
    except Exception:
        return value

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
    print(f"Starting auto-commit consumer with group_id={args.group}, topics={args.topics}")

    consumer = build_consumer(args)

    try:
        while True:
            records = consumer.poll(timeout_ms=args.poll_ms)

            for _, messages in records.items():
                for message in messages:
                    message_key = decode_key(message.key)
                    message_ts, message_ts_type = format_timestamp(message)
                    payload = parse_value(message.value)

                    print("-" * 100)
                    print(
                        f"Mode: auto-commit\n"
                        f"Group: {args.group}\n"
                        f"Topic: {message.topic}\n"
                        f"Partition: {message.partition}\n"
                        f"Offset: {message.offset}\n"
                        f"Key: {message_key}\n"
                        f"TS: {message_ts}, {message_ts_type}\n"
                        f"Message: {payload}"
                    )

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()