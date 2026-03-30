import json
import time
import requests
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'}
producer = Producer(conf)
topic_name = 'github-events'

def stream_github():
    print("Streaming GitHub Firehose to Kafka...")
    # No Key Required for public events
    url = "https://api.github.com/events"
    seen_ids = set()

    while True:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                events = response.json()
                for event in events:
                    if event['id'] not in seen_ids:
                        # Use the Repository ID as the Kafka Key
                        # This ensures all events for one repo stay in order
                        repo_id = str(event['repo']['id'])
                        producer.produce(
                            topic=topic_name,
                            key=repo_id.encode('utf-8'),
                            value=json.dumps(event).encode('utf-8')
                        )
                        seen_ids.add(event['id'])
                
                producer.flush()
                # GitHub allows 60 unauthenticated requests per hour
                # We sleep to respect their rate limit while still getting batches
                time.sleep(60) 
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_github()