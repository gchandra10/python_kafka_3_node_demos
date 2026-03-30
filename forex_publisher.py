import json
import time
import requests
from confluent_kafka import Producer

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'forex-producer',
    'acks': 1
}
producer = Producer(conf)
topic_name = 'forex-rates'

# Public Financial Data Source (No Key Required)
# We will use the free 'Exchangerate.host' or 'Frankfurter' API 
# which are open-source and require no registration.
BASE_URL = "https://api.frankfurter.dev/v1/latest?base=USD"

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

def stream_forex_data():
    print("Starting Global Forex Stream (No Key Required)...")
    
    try:
        while True:
            # Fetching latest rates relative to USD
            response = requests.get(BASE_URL, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                rates = data.get('rates', {})
                timestamp = data.get('date')
                
                for currency, rate in rates.items():
                    payload = {
                        "base": "USD",
                        "target": currency,
                        "rate": rate,
                        "observed_at": timestamp,
                        "ingested_at": time.time()
                    }
                    
                    # Partition by Currency Pair (e.g., 'EUR')
                    # This ensures the trend for a specific currency is ordered
                    producer.produce(
                        topic=topic_name,
                        key=currency.encode('utf-8'),
                        value=json.dumps(payload).encode('utf-8'),
                        callback=delivery_report
                    )
                
                print(f"Pushed {len(rates)} currency pairs to Kafka.")
                producer.poll(0)
            
            # Markets move, but the free API updates daily or hourly
            # We poll every 60 seconds to simulate a live monitoring tool
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nStopping forex stream...")
    finally:
        producer.flush()

if __name__ == "__main__":
    stream_forex_data()