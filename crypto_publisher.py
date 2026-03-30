import json
import time
import requests
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'}
producer = Producer(conf)
topic_name = 'crypto-ticker'

def stream_crypto():
    print("Streaming Global Crypto Ticker (No Key)...")
    url = "https://api.coinpaprika.com/v1/tickers"

    while True:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                tickers = response.json()
                # We take the top 50 to create a high-volume batch
                for coin in tickers[:50]:
                    payload = {
                        "symbol": coin['symbol'],
                        "price": coin['quotes']['USD']['price'],
                        "volume_24h": coin['quotes']['USD']['volume_24h'],
                        "market_cap": coin['quotes']['USD']['market_cap'],
                        "timestamp": coin['last_updated']
                    }
                    # Partition by Symbol (e.g., BTC, ETH)
                    producer.produce(
                        topic=topic_name,
                        key=payload['symbol'].encode('utf-8'),
                        value=json.dumps(payload).encode('utf-8')
                    )
                producer.flush()
                print(f"Pushed batch of {len(tickers[:50])} tickers.")
            
            # Respect the public API with a 30-second sleep
            time.sleep(30)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    stream_crypto()