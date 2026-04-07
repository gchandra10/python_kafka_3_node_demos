import json
import websocket
from confluent_kafka import Producer

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'coinbase-producer',
    'acks': 1
}
producer = Producer(conf)
topic_name = 'crypto-trades'

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Trade Logged: {msg.value().decode("utf-8")[:50]}...')

def on_message(ws, message):
    data = json.loads(message)
    # We only want the actual trade matches
    if data.get('type') == 'match':
        # Send to Kafka
        # Using the 'side' (buy/sell) as the key to partition data
        producer.produce(
            topic=topic_name,
            key=data['side'],
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

def on_open(ws):
    print("Connected to Coinbase. Subscribing to BTC-USD matches...")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]
    }
    ws.send(json.dumps(subscribe_msg))

# Start the WebSocket
ws = websocket.WebSocketApp(
    "wss://ws-feed.exchange.coinbase.com",
    on_open=on_open,
    on_message=on_message
)

try:
    ws.run_forever()
except KeyboardInterrupt:
    print("Closing stream...")
finally:
    producer.flush()