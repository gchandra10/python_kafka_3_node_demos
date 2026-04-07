import json
import time
from confluent_kafka import Consumer, KafkaError

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'rowan-lab-monitor-final',
    'auto.offset.reset': 'latest'
}

c = Consumer(conf)
# Topic list matches exactly with your Streamlit script
c.subscribe(['global-pulse', 'auction', 'transactions'])

# State for Auction and Fraud logic
first_bidder = None
fraud_registry = {} # Tracks user: [timestamps]

print("\n" + "="*60)
print("Listening: global-pulse, auction, transactions")
print("="*60 + "\n")

try:
    while True:
        msg = c.poll(0.1)

        if msg is None:
            continue
        if msg.error():
            continue

        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()

        try:
            data = json.loads(msg.value().decode('utf-8'))
        except Exception:
            continue

        # --- 1. Global Pulse Logic ---
        if topic == 'global-pulse':
            # Support both flat and nested structures
            student = data.get('student') or data.get('user') or 'Unknown'
            region = data.get('region') or data.get('data', {}).get('region', 'N/A')
            print(f"📍 [PULSE] Region: {region:7} | Part: {partition} | Student: {student}")

        # --- 2. Real-Time Auction Logic ---
        elif topic == 'auction':
            # Support both bidder and student keys
            bidder = data.get('bidder') or data.get('student') or 'Unknown'
            
            if first_bidder is None:
                first_bidder = bidder
                print(f"🏆 [AUCTION] WINNER DECLARED: {bidder} (Offset: {offset})")
            else:
                print(f"   [AUCTION] Late Bid: {bidder} at Offset {offset} - REJECTED")

        # --- 3. Fraud Detection Logic ---
        elif topic == 'transactions':
            # Check every possible ID key we've used
            user = data.get('user_id') or data.get('student') or data.get('user') or 'Unknown'
            amount = data.get('amount') or data.get('data', {}).get('amount', 0)
            
            now = time.time()
            fraud_registry.setdefault(user, []).append(now)
            recent_swipes = [t for t in fraud_registry[user] if now - t < 5]
            fraud_registry[user] = recent_swipes
            
            if len(recent_swipes) > 3:
                print(f"🚨 [FRAUD] ALERT: {user:12} | {len(recent_swipes)} swipes in 5s!")
            else:
                print(f"💳 [TXN] Valid: {user:12} | Part: {partition} | Amount: ${amount}")

except KeyboardInterrupt:
    print("\nStopping Monitor...")
finally:
    c.close()