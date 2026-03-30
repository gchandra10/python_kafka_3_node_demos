import json
import time
from confluent_kafka import Consumer, KafkaError

# Configuration for your 3-node cluster
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'rowan-class-monitor',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}

c = Consumer(conf)
# c.subscribe(['global-pulse', 'auction', 'transactions'])
c.subscribe(['transactions'])

# State for the Demo Logic
auction_winner = None
fraud_registry = {} # Tracks user: [timestamps]

print("\n" + "="*50)
print("🚀 KAFKA CONSOLE MONITOR ACTIVE")
print("Nodes: localhost:9092, 9093, 9094")
print("Waiting for student events...")
print("="*50 + "\n")

try:
    while True:
        msg = c.poll(0.1) # Fast polling for real-time feel

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Error: {msg.error()}")
            continue

        # 1. Grab metadata
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()

        # 2. Decode JSON safely
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            user = payload.get('user', 'Unknown')
            ts = payload.get('ts', time.time())
            data = payload.get('data', {})
        except Exception as e:
            print(f"Skipping malformed message: {e}")
            continue

        # 3. Logic per Demo Mode
        if topic == 'global-pulse':
            region = data.get('region', 'N/A')
            # Point out that Region 'North' always lands on the same Partition ID
            print(f"📍 [PULSE] Region: {region:6} | Part: {partition} | User: {user}")

        elif topic == 'auction':
            # Logic: The absolute first offset recorded wins
            if auction_winner is None:
                auction_winner = user
                print(f"🏆 [AUCTION] WINNER: {user} (Offset: {offset})")
            else:
                print(f"   [AUCTION] Late Bid: {user} (Offset: {offset}) - REJECTED")

        elif topic == 'transactions':
            now = time.time()
            # Fraud Logic: > 3 clicks in a 3-second window
            fraud_registry.setdefault(user, []).append(now)
            # Clean old timestamps
            recent_clicks = [t for t in fraud_registry[user] if now - t < 3]
            fraud_registry[user] = recent_clicks
            
            if len(recent_clicks) > 3:
                print(f"🚨 [FRAUD] Alert! User: {user:12} | Clicks: {len(recent_clicks)} (Rate Limit Exceeded)")
            else:
                print(f"💳 [TXN] Valid: {user:12} | Amount: ${data.get('amount', 0)}")

except KeyboardInterrupt:
    print("\nStopping monitor...")
finally:
    c.close()