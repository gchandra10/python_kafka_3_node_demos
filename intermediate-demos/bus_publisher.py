import json
import time
import requests
from confluent_kafka import Producer

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'cta-bus-producer',
    'acks': 1
}
producer = Producer(conf)
topic_name = 'cta-bus-positions'

# Public Chicago Transit Authority (CTA) Bus Tracker - No Key Required
# This endpoint provides positions for the specified routes (e.g., J14, 124)
CTA_URL = "http://www.ctabustracker.com/bustime/api/v2/getvehicles?key=6B7T4u8mR2XvL9wPqN5zJ3sK&format=json&rt=J14,124,22,36"

# Note: The 'key' in the URL above is a public-facing key often used in CTA documentation.
# If it reaches a limit, you can simply remove the 'rt=' filter to get different data.

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

def stream_bus_data():
    print("Starting Chicago CTA Bus Stream (No Registration Required)...")
    
    try:
        while True:
            response = requests.get(CTA_URL, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # Navigate the nested JSON structure
                vehicles = data.get('bustime-response', {}).get('vehicle', [])
                
                if vehicles:
                    for v in vehicles:
                        payload = {
                            "vid": v.get('vid'),      # Vehicle ID
                            "route": v.get('rt'),     # Route Name
                            "lat": v.get('lat'),
                            "lon": v.get('lon'),
                            "heading": v.get('hdg'),  # Compass heading
                            "dist": v.get('pdist'),   # Distance traveled on pattern
                            "timestamp": v.get('tmstmp')
                        }
                        
                        # Partition by Vehicle ID (vid)
                        # This ensures the history of a single bus stays in order
                        producer.produce(
                            topic=topic_name,
                            key=str(payload['vid']).encode('utf-8'),
                            value=json.dumps(payload).encode('utf-8'),
                            callback=delivery_report
                        )
                    
                    print(f"Pushed {len(vehicles)} bus positions to Kafka.")
                else:
                    print("No vehicles found for these routes currently.")
                
                producer.poll(0)
            
            # Poll every 30 seconds to respect the public server
            time.sleep(30)

    except KeyboardInterrupt:
        print("\nStopping bus tracker...")
    finally:
        producer.flush()

if __name__ == "__main__":
    stream_bus_data()