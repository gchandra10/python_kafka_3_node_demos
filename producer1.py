import json
import time
import requests
from confluent_kafka import Producer

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'opensky-producer',
    'acks': 1
}
producer = Producer(conf)
topic_name = 'aircraft-telemetry'

# OpenSky API URL (Public access, no API key required for basic use)
# Bounding box for USA: lamin=25.2, lomin=-125.7, lamax=49.3, lomax=-66.9
OPENSKY_URL = "https://opensky-network.org/api/states/all?lamin=25.2&lomin=-125.7&lamax=49.3&lomax=-66.9"

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

def fetch_and_push():
    print(f"Polling OpenSky API for live aircraft data...")
    try:
        while True:
            response = requests.get(OPENSKY_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                states = data.get('states', [])
                
                if states:
                    for s in states:
                        # Map list to a dictionary for better downstream usability
                        flight_data = {
                            "icao24": s[0],
                            "callsign": s[1].strip() if s[1] else "N/A",
                            "origin_country": s[2],
                            "longitude": s[5],
                            "latitude": s[6],
                            "baro_altitude": s[7],
                            "velocity": s[9],
                            "timestamp": data.get('time')
                        }
                        
                        # Use ICAO24 (unique aircraft ID) as the key
                        # This ensures one aircraft's path stays in the same partition
                        producer.produce(
                            topic=topic_name,
                            key=flight_data['icao24'].encode('utf-8'),
                            value=json.dumps(flight_data).encode('utf-8'),
                            callback=delivery_report
                        )
                    
                    producer.flush()
                    print(f"Successfully pushed {len(states)} aircraft updates to Kafka.")
                else:
                    print("No aircraft found in the bounding box currently.")
            else:
                print(f"API Error: {response.status_code}")

            # OpenSky public API has a rate limit (once every 10 seconds for anonymous)
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nStopping flight tracker...")

if __name__ == "__main__":
    fetch_and_push()