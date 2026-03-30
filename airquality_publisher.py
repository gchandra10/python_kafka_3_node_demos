import json
import time
import requests
from confluent_kafka import Producer

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'air-quality-producer',
    'acks': 1
}
producer = Producer(conf)
topic_name = 'air-quality-sensors'

# A list of global cities with their coordinates (Latitude, Longitude)
# London, Tokyo, Chennai, Sydney, Berlin, Dubai
LOCATIONS = [
    {"city": "London", "lat": 51.50, "lon": -0.12},
    {"city": "Tokyo", "lat": 35.68, "lon": 139.65},
    {"city": "Chennai", "lat": 13.08, "lon": 80.27},
    {"city": "Sydney", "lat": -33.86, "lon": 151.20},
    {"city": "Berlin", "lat": 52.52, "lon": 13.40},
    {"city": "Dubai", "lat": 25.20, "lon": 55.27}
]

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

def stream_air_quality():
    print("Starting Global Air Quality Stream (No Key Required)...")
    
    try:
        while True:
            for loc in LOCATIONS:
                # Open-Meteo is completely free for non-commercial use, no key needed
                url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={loc['lat']}&longitude={loc['lon']}&current=european_aqi,pm2_5,pm10,nitrogen_dioxide"
                
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    current = data.get('current', {})
                    
                    payload = {
                        "city": loc['city'],
                        "aqi": current.get('european_aqi'),
                        "pm2_5": current.get('pm2_5'),
                        "pm10": current.get('pm10'),
                        "no2": current.get('nitrogen_dioxide'),
                        "unit": "μg/m³",
                        "timestamp": current.get('time')
                    }
                    
                    # Partition by City
                    producer.produce(
                        topic=topic_name,
                        key=loc['city'].encode('utf-8'),
                        value=json.dumps(payload).encode('utf-8'),
                        callback=delivery_report
                    )
                    print(f"Pushed Air Quality for {loc['city']}: AQI {payload['aqi']}")
                
                producer.poll(0)
            
            # Staggering: 10 seconds between batches to be a good citizen
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nStopping sensor stream...")
    finally:
        producer.flush()

if __name__ == "__main__":
    stream_air_quality()