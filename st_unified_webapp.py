import streamlit as st
import json
import time
import random
from confluent_kafka import Producer

# --- Kafka Configuration ---
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': 'streamlit-producer',
    'acks': 1 
}
producer = Producer(KAFKA_CONF)

def delivery_report(err, msg):
    if err is not None:
        st.error(f"Message delivery failed: {err}")

# Helper function to ensure the JSON structure matches the consumer
def send_to_kafka(topic, key, payload_data):
    # This structure matches: payload.get('user'), payload.get('ts'), payload.get('data')
    envelope = {
        "user": student_name,
        "ts": time.time(),
        "data": payload_data
    }

    # Ensure the key is explicitly encoded to bytes
    producer.produce(
        topic,
        key=str(key).encode('utf-8'), # Force string to bytes
        value=json.dumps(envelope).encode('utf-8'),
        callback=delivery_report
    )
    producer.flush()

# --- Streamlit UI Setup ---
st.set_page_config(page_title="Rowan Big Data Kafka Lab", layout="centered")
st.title("🛰️ Kafka Real-Time Interactive Lab")

demo_mode = st.sidebar.selectbox(
    "Select Demonstration",
    ["1. Global Pulse (Partitioning)", "2. Real-Time Auction (Consistency)", "3. Fraud Detection (CEP)"]
)

student_name = st.sidebar.text_input("Enter Your Name/ID", value="Student_1")

# --- 1. Global Pulse Logic ---
if demo_mode == "1. Global Pulse (Partitioning)":
    st.header("🌐 Global Pulse")
    region = st.selectbox("Select your Region:", ["North", "South", "East", "West"])
    
    if st.button("Emit Pulse"):
        # We send the region as the 'data' AND the 'key'
        send_to_kafka(
            topic='global-pulse', 
            key=region, 
            payload_data={"region": region, "value": random.randint(20, 30)}
        )
        
        st.success(f"Pulse sent to {region} partition!")

# --- 2. Real-Time Auction Logic ---
elif demo_mode == "2. Real-Time Auction (Consistency)":
    st.header("🔨 Real-Time Auction")
    if st.button("Place Bid ($10)"):
        # We send the bid as the 'data'
        send_to_kafka(
            topic='auction', 
            key="GOLDEN_TICKET", 
            payload_data={"bid": 10}
        )
        st.balloons()
        st.success("Bid Placed!")

# --- 3. Fraud Detection Logic ---
elif demo_mode == "3. Fraud Detection (CEP)":
    st.header("💳 Fraud Simulator")
    if st.button("Swipe Credit Card"):
        # We send the amount as the 'data'
        send_to_kafka(
            topic='transactions', 
            key=student_name, 
            payload_data={"amount": 5.00}
        )
        st.info("Transaction sent...")