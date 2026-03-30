import streamlit as st
import json
import time
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'})

st.title("High-Speed Auction")
st.write("Bidding for: **Rowan Data Engineering Trophy**")

bidder = st.text_input("Bidder Name", "Anon")
if st.button("Place $10 Bid"):
    payload = {"bidder": bidder, "amount": 10, "ts": time.time()}
    # All bids for this item use the same key to ensure strict ordering
    producer.produce('auction', key="TROPHY_001", value=json.dumps(payload))
    producer.flush()
    st.balloons()
    st.success("Bid registered in the Kafka Log!")