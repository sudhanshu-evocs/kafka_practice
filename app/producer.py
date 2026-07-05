from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

events = [
    {"incident_id": "INC-1001", "severity": "High", "site": "Delhi DC"},
    {"incident_id": "INC-1002", "severity": "Medium", "site": "Mumbai DC"},
    {"incident_id": "INC-1003", "severity": "Low", "site": "Bangalore DC"}
]

for event in events:
    producer.send("ehs-incidents", value=event)
    print("Sent:", event)
    time.sleep(1)

producer.flush()
producer.close()