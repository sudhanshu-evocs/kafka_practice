from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ehs-incidents",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ehs-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Consumer started...")

for message in consumer:
    event = message.value
    print("Received:", event)

    if event.get("severity") == "High":
        print("High severity alert!")