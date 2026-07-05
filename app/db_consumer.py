from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ehs-incidents",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="db-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Database Consumer Started...")

for message in consumer:
    incident = message.value

    print("\n========== DATABASE ==========")
    print("Saving Incident to Database")
    print(incident)
    print("Record Saved Successfully")