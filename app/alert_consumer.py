from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ehs-incidents",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="alert-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Alert Consumer Started...")

for message in consumer:

    incident = message.value

    if incident["severity"] == "High":

        print("\n========== ALERT ==========")
        print("Emergency Notification")
        print("Incident:", incident["incident_id"])
        print("Severity:", incident["severity"])
        print("Site:", incident["site"])
        print("Notify Safety Team Immediately!")    