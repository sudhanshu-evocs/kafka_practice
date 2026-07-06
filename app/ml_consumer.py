from kafka import KafkaConsumer
import json
import time

class IncidentRiskModel:
    """
    A simulated Machine Learning model for EHS Risk Assessment.
    Predicts probability of escalating site hazard based on incoming incidents.
    """
    def __init__(self):
        # Base risk weights for different sites
        self.site_risk_weights = {
            "Delhi DC": 0.4,
            "Mumbai DC": 0.35,
            "Bangalore DC": 0.25
        }
        
        # Severity multipliers
        self.severity_multipliers = {
            "High": 2.5,
            "Medium": 1.5,
            "Low": 0.8
        }

    def predict_escalation_probability(self, site, severity):
        """
        Calculates the risk probability of the incident escalating into a site hazard.
        Calculations are bounded between 0.0 and 1.0.
        """
        base_weight = self.site_risk_weights.get(site, 0.3)
        multiplier = self.severity_multipliers.get(severity, 1.0)
        
        # Simple ML scoring logic (simulating feature scaling & inference)
        raw_score = base_weight * multiplier
        risk_probability = min(max(raw_score, 0.0), 1.0)
        return risk_probability


def main():
    # Initialize the Kafka Consumer with group_id='ml-group'
    # This allows it to run in parallel with db-group and alert-group
    consumer = KafkaConsumer(
        "ehs-incidents",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="ml-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    # Instantiate our ML model
    model = IncidentRiskModel()

    print("\n===========================================")
    print("  EHS Incident ML Risk Consumer Started... ")
    print("===========================================\n")

    for message in consumer:
        incident = message.value
        site = incident.get("site", "Unknown")
        severity = incident.get("severity", "Low")
        incident_id = incident.get("incident_id", "Unknown")

        # Extract features and perform inference
        risk_prob = model.predict_escalation_probability(site, severity)
        
        # Determine risk classification based on threshold
        risk_class = "LOW"
        if risk_prob >= 0.75:
            risk_class = "CRITICAL"
        elif risk_prob >= 0.50:
            risk_class = "MEDIUM"

        print(f"[{incident_id}] Analyzing Site: {site} | Severity: {severity}")
        print(f"  -> Model Predicted Escalation Probability: {risk_prob:.2f} ({risk_class} RISK)")
        
        if risk_class == "CRITICAL":
            print("  [!] WARNING: High risk of hazard escalation at site. Recommending immediate inspection.")
        print("-" * 50)


if __name__ == "__main__":
    main()
