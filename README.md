# Kafka Practice Environment

This project is a beginner-friendly implementation of Apache Kafka using Python. It demonstrates how a Producer publishes EHS incident events to a Kafka topic and how multiple Consumers independently process those events, simulating a real-world event-driven architecture.

This repository contains a local Apache Kafka environment configured using Docker Compose.

## Services Included

1. **ZooKeeper**: Coordination service for Kafka brokers.
2. **Kafka Broker**: Single-node Kafka broker exposed on port `9092` for external clients and `29092` for internal docker networking.
3. **Kafka UI**: A web-based interface (accessible at [http://localhost:8080](http://localhost:8080)) to manage and monitor topics, consumer groups, and messages.

---

## Getting Started

### 1. Start the Environment

Make sure **Docker Desktop** is running, then run:

```bash
docker compose up -d
```

### 2. Verify Services are Running

Check the status of the containers:

```bash
docker compose ps
```

---

## Practicing Kafka CLI Commands

You can run Kafka CLI commands directly inside the running `kafka` container.

### 1. Create a Topic

Create a topic named `practice-topic` with 3 partitions and 1 replication factor:

```bash
docker exec -it kafka kafka-topics --create --topic practice-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 2. List Topics

Verify the topic was created:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Describe a Topic

Get details about partitions and replicas:

```bash
docker exec -it kafka kafka-topics --describe --topic practice-topic --bootstrap-server localhost:9092
```

### 4. Produce Messages

Start a producer and type messages. Press `Enter` to send, and `Ctrl + C` to exit:

```bash
docker exec -it kafka kafka-console-producer --topic practice-topic --bootstrap-server localhost:9092
```

### 5. Consume Messages

Start a consumer to read messages from the beginning:

```bash
docker exec -it kafka kafka-console-consumer --topic practice-topic --bootstrap-server localhost:9092 --from-beginning
```

---

## Web Interface

Open your browser and navigate to [http://localhost:8080](http://localhost:8080) to access **Kafka UI**. Here you can:
- View topics, partitions, and replication status.
- Inspect, search, and produce messages visually.
- Monitor active consumers and consumer groups.

