# Kafka UI Portal

A React + Express portal for managing a local Kafka broker without writing command-line commands manually.

## Prerequisites

- Node.js and npm installed
- Java installed
- A Kafka binary distribution available at:
  `C:/Projects/KafkaUI/kafka_2.13-4.4.0`
- Git Bash or WSL recommended for Unix-based Kafka scripts on Windows

## Install frontend dependencies

```bash
cd /c/Projects/KafkaUI/kafka-ui-portal
npm install
```

## Run the portal

```bash
cd /c/Projects/KafkaUI/kafka-ui-portal
npm run dev
```

Then open:

- Frontend: http://localhost:5173
- API: http://localhost:3001/api/health

## Start Kafka in Git Bash / WSL

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
export KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=file:/C:/Projects/KafkaUI/kafka_2.13-4.4.0/config/log4j2.yaml"

CLUSTER_ID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format --standalone -t "$CLUSTER_ID" -c ./config/server.properties
./bin/kafka-server-start.sh ./config/server.properties
```

For a single-node local development broker, Kafka requires `--standalone` during metadata format. If you later want a multi-node KRaft cluster, use `--initial-controllers` or `--no-initial-controllers` and set `controller.quorum.voters` explicitly.

## Check topics

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Create a topic

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
./bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic demo-topic --partitions 1 --replication-factor 1
```

## Produce a message

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
printf 'hello from kafka ui\n' | ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-topic
```

## Consume messages

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic --from-beginning --max-messages 10
```

## Stop Kafka

```bash
cd /c/Projects/KafkaUI/kafka_2.13-4.4.0
./bin/kafka-server-stop.sh
```

## Notes

- This project expects the binary Kafka distribution under `C:/Projects/KafkaUI/kafka_2.13-4.4.0`.
- The KRaft broker must be formatted once before the first startup.
- The `KAFKA_LOG4J_OPTS` export is required on this Windows + Git Bash setup because Java otherwise interprets the Windows path incorrectly.
