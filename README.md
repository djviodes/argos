# Argos

A Go-based network traffic monitor built for hands-on learning in raw sockets, packet capture, and flow-level telemetry. Captures traffic on a network interface, parses packet headers, and aggregates flows (source, destination, byte count, duration) into a minimal NetFlow-style summary. Published to Kafka and persisted to PostgreSQL.

## Status

Early development. Capture, flow aggregation, and Kafka publishing are implemented and tested;
PostgreSQL storage and `cmd/argos` pipeline wiring are not yet built.

## Requirements

- Go 1.26+
- Kafka (local instance via Docker)
- PostgreSQL
- Root/elevated privileges (or `CAP_NET_RAW`) for raw socket packet capture

## Project Structure

```
/cmd/argos            — main package; wires stages together, owns startup/shutdown
/internal/capture     — raw socket / packet capture logic
/internal/flow        — flow aggregation (source, dest, byte count, duration)
/internal/kafka       — publishes flow records to a Kafka topic
/internal/storage     — PostgreSQL persistence layer
```

See [DESIGN.md](DESIGN.md) for package boundaries, type ownership, and shutdown/lifecycle design.

## Running locally

The full pipeline (`cmd/argos`) isn't wired up yet. To exercise Kafka publishing on its own:

```
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
	--topic some-topic --bootstrap-server localhost:9092
go run ./cmd/kafkasmoke
```

See [cmd/kafkasmoke](cmd/kafkasmoke/main.go) for details.

## License

TBD