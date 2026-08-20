# Argos

A Go-based network traffic monitor built for hands-on learning in raw sockets, packet capture, and flow-level telemetry. Captures traffic on a network interface, parses packet headers, and aggregates flows (source, destination, byte count, duration) into a minimal NetFlow-style summary. Published to Kafka and persisted to PostgreSQL.

## Status

Early development. Capture, flow aggregation, Kafka publishing, and PostgreSQL storage are all
implemented and tested — each with an automated fake-based test suite, plus manual verification
against real infrastructure (`cmd/kafkasmoke`, `cmd/storagesmoke`). `cmd/argos` pipeline wiring,
the last piece before the MVP is complete, is not yet built.

## Requirements

- Go 1.26+
- Kafka (local instance via Docker)
- PostgreSQL (local instance via Docker)
- Root/elevated privileges (or `CAP_NET_RAW`) for raw socket packet capture

## Project Structure

```
/cmd/argos            — main package; wires stages together, owns startup/shutdown
/internal/capture     — raw socket / packet capture logic
/internal/flow        — flow aggregation (source, dest, byte count, duration)
/internal/kafka       — publishes flow records to a Kafka topic
/internal/storage     — consumes flow records from Kafka, persists them to PostgreSQL
```

See [DESIGN.md](DESIGN.md) for package boundaries, type ownership, and shutdown/lifecycle design.

## Running locally

The full pipeline (`cmd/argos`) isn't wired up yet, but `kafka` and `storage` can each be
exercised against real infrastructure on their own.

Copy `.env.example` to `.env` and fill in real values first — `docker-compose.yml` reads
Postgres's user/password/db/port from it. `POSTGRES_PORT` in particular may need to be something
other than `5432` if you already have a local Postgres instance using that port.

```
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
	--topic flow-records --bootstrap-server localhost:9092
go run ./cmd/kafkasmoke      # publishes one fake flow record to Kafka
set -a && source .env && set +a
go run ./cmd/storagesmoke    # consumes it, persists it to Postgres, and prints the row back
```

See [cmd/kafkasmoke](cmd/kafkasmoke/main.go) and [cmd/storagesmoke](cmd/storagesmoke/main.go) for
details.

## License

TBD