# Argos

A Go-based network traffic monitor built for hands-on learning in raw sockets, packet capture, and flow-level telemetry. Captures traffic on a network interface, parses packet headers, and aggregates flows (source, destination, byte count, duration) into a minimal NetFlow-style summary. Published to Kafka and persisted to PostgreSQL.

## Status

MVP complete. All four pipeline stages — capture, flow aggregation, Kafka publishing, and
PostgreSQL storage — are implemented and tested, and `cmd/argos` wires them together into a
single binary with signal-driven graceful shutdown. End-to-end runtime verification against a
real interface is still pending a Linux test environment: `capture` opens a raw `AF_PACKET`
socket, which only exists on Linux, so `cmd/argos` has been verified to compile cleanly for Linux
(`GOOS=linux go build ./...`) but not yet run live.

## Requirements

- Go 1.26+
- Linux (to build or run `cmd/argos` itself — raw `AF_PACKET` sockets are Linux-only; the other
  packages and `cmd/kafkasmoke`/`cmd/storagesmoke` build on any platform)
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

Copy `.env.example` to `.env` and fill in real values first — `docker-compose.yml` reads
Postgres's user/password/db/port from it, and `cmd/argos` reads all nine variables at startup.
`POSTGRES_PORT` in particular may need to be something other than `5432` if you already have a
local Postgres instance using that port.

`kafka` and `storage` can each be exercised individually against real infrastructure without the
full pipeline:

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

Running the full pipeline (`cmd/argos`) additionally requires a Linux host, root/`CAP_NET_RAW`
privileges, and `ARGOS_INTERFACE` set to a real interface name (see `ip link show` on the target
machine):

```
set -a && source .env && set +a
sudo go run ./cmd/argos
```

## License

TBD