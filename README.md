# Argos

A Go-based network traffic monitor built for hands-on learning in raw sockets, packet capture, and flow-level telemetry. Captures traffic on a network interface, parses packet headers, and aggregates flows (source, destination, byte count, duration) into a minimal NetFlow-style summary. Published to Kafka and persisted to PostgreSQL.

## Status

Early development. Core capture and parsing logic is being built incrementally.

## Requirements

- Go 1.22+
- Kafka (local instance via Docker)
- PostgreSQL
- Root/elevated privileges (or `CAP_NET_RAW`) for raw socket packet capture

## Project Structure

```
/cmd/argos            — main package; wires stages together, owns startup/shutdown
/internal/capture     — raw socket / packet capture logic
/internal/flow        — flow aggregation (source, dest, byte count, duration)
/internal/kafka       — producer/consumer integration
/internal/storage     — PostgreSQL persistence layer
```

See [DESIGN.md](DESIGN.md) for package boundaries, type ownership, and shutdown/lifecycle design.

## Running locally

Setup instructions to be added as the project takes shape.

## License

TBD