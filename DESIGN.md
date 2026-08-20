# Argos — Design

## Purpose

Argos captures live network traffic on a given interface, parses packet headers, and aggregates
observed traffic into flow-level summaries, similar in spirit to NetFlow/IPFIX. Flow summaries
are published to Kafka and persisted to PostgreSQL for later querying and analysis.

This project exists primarily as a hands-on learning vehicle for networking fundamentals
(raw sockets, byte order, packet parsing) applied in Go, ahead of production work involving
network monitoring on Linux-based appliances.

## Architecture

```
[Network Interface]
       |
       v
  [capture]  --raw socket / pcap-->  parses packet headers
       |
       v
   [flow]  --aggregates packets into flow records-->
       |
       v
  [kafka]  --publishes flow records to a topic-->
       |
       v
  [storage]  --consumes from Kafka, persists to Postgres-->
```

Each stage runs as its own long-lived goroutine, connected to its neighbor by a channel. There
is no shared "god" package — every stage is a self-contained Go package with its own types,
under `internal/`.

## Package layout

```
/cmd/argos            — main package; wires concrete stages together, owns startup/shutdown
/internal/capture     — raw socket / pcap capture; defines Packet
/internal/flow        — flow aggregation; defines FlowRecord
/internal/kafka       — publishes flow records to Kafka
/internal/storage     — consumes from Kafka, persists flow records to Postgres
```

Everything lives under `internal/` since this project isn't meant to be imported as a library —
it's a single binary. `cmd/argos/main.go` is the only file that imports all four stage packages
directly; every other package only knows about its immediate neighbor, and only through an
interface (see below).

## Components

### capture

Opens a raw socket (or uses a pcap-based library) on a specified network interface and reads
incoming packets. Responsible for the lowest-level parsing: extracting IP header fields
(source/destination address, protocol) and transport header fields (source/destination port)
from raw bytes, respecting network byte order. Owns and defines the `Packet` type.

### flow

Aggregates individual packets into flow records — grouped by source/destination IP and port,
tracking byte counts and flow duration (first-seen/last-seen timestamps). A flow record is a
simplified analog to a NetFlow/IPFIX flow entry. Owns and defines the `FlowRecord` type. Depends
on `capture` only through a small interface describing what it needs from a packet, not on
`capture`'s concrete type.

### kafka

Publishes completed (or periodically flushed) flow records to a Kafka topic as the transport
layer between capture and storage. Chosen to mirror the event-driven pipeline pattern relevant
to production telemetry ingestion systems. Depends on `flow` only through a small interface
describing what it needs from a flow record.

### storage

Consumes flow records from Kafka and persists them to PostgreSQL, with a schema designed for
querying by source, destination, time range, and volume. Depends on `flow` only through a small
interface describing what it needs from a flow record.

## Type ownership and boundaries

Each stage owns the type it produces, and each stage's *consumer* defines the interface it
depends on — not the producer's concrete type. For example, `flow` doesn't import
`capture.Packet` directly; it declares its own narrow interface describing only what it needs:

```go
// in package flow
type PacketSource interface {
    SrcIP() net.IP
    DstIP() net.IP
    SrcPort() uint16
    DstPort() uint16
    Protocol() uint8
    Len() int
    Timestamp() time.Time
}
```

`capture.Packet` satisfies this interface structurally, with no import from `capture` back to
`flow`. The same pattern repeats at the `flow → kafka` and `flow → storage` boundaries.

This keeps the dependency graph one-directional (`cmd/argos` → each stage; no stage imports
another stage's package) and makes each stage independently testable — `flow` can be tested with
fake packets and never needs raw-socket / root privileges, `kafka` and `storage` can be tested
with fake flow records and never need a running Kafka broker or Postgres instance.

## Lifecycle and shutdown

Each stage is a struct, not a bare function — e.g. `capture.New(iface) (*Capture, error)`, with a
`Run(ctx) error` method and a `Packets() <-chan Packet` accessor. The struct gives each stage a
natural place to hold the resource that needs cleanup (socket, producer handle, DB pool).

Shutdown is coordinated from `main.go`:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

cap, err := capture.New(iface)
if err != nil {
    return err
}
flowAgg := flow.New()
kafkaWriter, err := kafka.New(brokerAddr, topic)
if err != nil {
    return err
}

g, ctx := errgroup.WithContext(ctx)
g.Go(func() error { return cap.Run(ctx) })
g.Go(func() error { return flowAgg.Run(ctx, bridgePackets(cap.Packets())) })
g.Go(func() error { return kafkaWriter.Run(ctx, bridgeFlows(flowAgg.Flushed())) })
g.Go(func() error { return storage.Run(ctx) })

return g.Wait()
```

`bridgePackets` and `bridgeFlows` are small adapters `cmd/argos` owns: each ranges over a
producer's output channel of a concrete type (`<-chan capture.Packet`, `<-chan flow.FlowRecord`)
and re-sends each value onto a channel of the consumer's interface type (`chan flow.PacketSource`,
`chan kafka.FlowSource`). This isn't optional boilerplate — Go's channel types are invariant, so a
`<-chan capture.Packet` can't be passed directly where a `<-chan flow.PacketSource` is expected,
even though `capture.Packet` satisfies that interface (and likewise for `flow.FlowRecord` and
`kafka.FlowSource`). `cmd/argos` is the one place allowed to know about both concrete types at
each boundary, so the bridging belongs there, keeping each stage free of any import from its
upstream neighbor.

- A single root `context.Context`, cancelled on `SIGINT`/`SIGTERM`, is threaded through every
  stage.
- `errgroup` means any stage's error cancels `ctx` for the rest, and `g.Wait()` is the one place
  that blocks until every stage has actually exited.
- Inside each stage's loop, blocking operations are wrapped in a `select` against `ctx.Done()` so
  cancellation is responsive even mid-read/mid-send, not just checked once per loop iteration.
- Shutdown cascades rather than kills: when `capture`'s `ctx` is cancelled, it stops reading and
  closes its output channel. `flow`, ranging over that channel, sees it close, performs a final
  flush of in-progress flow records, then closes its own output channel — and so on downstream
  through `kafka` and `storage`. Work already in flight gets a chance to drain instead of being
  dropped, at the cost of shutdown not being instantaneous.

## Design decisions

- **Package layout**: one package per pipeline stage under `internal/`, with `cmd/argos` as the
  sole wiring point. Chosen over a flat set of top-level packages so `internal/` prevents this
  project from being imported as a library, which it isn't meant to be.
- **Type ownership**: producer owns the concrete type it produces; consumer defines the interface
  it depends on. Chosen over a shared `types`/`model` package so each stage can be tested in
  isolation (particularly `flow`, which shouldn't need root/`CAP_NET_RAW` just to run its tests).
- **Lifecycle**: struct-based stages (not bare functions) coordinated via a single root `context`
  + `errgroup`, with cascading drain-on-shutdown rather than an immediate kill. Chosen so
  in-flight flow records aren't silently dropped on `Ctrl+C`.
- **Packet capture mechanism**: raw `AF_PACKET`/`SOCK_RAW` sockets via `golang.org/x/sys/unix`,
  not a pcap-based library. Chosen to keep the raw-socket/byte-order learning goal intact — a
  pcap wrapper would hand back already-parsed frames, skipping exactly the fundamentals this
  project exists to practice. The socket's `SO_RCVTIMEO` is set to 1 second so `Run`'s blocking
  read periodically returns even with no traffic, keeping `ctx` cancellation responsive without
  needing to select on a raw file descriptor directly. Errors are split by severity: a
  `parsePacket` failure (malformed or out-of-scope input) is logged and the packet is dropped,
  not treated as fatal; a socket-level read error is fatal and propagates up, cascading a
  shutdown through `errgroup`.
- **Flow timeout/expiry strategy**: idle timeout only, fixed at 60 seconds, checked via a
  periodic ticker inside `Flow.Run` rather than a per-flow timer. An active timeout (force-flushing
  a flow that's still receiving traffic after some maximum duration) was considered but deferred —
  idle-only covers the common case without the extra bookkeeping an active timeout would add.
- **Kafka message design**: each flow record is serialized to JSON for the message `Value` (see
  Post-MVP for the Protobuf deferral), with the `Key` built from the flow's 5-tuple and `Time` set
  to the flow's `LastSeen` rather than publish time — when the flow was actually observed is more
  meaningful downstream than when it happened to reach Kafka. The `Writer`'s `Balancer` is `Hash`
  rather than the default `LeastBytes`, so every message for a given flow key routes to the same
  partition, preserving per-flow ordering — `LeastBytes` ignores the key entirely and wouldn't
  guarantee that. A `WriteMessages` failure is treated as fatal, propagating up through `Run` and
  cascading a shutdown via `errgroup`, since it almost always signals a broken connection to the
  broker rather than a bad record — record-level problems (JSON serialization failures) are a
  separate, earlier failure mode already handled before `WriteMessages` is ever called.

## Post-MVP

Decisions made now that intentionally defer real work until after the MVP is functional,
recorded here so the reasoning behind the deferral isn't lost:

- **`storage`'s dead letter queue for records that fail to persist** (highest priority of the
  items below — first thing tackled post-MVP, ahead of the rest of this list): `storage`'s insert
  path retries a record that fails to persist, without committing its Kafka offset, so a transient
  failure resolves itself on the next attempt and a sustained one is retried again after a restart
  rather than silently dropped. After 5 consecutive failures, `storage` treats this as fatal rather
  than retrying forever. The planned fix for what happens to that record: publish it to a separate
  `flow-records-dlq` Kafka topic for later inspection or manual replay, before giving up. A
  Postgres table holding the same information was considered and rejected — the realistic trigger
  for 5 consecutive failures is a sustained Postgres outage, not one malformed record, and in that
  scenario a dead-letter *table* is useless, since it shares Postgres's own failure domain: if the
  database is unreachable, writing the failure record to any table in it fails too. A Kafka topic
  doesn't share that failure domain. Deferred for now (new producer path, new topic, and message
  shape all need designing) but prioritized above the other Post-MVP items below because it closes
  a real data-loss/observability gap, not a performance or tooling-maturity gap like the rest of
  this list — until it's built, a sustained Postgres outage crash-loops `storage` with no record of
  what failed beyond the logs.
- **IPv6 (and other protocols) support**: the project is IPv4-only for now (see Non-goals), but
  `flow.FlowKey` already uses `netip.Addr` rather than `net.IP` specifically so this transition is
  painless later — `netip.Addr` represents IPv4 and IPv6 uniformly, so `FlowKey`'s shape won't
  need to change when IPv6 support is added.
- **`flow`'s flush path becoming a worker pool**: `Flow.Run` is currently a single goroutine that
  both owns the aggregation map and performs the (potentially slow, if the downstream consumer is
  behind) blocking send to `Flushed()`. A large `flushIdle` cycle can delay processing new incoming
  packets until it completes. The planned fix: keep the map single-owner — only one goroutine ever
  touches `f.flows`, so no mutex is needed — but hand records that have already been removed from
  the map off to a small pool of worker goroutines whose only job is the downstream send. This
  decouples "identify what's idle" (fast, sequential) from "wait for someone to receive it"
  (potentially slow, now parallelized). Deferred because it adds real concurrency complexity for a
  benefit that only matters at a scale this project isn't at yet.
- **`kafka`'s message serialization becomes Protobuf**: flow records are currently serialized to
  JSON for the message `Value` — no schema enforcement, but no extra tooling or infrastructure
  either (no `protoc` compiler, no schema registry). Protobuf would enforce the producer/consumer
  contract at compile time and reduce message size, at the cost of a `.proto` schema and generated
  Go code. Deferred deliberately rather than for scope reasons: Protobuf is a real, separate skill
  worth dedicated hands-on practice once the MVP pipeline is functional end-to-end, not something
  to pick up as a side effect of getting `kafka` working for the first time.
- **Formal integration test suite for `kafka` against a real broker**: `kafka`'s automated tests
  use fakes (per the Type ownership pattern above), needing no Docker or live broker to run. Since
  fake-based tests only prove `kafka`'s own logic is internally consistent, not that it actually
  publishes successfully against a real broker, `kafka` was manually verified against a local
  Docker Kafka instance (via `docker-compose.yml` and `cmd/kafkasmoke`) before being considered
  functional. A repeatable, automated integration suite exercising the real `kafka-go` `Writer` is
  still deferred, consistent with keeping this project's Kafka-specific scope narrow for now (see
  the Protobuf entry above).

Still open, to be filled in as real decisions are made:

- Flow record schema / Postgres schema design
- Whether to support multiple interfaces

## Non-goals (for now)

- Deep packet inspection / application-layer protocol parsing
- Real-time alerting or anomaly detection
- Support for non-Linux platforms
- Handling of encrypted or tunneled traffic beyond outer headers
- IPv6 support; protocols other than TCP/UDP (see Post-MVP for IPv6)
