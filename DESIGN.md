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

Each stage is a struct, not a bare function — e.g. `capture.New(iface) *Capture`, with a
`Run(ctx) error` method and a `Packets() <-chan Packet` accessor. The struct gives each stage a
natural place to hold the resource that needs cleanup (socket, producer handle, DB pool).

Shutdown is coordinated from `main.go`:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

g, ctx := errgroup.WithContext(ctx)
g.Go(func() error { return capture.Run(ctx) })
g.Go(func() error { return flow.Run(ctx) })
g.Go(func() error { return kafka.Run(ctx) })
g.Go(func() error { return storage.Run(ctx) })

err := g.Wait()
```

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

Still open, to be filled in as real decisions are made:

- Choice of packet capture library (raw sockets vs. a pcap-based library)
- Flow record schema / Postgres schema design
- Flow timeout/expiry strategy
- Whether to support multiple interfaces

## Non-goals (for now)

- Deep packet inspection / application-layer protocol parsing
- Real-time alerting or anomaly detection
- Support for non-Linux platforms
- Handling of encrypted or tunneled traffic beyond outer headers
