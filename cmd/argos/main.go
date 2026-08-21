// Command argos captures live network traffic on an interface, aggregates it
// into flow records, publishes those records to a Kafka topic, and consumes
// them back off that topic to persist in PostgreSQL.
//
// All configuration is read from the environment at startup:
//
//	ARGOS_INTERFACE    interface to capture on (e.g. eth0; see "ip link show")
//	KAFKA_BROKER_ADDR  broker address (e.g. localhost:9092)
//	KAFKA_TOPIC        topic flow records are published to and consumed from
//	KAFKA_GROUP_ID     consumer group the storage stage reads as
//	POSTGRES_HOST      database host
//	POSTGRES_PORT      database port
//	POSTGRES_USER      database user
//	POSTGRES_PASSWORD  database password
//	POSTGRES_DB        database name
//
// Capture opens a raw AF_PACKET socket, so argos runs only on Linux and needs
// either root or CAP_NET_RAW:
//
//	set -a && source .env && set +a
//	sudo go run ./cmd/argos
//
// SIGINT or SIGTERM starts a graceful shutdown: each stage stops taking new
// work and drains what it is already holding, within a bounded window, before
// the process exits.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/djviodes/argos/internal/capture"
	"github.com/djviodes/argos/internal/flow"
	"github.com/djviodes/argos/internal/kafka"
	"github.com/djviodes/argos/internal/storage"
	"golang.org/x/sync/errgroup"
)

var postgresPassword = os.Getenv("POSTGRES_PASSWORD")
var postgresUser = os.Getenv("POSTGRES_USER")
var postgresDb = os.Getenv("POSTGRES_DB")
var postgresPort = os.Getenv("POSTGRES_PORT")
var postgresHost = os.Getenv("POSTGRES_HOST")
var argosInterface = os.Getenv("ARGOS_INTERFACE")
var kafkaBrokerAddr = os.Getenv("KAFKA_BROKER_ADDR")
var kafkaTopic = os.Getenv("KAFKA_TOPIC")
var kafkaGroupID = os.Getenv("KAFKA_GROUP_ID")

func main() {
	if err := run(); err != nil {
		slog.Error("run failed", "err", err)
		os.Exit(1)
	}
}

// run constructs the four pipeline stages, wires them together, and blocks
// until every one of them has exited. Each stage runs in its own goroutine
// under an errgroup, so the first stage to fail cancels ctx for the rest and
// run returns that stage's error once they have all finished shutting down.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(postgresUser, postgresPassword),
		Host:   postgresHost + ":" + postgresPort,
		Path:   "/" + postgresDb,
	}
	pgConnString := u.String()

	c, captureErr := capture.New(argosInterface)
	if captureErr != nil {
		return fmt.Errorf("initializing new capture: %w", captureErr)
	}

	defer c.Close()

	f := flow.New()
	k, kafkaErr := kafka.New(kafkaBrokerAddr, kafkaTopic)
	if kafkaErr != nil {
		return fmt.Errorf("initializing new kafka: %w", kafkaErr)
	}

	s, storageErr := storage.New(ctx, pgConnString, kafkaBrokerAddr, kafkaTopic, kafkaGroupID)
	if storageErr != nil {
		return fmt.Errorf("initializing new storage: %w", storageErr)
	}

	defer s.Close()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.Run(ctx) })
	g.Go(func() error { return f.Run(ctx, bridgePackets(c.Packets())) })
	g.Go(func() error { return k.Run(ctx, bridgeFlows(f.Flushed())) })
	g.Go(func() error { return s.Run(ctx) })
	return g.Wait()
}

// bridgePackets adapts capture's output channel to the channel type flow
// consumes. Go's channel types are invariant, so a <-chan capture.Packet
// cannot be passed where a <-chan flow.PacketSource is expected even though
// capture.Packet satisfies flow.PacketSource — the values have to be copied
// across onto a channel of the interface type. cmd/argos owns this adapter
// because it is the only place allowed to know both concrete types. The
// returned channel closes once in closes, propagating shutdown downstream.
func bridgePackets(in <-chan capture.Packet) <-chan flow.PacketSource {
	out := make(chan flow.PacketSource)

	go func() {
		defer close(out)

		for p := range in {
			out <- p
		}
	}()

	return out
}

// bridgeFlows adapts flow's output channel to the channel type kafka consumes,
// for the same reason bridgePackets exists: flow.FlowRecord satisfies
// kafka.FlowSource, but <-chan flow.FlowRecord and <-chan kafka.FlowSource are
// distinct types and neither is assignable to the other. The returned channel
// closes once in closes.
func bridgeFlows(in <-chan flow.FlowRecord) <-chan kafka.FlowSource {
	out := make(chan kafka.FlowSource)

	go func() {
		defer close(out)

		for r := range in {
			out <- r
		}
	}()

	return out
}
