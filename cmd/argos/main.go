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

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.Run(ctx) })
	g.Go(func() error { return f.Run(ctx, bridgePackets(c.Packets())) })
	g.Go(func() error { return k.Run(ctx, bridgeFlows(f.Flushed())) })
	g.Go(func() error { return s.Run(ctx) })
	return g.Wait()
}

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
