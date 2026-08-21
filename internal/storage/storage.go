// Package storage consumes flow records from a Kafka topic and persists
// them to PostgreSQL.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
)

// kafkaMessage is the JSON shape storage expects to find on the topic,
// matching what the kafka package publishes. storage unmarshals into its own
// private struct rather than importing a type from upstream because the
// boundary between the two stages is the topic itself — bytes on the wire —
// not a Go API.
type kafkaMessage struct {
	SrcIP       netip.Addr
	DstIP       netip.Addr
	SrcPort     uint16
	DstPort     uint16
	Protocol    uint8
	ByteCount   int
	PacketCount int
	FirstSeen   time.Time
	LastSeen    time.Time
}

// kafkaReader is the subset of kafka-go's *Reader that Storage depends on,
// declared as an interface so tests can substitute a fake and run without a
// live broker.
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// postgresWriter is the subset of pgxpool's *Pool that Storage depends on,
// declared as an interface so tests can substitute a fake and run without a
// live database.
type postgresWriter interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Close()
}

// Storage consumes flow records from a Kafka topic and persists them to
// PostgreSQL.
type Storage struct {
	kafkaReader    kafkaReader
	postgresWriter postgresWriter
}

// Close closes the Kafka reader and the PostgreSQL connection pool. It is safe
// to call more than once and from more than one goroutine — both underlying
// closes already guard themselves, pgxpool with a sync.Once and kafka-go with
// a mutex-protected flag — so a caller that defers Close immediately after a
// successful New does not conflict with Run closing them on its way out, and
// Storage needs no such guard of its own. Errors from the underlying closes are
// discarded, since there is no meaningful recovery from failing to release
// resources that are already being abandoned.
func (s *Storage) Close() {
	s.kafkaReader.Close()
	s.postgresWriter.Close()
}

// New returns a Storage ready to consume flow records from kafkaTopic as
// consumer group kafkaGroupID on the broker at kafkaBrokerAddr, and persist
// them to the PostgreSQL database at pgConnString.
func New(ctx context.Context, pgConnString, kafkaBrokerAddr, kafkaTopic, kafkaGroupID string) (*Storage, error) {
	if pgConnString == "" {
		return nil, errors.New("pgConnString must not be empty")
	}

	if kafkaBrokerAddr == "" {
		return nil, errors.New("kafkaBrokerAddr must not be empty")
	}

	if kafkaTopic == "" {
		return nil, errors.New("kafkaTopic must not be empty")
	}

	if kafkaGroupID == "" {
		return nil, errors.New("kafkaGroupID must not be empty")
	}

	pool, err := pgxpool.New(ctx, pgConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to open pool: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{kafkaBrokerAddr},
		Topic:   kafkaTopic,
		GroupID: kafkaGroupID,
	})

	return &Storage{
		kafkaReader:    reader,
		postgresWriter: pool,
	}, nil
}

// Run consumes flow records from Kafka, persisting each to PostgreSQL. A
// record that fails to persist is retried with exponential backoff and its
// Kafka offset left uncommitted, so a transient failure resolves itself and
// a sustained one is retried again after a restart rather than silently
// dropped; after 5 consecutive failures, Run returns the error rather than
// retrying forever. Run also returns if ctx is cancelled or if fetching or
// committing a Kafka message fails, closing the Kafka reader and the
// PostgreSQL connection pool in every case.
func (s *Storage) Run(ctx context.Context) error {
	const query = `
		INSERT INTO flow_records (
			src_ip,
			dst_ip,
			src_port,
			dst_port,
			protocol,
			byte_count,
			packet_count,
			first_seen,
			last_seen
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9
		) ON CONFLICT (src_ip, dst_ip, src_port, dst_port, protocol, first_seen) DO NOTHING;
	`

	defer s.Close()

	for {
		msg, err := s.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil
			}

			return fmt.Errorf("failed to fetch message: %w", err)
		}

		kafkaRecord := kafkaMessage{}
		unmarshalErr := json.Unmarshal(msg.Value, &kafkaRecord)
		if unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal msg: %w", unmarshalErr)
		}

		attempts := 0
		for {
			result, execErr := s.postgresWriter.Exec(
				ctx,
				query,
				kafkaRecord.SrcIP,
				kafkaRecord.DstIP,
				kafkaRecord.SrcPort,
				kafkaRecord.DstPort,
				kafkaRecord.Protocol,
				kafkaRecord.ByteCount,
				kafkaRecord.PacketCount,
				kafkaRecord.FirstSeen,
				kafkaRecord.LastSeen,
			)
			if execErr == nil {
				if result.RowsAffected() == 0 {
					slog.Debug("record already exists, skipped", "rows_affected", result.RowsAffected())
				}

				break
			}

			attempts++
			if attempts >= 5 {
				return fmt.Errorf("insert exceeded 5 retries with error: %w", execErr)
			}

			timeToSleep := time.Duration(1<<attempts) * 100 * time.Millisecond

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(timeToSleep):
				continue
			}

		}

		commitErr := s.kafkaReader.CommitMessages(ctx, msg)
		if commitErr != nil {
			return fmt.Errorf("failed to commit message: %w", commitErr)
		}
	}
}
