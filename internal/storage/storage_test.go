package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeKafkaReader struct {
	messages    chan kafkago.Message
	fetchErr    error
	commitErr   error
	commitCalls []kafkago.Message
	closed      bool
}

func (k *fakeKafkaReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if k.fetchErr != nil {
		return kafkago.Message{}, k.fetchErr
	}

	select {
	case msg := <-k.messages:
		return msg, nil
	case <-ctx.Done():
		return kafkago.Message{}, ctx.Err()
	}
}
func (k *fakeKafkaReader) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	for _, msg := range msgs {
		k.commitCalls = append(k.commitCalls, msg)
	}

	return k.commitErr
}
func (k *fakeKafkaReader) Close() error { k.closed = true; return nil }

var errFakeFetch = errors.New("fetching messages failed")
var errFakeCommit = errors.New("committing messages failed")

type fakePostgresWriter struct {
	failCount    int
	calls        int
	execArgs     [][]any
	rowsAffected int64
}

func (p *fakePostgresWriter) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.calls++

	p.execArgs = append(p.execArgs, args)

	if p.calls <= p.failCount {
		return pgconn.CommandTag{}, errFakeExec
	}

	s := fmt.Sprintf("INSERT 0 %d", p.rowsAffected)
	commandTag := pgconn.NewCommandTag(s)

	return commandTag, nil
}

var errFakeExec = errors.New("executing messages failed")

func TestNew(t *testing.T) {
	const pgConnString = "postgres://user:pass@localhost:5432/db"
	const brokerAddr = "localhost:9092"
	const topic = "flow-records"
	const groupID = "testGroupID"

	ctx := context.Background()

	tests := []struct {
		name         string
		pgConnString string
		brokerAddr   string
		topic        string
		groupID      string
		wantErr      bool
	}{
		{name: "valid", pgConnString: pgConnString, brokerAddr: brokerAddr, topic: topic, groupID: groupID},
		{name: "emptyPgConnString", brokerAddr: brokerAddr, topic: topic, groupID: groupID, wantErr: true},
		{name: "emptyKafkaBrokerAddr", pgConnString: pgConnString, topic: topic, groupID: groupID, wantErr: true},
		{name: "emptyKafkaTopic", pgConnString: pgConnString, brokerAddr: brokerAddr, groupID: groupID, wantErr: true},
		{name: "emptyKafkaGroupID", pgConnString: pgConnString, brokerAddr: brokerAddr, topic: topic, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(ctx, tt.pgConnString, tt.brokerAddr, tt.topic, tt.groupID)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err %t did not match wantErr %t", (err != nil), tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if s == nil {
				t.Error("s returned nil when it should not have")
			}
		})
	}
}

func TestRunFetchError(t *testing.T) {
	t.Run("duringNormalOperation", func(t *testing.T) {
		fakeReader := &fakeKafkaReader{fetchErr: errFakeFetch}
		fakePGWriter := &fakePostgresWriter{}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx := context.Background()
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		select {
		case err := <-errCh:
			if !errors.Is(err, errFakeFetch) {
				t.Errorf("got error %v, want error %v", err, errFakeFetch)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Run did not return")
		}

		if !fakeReader.closed {
			t.Error("expected fakeReader to be closed")
		}

		if fakePGWriter.calls != 0 {
			t.Errorf("got postgres writer calls %d, want postgres writer calls 0", fakePGWriter.calls)
		}

		if len(fakeReader.commitCalls) != 0 {
			t.Errorf("got reader commit calls %d, want reader commit calls 0", len(fakeReader.commitCalls))
		}
	})
}

func TestRunCtxCancelledWhileFetching(t *testing.T) {
	t.Run("cancelledDuringFetch", func(t *testing.T) {
		fakeReader := &fakeKafkaReader{}
		fakePGWriter := &fakePostgresWriter{}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("expected nil err but got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Run did not return")
		}

		if !fakeReader.closed {
			t.Error("expected fakeReader to be closed")
		}

		if fakePGWriter.calls != 0 {
			t.Errorf("got postgres writer calls %d, want postgres writer calls 0", fakePGWriter.calls)
		}

		if len(fakeReader.commitCalls) != 0 {
			t.Errorf("got reader commit calls %d, want reader commit calls 0", len(fakeReader.commitCalls))
		}
	})
}

func TestRunUnmarshalFailure(t *testing.T) {

}

func TestRunPersistsRecord(t *testing.T) {

}

func TestRunExecRetriesThenSucceeds(t *testing.T) {

}

func TestRunExecExceedsRetryLimit(t *testing.T) {

}

func TestRunExecRetryCtxCancelled(t *testing.T) {

}

func TestRunCommitError(t *testing.T) {

}
