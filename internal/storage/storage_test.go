package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
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

var (
	errFakeFetch  = errors.New("fetching messages failed")
	errFakeCommit = errors.New("committing messages failed")
)

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

func newTestMessageBytes(t *testing.T) (kafkaMessage, []byte) {
	msg := kafkaMessage{
		SrcIP:       netip.AddrFrom4([4]byte{0xc0, 0xa8, 0x01, 0x0a}),
		DstIP:       netip.AddrFrom4([4]byte{0x5d, 0xb8, 0xd8, 0x22}),
		SrcPort:     0xd431,
		DstPort:     0x01bb,
		Protocol:    0x06,
		ByteCount:   1500,
		PacketCount: 1,
		FirstSeen:   time.Now().Add(-5 * time.Second),
		LastSeen:    time.Now(),
	}

	value, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("serializing kafka message: %v", err)
	}

	return msg, value
}

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
	t.Run("malformedMessage", func(t *testing.T) {
		fakeReader := &fakeKafkaReader{messages: make(chan kafkago.Message)}
		fakePGWriter := &fakePostgresWriter{}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx := context.Background()
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		fakeReader.messages <- kafkago.Message{Value: []byte("not valid json")}

		select {
		case err := <-errCh:
			if err == nil {
				t.Error("expected non-nil err but got nil")
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

func TestRunPersistsRecord(t *testing.T) {
	t.Run("validRecord", func(t *testing.T) {
		msg, value := newTestMessageBytes(t)

		fakeReader := &fakeKafkaReader{messages: make(chan kafkago.Message)}
		fakePGWriter := &fakePostgresWriter{}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		fakeReader.messages <- kafkago.Message{Value: value}

		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("expected nil err but got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Run did not return")
		}

		if len(fakePGWriter.execArgs) != 1 {
			t.Errorf("got postgres writer arg count %d, want postgres writer arg count 1", len(fakePGWriter.execArgs))
		}

		if fakePGWriter.execArgs[0][0].(netip.Addr) != msg.SrcIP {
			t.Errorf("got src IP %#x, want src IP %#x", fakePGWriter.execArgs[0][0], msg.SrcIP)
		}

		if fakePGWriter.execArgs[0][1].(netip.Addr) != msg.DstIP {
			t.Errorf("got dst IP %#x, want dst IP %#x", fakePGWriter.execArgs[0][1], msg.DstIP)
		}

		if fakePGWriter.execArgs[0][2].(uint16) != msg.SrcPort {
			t.Errorf("got src port %#x, want src port %#x", fakePGWriter.execArgs[0][2], msg.SrcPort)
		}

		if fakePGWriter.execArgs[0][3].(uint16) != msg.DstPort {
			t.Errorf("got dst port %#x, want dst port %#x", fakePGWriter.execArgs[0][3], msg.DstPort)
		}

		if fakePGWriter.execArgs[0][4].(uint8) != msg.Protocol {
			t.Errorf("got protocol %#x, want protocol %#x", fakePGWriter.execArgs[0][4], msg.Protocol)
		}

		if fakePGWriter.execArgs[0][5].(int) != msg.ByteCount {
			t.Errorf("got byte count %d, want byte count %d", fakePGWriter.execArgs[0][5], msg.ByteCount)
		}

		if fakePGWriter.execArgs[0][6].(int) != msg.PacketCount {
			t.Errorf("got packet count %d, want packet count %d", fakePGWriter.execArgs[0][6], msg.PacketCount)
		}

		if !fakePGWriter.execArgs[0][7].(time.Time).Equal(msg.FirstSeen) {
			t.Errorf("got first seen %v, want first seen %v", fakePGWriter.execArgs[0][7], msg.FirstSeen)
		}

		if !fakePGWriter.execArgs[0][8].(time.Time).Equal(msg.LastSeen) {
			t.Errorf("got last seen %v, want last seen %v", fakePGWriter.execArgs[0][8], msg.LastSeen)
		}

		if len(fakeReader.commitCalls) != 1 {
			t.Errorf("got reader commit calls %d, want reader commit calls 1", len(fakeReader.commitCalls))
		}

		if !bytes.Equal(fakeReader.commitCalls[0].Value, value) {
			t.Error("readers committed value does not match expected value")
		}

		if !fakeReader.closed {
			t.Error("expected fakeReader to be closed")
		}
	})
}

func TestRunExecRetriesThenSucceeds(t *testing.T) {
	t.Run("retriesOnceThenSucceeds", func(t *testing.T) {
		_, value := newTestMessageBytes(t)

		fakeReader := &fakeKafkaReader{messages: make(chan kafkago.Message)}
		fakePGWriter := &fakePostgresWriter{failCount: 1}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		fakeReader.messages <- kafkago.Message{Value: value}
		time.Sleep(300 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("expected nil err but got %v", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Run did not return")
		}

		if fakePGWriter.calls != 2 {
			t.Errorf("got writer call count %d, want writer call count 2", fakePGWriter.calls)
		}

		if len(fakeReader.commitCalls) != 1 {
			t.Errorf("got reader commit calls %d, want reader commit calls 1", len(fakeReader.commitCalls))
		}

		if !fakeReader.closed {
			t.Error("expected fakeReader to be closed")
		}
	})
}

func TestRunExecExceedsRetryLimit(t *testing.T) {
	t.Run("exceedsRetryLimit", func(t *testing.T) {
		_, value := newTestMessageBytes(t)

		fakeReader := &fakeKafkaReader{messages: make(chan kafkago.Message)}
		fakePGWriter := &fakePostgresWriter{failCount: 10}
		s := &Storage{kafkaReader: fakeReader, postgresWriter: fakePGWriter}
		ctx := context.Background()
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.Run(ctx)
		}()

		fakeReader.messages <- kafkago.Message{Value: value}

		select {
		case err := <-errCh:
			if !errors.Is(err, errFakeExec) {
				t.Errorf("got error %v, want error %v", err, errFakeExec)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
		}

		if fakePGWriter.calls != 5 {
			t.Errorf("got postgres writer calls %d, want postgres writer calls 5", fakePGWriter.calls)
		}

		if len(fakeReader.commitCalls) != 0 {
			t.Errorf("got reader commit calls %d, want reader commit calls 0", len(fakeReader.commitCalls))
		}

		if !fakeReader.closed {
			t.Error("expected fakeReader to be closed")
		}
	})
}

func TestRunExecRetryCtxCancelled(t *testing.T) {

}

func TestRunCommitError(t *testing.T) {

}
