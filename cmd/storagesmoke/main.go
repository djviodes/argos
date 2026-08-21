// Command storagesmoke manually verifies that the storage package can consume
// a flow record from a real Kafka broker and persist it to a real PostgreSQL
// database.
//
// Before running, start the broker and database, create the topic, and publish
// a record for storage to find:
//
//	docker compose up -d
//	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create \
//		--topic flow-records --bootstrap-server localhost:9092
//	go run ./cmd/kafkasmoke
//
// Then load the Postgres credentials and run this program. It consumes for ten
// seconds, then prints every row in flow_records so the persisted values can be
// compared by eye against what kafkasmoke published:
//
//	set -a && source .env && set +a
//	go run ./cmd/storagesmoke
package main

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/djviodes/argos/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// dbRecord mirrors a row of the flow_records table, with its fields in the
// column order SELECT * returns so a scanned row can be printed back out.
type dbRecord struct {
	ID          pgtype.UUID
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

func main() {
	closingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD")),
		Host:   "localhost:" + os.Getenv("POSTGRES_PORT"),
		Path:   "/" + os.Getenv("POSTGRES_DB"),
	}
	pgConnString := u.String()

	s, newStorageErr := storage.New(closingCtx, pgConnString, "localhost:9092", "flow-records", "argos-flow-storage")
	if newStorageErr != nil {
		log.Fatalf("creating new storage: %v", newStorageErr)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := s.Run(closingCtx)
		if err != nil {
			log.Fatalf("starting storage run: %v", err)
		}
	}()

	wg.Wait()

	ctx := context.Background()

	conn, connErr := pgx.Connect(ctx, pgConnString)
	if connErr != nil {
		log.Fatalf("Unable to connect to database: %v", connErr)
	}
	defer conn.Close(ctx)

	rows, queryErr := conn.Query(ctx, "SELECT * FROM flow_records")
	if queryErr != nil {
		log.Fatalf("querying the db failed: %v", queryErr)
	}
	defer rows.Close()

	for rows.Next() {
		var record dbRecord

		scanErr := rows.Scan(
			&record.ID,
			&record.SrcIP,
			&record.DstIP,
			&record.SrcPort,
			&record.DstPort,
			&record.Protocol,
			&record.ByteCount,
			&record.PacketCount,
			&record.FirstSeen,
			&record.LastSeen,
		)
		if scanErr != nil {
			log.Fatalf("row scan failed: %v", scanErr)
		}

		fmt.Printf("id=%v src_ip=%v dst_ip=%v src_port=%d dst_port=%d protocol=%d byte_count=%d packet_count=%d first_seen=%v last_seen=%v\n",
			record.ID, record.SrcIP, record.DstIP, record.SrcPort, record.DstPort,
			record.Protocol, record.ByteCount, record.PacketCount, record.FirstSeen, record.LastSeen,
		)
	}
}
