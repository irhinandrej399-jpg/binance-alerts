//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"example.com/binance-alerts/services/market-data/internal/postgres"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setup(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required; use a disposable test database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_outbox_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &postgres.Store{Pool: pool}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}
func record(id string) prices.Record {
	return prices.Record{EventID: id, Key: []byte("BTCUSDT"), Value: []byte(`{"event_id":"` + id + `"}`)}
}
func pending(t *testing.T, s *postgres.Store) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestIntegrationOutboxDuplicateAndDelivery(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for _, id := range []string{"one", "one", "two"} {
		if err := s.Append(ctx, record(id)); err != nil {
			t.Fatal(err)
		}
	}
	if n := pending(t, s); n != 2 {
		t.Fatalf("pending=%d", n)
	}
	var ids []string
	n, err := s.Deliver(ctx, 10, func(_ context.Context, r prices.Record) error {
		ids = append(ids, r.EventID)
		if string(r.Key) != "BTCUSDT" {
			t.Error("key changed")
		}
		return nil
	})
	if err != nil || n != 2 || len(ids) != 2 || ids[0] != "one" || ids[1] != "two" || pending(t, s) != 0 {
		t.Fatalf("n=%d err=%v ids=%v", n, err, ids)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
}
func TestIntegrationRollbackAfterPartialPublishAndRetry(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for _, id := range []string{"one", "two"} {
		if err := s.Append(ctx, record(id)); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	sentinel := errors.New("ack lost")
	n, err := s.Deliver(ctx, 10, func(context.Context, prices.Record) error {
		calls++
		if calls == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || n != 0 || pending(t, s) != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	var ids []string
	n, err = s.Deliver(ctx, 10, func(_ context.Context, r prices.Record) error { ids = append(ids, r.EventID); return nil })
	if err != nil || n != 2 || len(ids) != 2 || ids[0] != "one" || ids[1] != "two" {
		t.Fatalf("n=%d err=%v ids=%v", n, err, ids)
	}
}
func TestIntegrationConcurrentRelayDoesNotPublishLockedBatch(t *testing.T) {
	s := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Append(ctx, record("one")); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	done := make(chan error, 1)
	go func() {
		_, err := s.Deliver(ctx, 1, func(context.Context, prices.Record) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first relay did not start")
	}
	calls := 0
	n, err := s.Deliver(ctx, 1, func(context.Context, prices.Record) error { calls++; return nil })
	if err != nil || n != 0 || calls != 0 {
		t.Fatalf("second relay n=%d err=%v calls=%d", n, err, calls)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop")
	}
}
func TestIntegrationPurgeDoesNotDeletePending(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for _, id := range []string{"sent-old", "pending-old", "sent-new"} {
		if err := s.Append(ctx, record(id)); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Pool.Exec(ctx, "UPDATE outbox SET published_at=CASE WHEN event_id='sent-old' THEN now()-interval '2 days' ELSE now() END WHERE event_id LIKE 'sent-%'")
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.Purge(ctx, 24*time.Hour, 100)
	if err != nil || n != 1 || pending(t, s) != 1 {
		t.Fatalf("removed=%d err=%v", n, err)
	}
	var total int
	if err = s.Pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&total); err != nil || total != 2 {
		t.Fatalf("total=%d err=%v", total, err)
	}
}
