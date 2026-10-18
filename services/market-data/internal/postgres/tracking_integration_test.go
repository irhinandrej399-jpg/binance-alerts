//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/postgres"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"sync"
	"testing"
)

func symbols(t *testing.T, s *postgres.Store, want ...string) {
	t.Helper()
	got, err := s.TrackingSymbols(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("symbols=%v want=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("symbols=%v want=%v", got, want)
		}
	}
}
func TestIntegrationTrackingVersionsAndRestart(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for _, change := range []tracking.Change{{"BTCUSDT", true, 1}, {"BTCUSDT", true, 1}, {"BTCUSDT", false, 3}, {"BTCUSDT", true, 2}, {"ETHUSDT", true, 1}} {
		if err := s.ApplyTracking(ctx, change); err != nil {
			t.Fatal(err)
		}
	}
	symbols(t, s, "ETHUSDT")
	if err := s.ApplyTracking(ctx, tracking.Change{"BTCUSDT", true, 3}); !errors.Is(err, tracking.ErrVersionConflict) {
		t.Fatalf("err=%v", err)
	}
	symbols(t, s, "ETHUSDT")
	if err := s.ApplyTracking(ctx, tracking.Change{"BTCUSDT", true, 4}); err != nil {
		t.Fatal(err)
	}
	restored := &postgres.Store{Pool: s.Pool}
	symbols(t, restored, "BTCUSDT", "ETHUSDT")
	if err := restored.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	symbols(t, restored, "BTCUSDT", "ETHUSDT")
}
func TestIntegrationConcurrentTrackingKeepsHighestVersion(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := int64(1); i <= 20; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			if err := s.ApplyTracking(ctx, tracking.Change{"BTCUSDT", version%2 != 0, version}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	symbols(t, s)
	var version int64
	if err := s.Pool.QueryRow(ctx, "SELECT version FROM instrument_tracking WHERE symbol='BTCUSDT'").Scan(&version); err != nil || version != 20 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}
func TestIntegrationTrackingRejectionIsDurableAndIdempotent(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	for range 2 {
		if err := s.RejectTracking(ctx, "tracking", 1, 42, "invalid command"); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM tracking_rejections WHERE topic='tracking' AND partition_id=1 AND offset_id=42").Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
