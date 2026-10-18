//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"

	"example.com/binance-alerts/services/market-data/internal/binance"
	kafkapub "example.com/binance-alerts/services/market-data/internal/kafka"
	"example.com/binance-alerts/services/market-data/internal/postgres"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"example.com/binance-alerts/services/market-data/internal/tracking"
)

type temporarilyUnavailableTracking struct {
	store    *postgres.Store
	attempts atomic.Int64
}

func (r *temporarilyUnavailableTracking) ApplyTracking(ctx context.Context, c tracking.Change) error {
	if r.attempts.Add(1) == 1 {
		return errors.New("temporary database failure")
	}
	return r.store.ApplyTracking(ctx, c)
}
func (r *temporarilyUnavailableTracking) RejectTracking(ctx context.Context, topic string, partition int32, offset int64, reason string) error {
	return r.store.RejectTracking(ctx, topic, partition, offset, reason)
}
func until(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
func TestIntegrationTrackingKafkaDatabaseAndWebSocket(t *testing.T) {
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		t.Fatal("TEST_KAFKA_BROKERS is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := setup(t)
	topic := fmt.Sprintf("test.tracking.%d", time.Now().UnixNano())
	groupID := topic + ".consumer"
	admin, err := sarama.NewClusterAdmin(brokers, kafkapub.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.DeleteTopic(topic); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := admin.DeleteConsumerGroup(groupID); err != nil {
			t.Error(err)
		}
	}()
	publisher, err := kafkapub.New(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	subscribe := make(chan []string, 10)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			ID     string   `json:"id"`
		}
		if conn.ReadJSON(&request) != nil {
			return
		}
		if request.Method != "SUBSCRIBE" {
			t.Error("missing subscribe")
		}
		select {
		case subscribe <- request.Params:
		case <-ctx.Done():
			return
		}
		if conn.WriteJSON(map[string]any{"id": request.ID, "result": nil}) != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer source.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	snapshots := make(chan tracking.Snapshot, 1)
	pollDone := make(chan error, 1)
	sourceDone := make(chan error, 1)
	go func() {
		defer close(snapshots)
		pollDone <- tracking.Poll(workCtx, store, 5*time.Millisecond, snapshots)
	}()
	go func() {
		sourceDone <- tracking.Supervise(workCtx, snapshots, func(ctx context.Context, symbols []string, ready func(bool)) error {
			client := &binance.Client{URL: "ws" + strings.TrimPrefix(source.URL, "http"), Symbols: symbols, RetryDelay: time.Millisecond, OnState: ready, Handle: func(ctx context.Context, raw []byte) error { return prices.Handle(ctx, raw, store) }}
			return client.Run(ctx)
		}, nil, nil)
	}()
	defer func() {
		stopWork()
		for _, done := range []<-chan error{pollDone, sourceDone} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("source supervisor did not stop")
			}
		}
	}()
	repo := &temporarilyUnavailableTracking{store: store}
	startConsumer := func() (context.CancelFunc, <-chan error) {
		consumerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- kafkapub.RunTracking(consumerCtx, brokers, groupID, topic, &kafkapub.TrackingHandler{Repository: repo, RetryDelay: time.Millisecond})
		}()
		return stop, done
	}
	stopConsumer, consumerDone := startConsumer()
	defer func() {
		stopConsumer()
		select {
		case <-consumerDone:
		case <-time.After(5 * time.Second):
			t.Error("tracking consumer did not stop")
		}
	}()
	send := func(symbol string, enabled bool, version int64) {
		raw, _ := json.Marshal(map[string]any{"schema_version": 1, "event_type": "InstrumentTrackingChanged", "symbol": symbol, "enabled": enabled, "version": version})
		if err := publisher.Publish(ctx, prices.Record{EventID: fmt.Sprintf("%s:%d", symbol, version), Key: []byte(symbol), Value: raw}); err != nil {
			t.Fatal(err)
		}
	}
	expectSubscription := func(want ...string) {
		for {
			select {
			case got := <-subscribe:
				if slices.Equal(got, want) {
					return
				}
			case <-ctx.Done():
				t.Fatalf("subscription %v was not applied", want)
			}
		}
	}
	send("ETHUSDT", true, 1)
	expectSubscription("ethusdt@aggTrade")
	send("BTCUSDT", true, 1)
	expectSubscription("btcusdt@aggTrade", "ethusdt@aggTrade")
	send("BTCUSDT", false, 3)
	send("BTCUSDT", true, 2)
	expectSubscription("ethusdt@aggTrade")
	if err := publisher.Publish(ctx, prices.Record{EventID: "invalid", Key: []byte("BTCUSDT"), Value: []byte(`{"enabled":"bad"}`)}); err != nil {
		t.Fatal(err)
	}
	until(t, ctx, func() bool {
		var n int
		err := store.Pool.QueryRow(ctx, "SELECT count(*) FROM tracking_rejections WHERE topic=$1", topic).Scan(&n)
		return err == nil && n == 1
	})
	// Закрытие group сохраняет отмеченные offsets; состояние остаётся в БД.
	stopConsumer()
	select {
	case err := <-consumerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer stopped: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopConsumer, consumerDone = startConsumer()
	send("BTCUSDT", true, 4)
	expectSubscription("btcusdt@aggTrade", "ethusdt@aggTrade")
	until(t, ctx, func() bool {
		var version int64
		err := store.Pool.QueryRow(ctx, "SELECT version FROM instrument_tracking WHERE symbol='BTCUSDT'").Scan(&version)
		return err == nil && version == 4
	})
	if repo.attempts.Load() < 5 {
		t.Fatalf("missing retries or commands: attempts=%d", repo.attempts.Load())
	}
}
