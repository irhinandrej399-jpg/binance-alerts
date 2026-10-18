//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/binance"
	kafkapub "example.com/binance-alerts/services/market-data/internal/kafka"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"example.com/binance-alerts/services/market-data/internal/relay"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type acknowledgedThenFailed struct{ publisher *kafkapub.Publisher }

func (p acknowledgedThenFailed) Publish(ctx context.Context, r prices.Record) error {
	if err := p.publisher.Publish(ctx, r); err != nil {
		return err
	}
	return errors.New("simulated crash after broker ack before database commit")
}
func TestIntegrationWebSocketOutboxKafkaAndAckCommitGap(t *testing.T) {
	brokers := strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		t.Fatal("TEST_KAFKA_BROKERS is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := setup(t)
	topic := fmt.Sprintf("test.market.prices.%d", time.Now().UnixNano())
	conf := kafkapub.Config()
	admin, err := sarama.NewClusterAdmin(brokers, conf)
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
	pub, err := kafkapub.New(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	up := websocket.Upgrader{}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request any
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"id":"market-data","result":null}`))
		raw := []byte(`{"e":"aggTrade","E":1770000000125,"s":"BTCUSDT","a":12345,"p":"60000.01000000","T":1770000000123}`)
		for range 2 {
			if conn.WriteMessage(websocket.TextMessage, raw) != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer source.Close()
	inputCtx, stop := context.WithCancel(ctx)
	defer stop()
	saves := 0
	client := &binance.Client{URL: "ws" + strings.TrimPrefix(source.URL, "http"), Symbols: []string{"BTCUSDT"}, Handle: func(ctx context.Context, raw []byte) error {
		if err := prices.Handle(ctx, raw, s); err != nil {
			return err
		}
		saves++
		if saves == 2 {
			stop()
		}
		return nil
	}}
	if err := client.Session(inputCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("source err=%v", err)
	}
	if saves != 2 || pending(t, s) != 1 {
		t.Fatalf("saves=%d, pending=%d", saves, pending(t, s))
	}
	// Подтверждённая Kafka запись остаётся pending при ошибке до commit БД.
	if _, err = relay.Batch(ctx, s, acknowledgedThenFailed{pub}, 10); err == nil || pending(t, s) != 1 {
		t.Fatalf("ack/commit gap err=%v", err)
	}
	if n, err := relay.Batch(ctx, s, pub, 10); err != nil || n != 1 || pending(t, s) != 0 {
		t.Fatalf("retry n=%d err=%v", n, err)
	}
	consumer, err := sarama.NewConsumer(brokers, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()
	var received []*sarama.ConsumerMessage
	for len(received) < 2 {
		select {
		case m, ok := <-partition.Messages():
			if !ok {
				t.Fatal("consumer closed")
			}
			received = append(received, m)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if string(received[0].Key) != "BTCUSDT" || !bytes.Equal(received[0].Value, received[1].Value) {
		t.Fatal("retry changed key or event payload")
	}
	var event prices.Event
	if err = json.Unmarshal(received[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	if event.Price != "60000.01000000" || event.EventID != "binance:spot:aggTrade:BTCUSDT:12345" {
		t.Fatalf("event=%+v", event)
	}
}
