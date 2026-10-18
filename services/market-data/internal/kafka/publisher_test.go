package kafka

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"github.com/IBM/sarama"
	"testing"
)

type fakeSender struct {
	messages []*sarama.ProducerMessage
	err      error
	closed   bool
}

func (f *fakeSender) SendMessage(m *sarama.ProducerMessage) (int32, int64, error) {
	f.messages = append(f.messages, m)
	return 0, 0, f.err
}
func (f *fakeSender) Close() error { f.closed = true; return nil }
func TestConfigRequiresAllAcksAndNoUnboundedRetries(t *testing.T) {
	c := Config()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Producer.RequiredAcks != sarama.WaitForAll || !c.Producer.Return.Successes || c.Producer.Retry.Max != 0 || c.Net.MaxOpenRequests != 1 {
		t.Fatal("unsafe acknowledgement/retry configuration")
	}
}
func TestPublishPreservesKeyValueAndError(t *testing.T) {
	sentinel := errors.New("ack unknown")
	f := &fakeSender{err: sentinel}
	p := &Publisher{topic: "market.prices.v1", factory: func() (sender, error) { return f, nil }}
	r := prices.Record{EventID: "id-1", Key: []byte("BTCUSDT"), Value: []byte(`{"price":"60000.01"}`)}
	if err := p.Publish(context.Background(), r); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if len(f.messages) != 1 {
		t.Fatalf("messages=%d", len(f.messages))
	}
	m := f.messages[0]
	key, _ := m.Key.Encode()
	value, _ := m.Value.Encode()
	if m.Topic != p.topic || string(key) != string(r.Key) || string(value) != string(r.Value) {
		t.Fatalf("message=%+v", m)
	}
}
func TestLazyCreationRetriesAndClose(t *testing.T) {
	attempts := 0
	f := &fakeSender{}
	p := &Publisher{factory: func() (sender, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("broker down")
		}
		return f, nil
	}}
	r := prices.Record{EventID: "id", Key: []byte("BTCUSDT"), Value: []byte(`{}`)}
	if err := p.Publish(context.Background(), r); err == nil {
		t.Fatal("expected creation failure")
	}
	if err := p.Publish(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d", attempts)
	}
	if err := p.Close(); err != nil || !f.closed {
		t.Fatalf("err=%v closed=%v", err, f.closed)
	}
	if err := p.Publish(context.Background(), r); err == nil {
		t.Fatal("closed publisher accepted a message")
	}
}
func TestCancelledDoesNotCreateProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	p := &Publisher{factory: func() (sender, error) { called = true; return &fakeSender{}, nil }}
	if err := p.Publish(ctx, prices.Record{}); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}
func TestNewRejectsEmptyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		brokers []string
		topic   string
	}{{nil, "prices"}, {[]string{"localhost:9092"}, ""}} {
		if _, err := New(tc.brokers, tc.topic); err == nil {
			t.Fatal("accepted empty configuration")
		}
	}
}
