package kafka

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"github.com/IBM/sarama"
	"sync"
	"testing"
	"time"
)

const trackingCommand = `{"schema_version":1,"event_type":"InstrumentTrackingChanged","symbol":"BTCUSDT","enabled":true,"version":1}`

type trackingRepo struct {
	mu                    sync.Mutex
	calls, rejections     int
	failApply, failReject int
	conflict              bool
	applied               chan struct{}
}

func (r *trackingRepo) ApplyTracking(context.Context, tracking.Change) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.applied != nil {
		select {
		case r.applied <- struct{}{}:
		default:
		}
	}
	if r.calls <= r.failApply {
		return errors.New("database unavailable")
	}
	if r.conflict {
		return tracking.ErrVersionConflict
	}
	return nil
}
func (r *trackingRepo) RejectTracking(_ context.Context, topic string, partition int32, offset int64, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejections++
	if r.rejections <= r.failReject {
		return errors.New("rejection write failed")
	}
	return nil
}

type trackingSession struct {
	sarama.ConsumerGroupSession
	ctx    context.Context
	marked chan *sarama.ConsumerMessage
}

func (s *trackingSession) Context() context.Context                        { return s.ctx }
func (s *trackingSession) MarkMessage(m *sarama.ConsumerMessage, _ string) { s.marked <- m }

type trackingClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func (c trackingClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
func TestTrackingConsumerRetriesBeforeMarking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	repo := &trackingRepo{failApply: 1}
	session := &trackingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 2)}
	messages := make(chan *sarama.ConsumerMessage, 2)
	messages <- &sarama.ConsumerMessage{Key: []byte("BTCUSDT"), Value: []byte(trackingCommand), Offset: 1}
	messages <- &sarama.ConsumerMessage{Key: []byte("BTCUSDT"), Value: []byte(trackingCommand), Offset: 2}
	close(messages)
	h := &TrackingHandler{Repository: repo, RetryDelay: time.Millisecond}
	if err := h.ConsumeClaim(session, trackingClaim{messages: messages}); err != nil {
		t.Fatal(err)
	}
	if repo.calls != 3 || len(session.marked) != 2 {
		t.Fatalf("calls=%d marked=%d", repo.calls, len(session.marked))
	}
	if first := <-session.marked; first.Offset != 1 {
		t.Fatal("messages were reordered")
	}
}
func TestTrackingConsumerPersistsRejectionBeforeMarking(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		repo := &trackingRepo{failReject: 1, conflict: conflict}
		session := &trackingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
		raw := []byte(`invalid`)
		if conflict {
			raw = []byte(trackingCommand)
		}
		messages := make(chan *sarama.ConsumerMessage, 1)
		messages <- &sarama.ConsumerMessage{Topic: "tracking", Partition: 1, Offset: 42, Key: []byte("BTCUSDT"), Value: raw}
		close(messages)
		h := &TrackingHandler{Repository: repo, RetryDelay: time.Millisecond}
		if err := h.ConsumeClaim(session, trackingClaim{messages: messages}); err != nil {
			t.Fatal(err)
		}
		cancel()
		if repo.rejections != 2 || len(session.marked) != 1 {
			t.Fatalf("rejections=%d marked=%d", repo.rejections, len(session.marked))
		}
	}
}
func TestTrackingConsumerCancellationDoesNotMarkFailedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &trackingRepo{failApply: 100}
	session := &trackingSession{ctx: ctx, marked: make(chan *sarama.ConsumerMessage, 1)}
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- &sarama.ConsumerMessage{Key: []byte("BTCUSDT"), Value: []byte(trackingCommand)}
	h := &TrackingHandler{Repository: repo, RetryDelay: time.Hour, OnError: func(error) { cancel() }}
	if err := h.ConsumeClaim(session, trackingClaim{messages: messages}); err != nil {
		t.Fatal(err)
	}
	if repo.calls != 1 || len(session.marked) != 0 {
		t.Fatalf("calls=%d marked=%d", repo.calls, len(session.marked))
	}
}
