package relay_test

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"example.com/binance-alerts/services/market-data/internal/relay"
	"testing"
)

type repo struct {
	records []prices.Record
	called  bool
}

func (r *repo) Deliver(ctx context.Context, limit int, send func(context.Context, prices.Record) error) (int, error) {
	r.called = true
	n := 0
	for _, record := range r.records {
		if n == limit {
			break
		}
		if err := send(ctx, record); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

type publisher struct {
	ids []string
	err error
}

func (p *publisher) Publish(_ context.Context, r prices.Record) error {
	p.ids = append(p.ids, r.EventID)
	return p.err
}
func TestBatchPublishesRecordsInOrder(t *testing.T) {
	r := &repo{records: []prices.Record{{EventID: "first"}, {EventID: "second"}}}
	p := &publisher{}
	n, err := relay.Batch(context.Background(), r, p, 2)
	if err != nil || n != 2 || len(p.ids) != 2 || p.ids[0] != "first" || p.ids[1] != "second" {
		t.Fatalf("n=%d err=%v ids=%v", n, err, p.ids)
	}
}
func TestBatchStopsOnPublisherError(t *testing.T) {
	sentinel := errors.New("broker down")
	r := &repo{records: []prices.Record{{EventID: "first"}, {EventID: "second"}}}
	p := &publisher{err: sentinel}
	n, err := relay.Batch(context.Background(), r, p, 2)
	if !errors.Is(err, sentinel) || n != 0 || len(p.ids) != 1 {
		t.Fatalf("n=%d err=%v ids=%v", n, err, p.ids)
	}
}
func TestBatchCancelledDoesNotAcquireOrSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &repo{}
	p := &publisher{}
	_, err := relay.Batch(ctx, r, p, 2)
	if !errors.Is(err, context.Canceled) || r.called || len(p.ids) != 0 {
		t.Fatalf("err=%v repo called=%v", err, r.called)
	}
}
func TestBatchRejectsNonPositiveLimit(t *testing.T) {
	r := &repo{}
	_, err := relay.Batch(context.Background(), r, &publisher{}, 0)
	if err == nil || r.called {
		t.Fatalf("err=%v called=%v", err, r.called)
	}
}
