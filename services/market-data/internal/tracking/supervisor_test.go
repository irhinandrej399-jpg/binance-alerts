package tracking_test

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	select {
	case v := <-ch:
		return v
	case <-deadline.Done():
		t.Fatal(deadline.Err())
		var zero T
		return zero
	}
}
func TestSupervisorReplacesOnlyChangedSetsAndWaitsForOldReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshots := make(chan tracking.Snapshot, 8)
	started := make(chan []string, 4)
	oldCancelled := make(chan struct{})
	allowOldStop := make(chan struct{})
	defer close(allowOldStop)
	done := make(chan error, 1)
	runner := func(ctx context.Context, symbols []string, ready func(bool)) error {
		started <- symbols
		ready(true)
		<-ctx.Done()
		if len(symbols) == 2 {
			close(oldCancelled)
			<-allowOldStop
		}
		return ctx.Err()
	}
	go func() { done <- tracking.Supervise(ctx, snapshots, runner, nil, nil) }()
	snapshots <- tracking.Snapshot{Symbols: []string{"ETHUSDT", "BTCUSDT", "BTCUSDT"}}
	if got := receive(t, ctx, started); !reflect.DeepEqual(got, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("started=%v", got)
	}
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT", "ETHUSDT"}}
	snapshots <- tracking.Snapshot{Symbols: []string{"ETHUSDT"}}
	receive(t, ctx, oldCancelled)
	select {
	case got := <-started:
		t.Fatalf("new reader started before old stopped: %v", got)
	default:
	}
	allowOldStop <- struct{}{}
	if got := receive(t, ctx, started); !reflect.DeepEqual(got, []string{"ETHUSDT"}) {
		t.Fatalf("started=%v", got)
	}
	cancel()
	if err := receive(t, context.Background(), done); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
func TestSupervisorEmptySnapshotIsIdleAndStopsExistingReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshots := make(chan tracking.Snapshot, 8)
	states := make(chan bool, 20)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- tracking.Supervise(ctx, snapshots, func(ctx context.Context, _ []string, ready func(bool)) error {
			started <- struct{}{}
			ready(true)
			<-ctx.Done()
			stopped <- struct{}{}
			return ctx.Err()
		}, func(v bool) { states <- v }, nil)
	}()
	snapshots <- tracking.Snapshot{}
	if ready := receive(t, ctx, states); !ready {
		t.Fatal("empty snapshot should be ready idle")
	}
	select {
	case <-started:
		t.Fatal("empty snapshot opened a connection")
	default:
	}
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT"}}
	receive(t, ctx, started)
	snapshots <- tracking.Snapshot{}
	receive(t, ctx, stopped)
	cancel()
	if err := receive(t, context.Background(), done); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
func TestSupervisorKeepsReaderOnSnapshotErrorAndRecoversReadiness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshots := make(chan tracking.Snapshot, 8)
	states := make(chan bool, 20)
	reported := make(chan error, 4)
	started := make(chan context.Context, 2)
	done := make(chan error, 1)
	go func() {
		done <- tracking.Supervise(ctx, snapshots, func(ctx context.Context, _ []string, ready func(bool)) error {
			started <- ctx
			ready(true)
			<-ctx.Done()
			return ctx.Err()
		}, func(v bool) { states <- v }, func(err error) { reported <- err })
	}()
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT"}}
	reader := receive(t, ctx, started)
	for !receive(t, ctx, states) {
	}
	sentinel := errors.New("db unavailable")
	snapshots <- tracking.Snapshot{Err: sentinel}
	if err := receive(t, ctx, reported); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if ready := receive(t, ctx, states); ready {
		t.Fatal("snapshot failure did not reset readiness")
	}
	select {
	case <-reader.Done():
		t.Fatal("snapshot failure stopped valid reader")
	default:
	}
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT"}}
	if ready := receive(t, ctx, states); !ready {
		t.Fatal("unchanged healthy snapshot did not restore readiness")
	}
	snapshots <- tracking.Snapshot{Symbols: []string{"bad/pair"}}
	receive(t, ctx, reported)
	select {
	case <-reader.Done():
		t.Fatal("invalid snapshot stopped valid reader")
	default:
	}
	cancel()
	if err := receive(t, context.Background(), done); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestSupervisorReturnsUnexpectedReaderErrorWithoutHanging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snapshots := make(chan tracking.Snapshot, 1)
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT"}}
	sentinel := errors.New("unexpected reader failure")
	done := make(chan error, 1)
	go func() {
		done <- tracking.Supervise(ctx, snapshots, func(context.Context, []string, func(bool)) error { return sentinel }, nil, nil)
	}()
	if err := receive(t, ctx, done); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v want=%v", err, sentinel)
	}
}

func TestSupervisorRejectsOverCapacitySnapshotWithoutStoppingReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshots := make(chan tracking.Snapshot, 2)
	started := make(chan context.Context, 1)
	reported := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- tracking.Supervise(ctx, snapshots, func(ctx context.Context, _ []string, ready func(bool)) error {
			started <- ctx
			ready(true)
			<-ctx.Done()
			return ctx.Err()
		}, nil, func(err error) { reported <- err })
	}()
	snapshots <- tracking.Snapshot{Symbols: []string{"BTCUSDT"}}
	reader := receive(t, ctx, started)
	symbols := make([]string, 1025)
	for i := range symbols {
		symbols[i] = "PAIR" + strconv.Itoa(i)
	}
	snapshots <- tracking.Snapshot{Symbols: symbols}
	receive(t, ctx, reported)
	select {
	case <-reader.Done():
		t.Fatal("over-capacity snapshot stopped existing reader")
	default:
	}
	cancel()
	if err := receive(t, context.Background(), done); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
