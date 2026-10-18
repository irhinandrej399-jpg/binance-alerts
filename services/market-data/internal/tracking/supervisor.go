package tracking

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

type Snapshot struct {
	Symbols []string
	Err     error
}
type Runner func(context.Context, []string, func(bool)) error

type readiness struct {
	mu                    sync.Mutex
	healthy, idle, source bool
	notify                func(bool)
}

func (s *readiness) update(change func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change()
	if s.notify != nil {
		s.notify(s.healthy && (s.idle || s.source))
	}
}
func canonical(symbols []string) ([]string, error) {
	result := slices.Clone(symbols)
	slices.Sort(result)
	result = slices.Compact(result)
	if len(result) > 1024 {
		return nil, fmt.Errorf("tracking snapshot exceeds one connection capacity")
	}
	for _, symbol := range result {
		if !symbolPattern.MatchString(symbol) {
			return nil, fmt.Errorf("invalid symbol in tracking snapshot")
		}
	}
	return result, nil
}

// Supervise заменяет reader только при изменении множества пар и дожидается остановки старого.
// Runner должен завершаться по отмене контекста, а callback готовности — выполняться быстро.
func Supervise(ctx context.Context, updates <-chan Snapshot, run Runner, onState func(bool), report func(error)) error {
	status := &readiness{notify: onState}
	var cancel context.CancelFunc
	var done chan error
	var active []string
	haveSnapshot := false
	stop := func() {
		if cancel != nil {
			cancel()
			<-done
			cancel = nil
			done = nil
		}
	}
	defer stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			cancel()
			cancel = nil
			done = nil
			status.update(func() { status.source = false })
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				return fmt.Errorf("instrument reader stopped unexpectedly")
			}
			return fmt.Errorf("instrument reader stopped unexpectedly: %w", err)
		case snapshot, ok := <-updates:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("tracking snapshot stream closed")
			}
			symbols, err := canonical(snapshot.Symbols)
			if snapshot.Err != nil {
				err = snapshot.Err
			}
			if err != nil {
				status.update(func() { status.healthy = false })
				if report != nil {
					report(err)
				}
				continue
			}
			if haveSnapshot && slices.Equal(active, symbols) {
				status.update(func() { status.healthy = true })
				continue
			}
			if cancel != nil {
				status.update(func() { status.healthy = false })
				stop()
			}
			active = symbols
			haveSnapshot = true
			status.update(func() { status.healthy = true; status.idle = len(symbols) == 0; status.source = false })
			if len(symbols) == 0 {
				continue
			}
			cancel, done = startReader(ctx, slices.Clone(symbols), run, func(ready bool) { status.update(func() { status.source = ready }) })
		}
	}
}

type Loader interface {
	TrackingSymbols(context.Context) ([]string, error)
}

// Poll отправляет полный снимок состояния; канал ограничивает накопление обновлений в памяти.
func Poll(ctx context.Context, loader Loader, interval time.Duration, out chan<- Snapshot) error {
	if interval <= 0 {
		return fmt.Errorf("tracking refresh interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		symbols, err := loader.TrackingSymbols(readCtx)
		cancel()
		select {
		case out <- Snapshot{symbols, err}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// startReader связывает освобождение контекста с окончанием конкретного reader.
func startReader(parent context.Context, symbols []string, run Runner, ready func(bool)) (context.CancelFunc, chan error) {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() { defer cancel(); done <- run(ctx, symbols, ready) }()
	return cancel, done
}
