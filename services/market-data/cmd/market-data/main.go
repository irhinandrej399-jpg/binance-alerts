package main

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/binance"
	"example.com/binance-alerts/services/market-data/internal/config"
	"example.com/binance-alerts/services/market-data/internal/httpapi"
	kafkapub "example.com/binance-alerts/services/market-data/internal/kafka"
	"example.com/binance-alerts/services/market-data/internal/postgres"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"example.com/binance-alerts/services/market-data/internal/relay"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database configuration is invalid")
	}
	defer pool.Close()
	startCtx, startCancel := context.WithTimeout(ctx, 10*time.Second)
	defer startCancel()
	if err = pool.Ping(startCtx); err != nil {
		return fmt.Errorf("database unavailable: %w", err)
	}
	store := &postgres.Store{Pool: pool}
	if len(os.Args) > 1 {
		if len(os.Args) != 2 || os.Args[1] != "migrate" {
			return fmt.Errorf("usage: market-data [migrate]")
		}
		return store.Migrate(startCtx)
	}
	var schemaReady bool
	if err = pool.QueryRow(startCtx, "SELECT to_regclass('outbox') IS NOT NULL").Scan(&schemaReady); err != nil {
		return err
	}
	if !schemaReady {
		return fmt.Errorf("outbox schema is missing; run market-data migrate")
	}
	publisher, err := kafkapub.New(cfg.Brokers, cfg.Topic)
	if err != nil {
		return err
	}
	defer publisher.Close()
	var sourceReady atomic.Bool
	client := &binance.Client{URL: cfg.BinanceURL, Symbols: cfg.Symbols, RetryDelay: cfg.RetryDelay, OnState: sourceReady.Store, OnError: func(err error) { logger.Warn("source operation failed", "error", err) }, Handle: func(ctx context.Context, raw []byte) error { return prices.Handle(ctx, raw, store) }}
	handler := httpapi.New(func(ctx context.Context) error {
		if !sourceReady.Load() {
			return fmt.Errorf("source not ready")
		}
		return pool.Ping(ctx)
	})
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	group, workCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		<-workCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			_ = server.Close()
		}
		return err
	})
	group.Go(func() error {
		err := client.Run(workCtx)
		if workCtx.Err() != nil {
			return nil
		}
		return err
	})
	group.Go(func() error { return runRelay(workCtx, store, publisher, cfg, logger) })
	logger.Info("market data started", "http", cfg.HTTPAddr, "symbols", cfg.Symbols, "topic", cfg.Topic)
	return group.Wait()
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func runRelay(ctx context.Context, store *postgres.Store, publisher relay.Publisher, cfg config.Config, logger *slog.Logger) error {
	lastCleanup := time.Now()
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(ctx, cfg.BatchTimeout)
		n, err := relay.Batch(batchCtx, store, publisher, cfg.BatchSize)
		cancel()
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			logger.Warn("outbox delivery failed", "error", err)
		}
		if time.Since(lastCleanup) >= time.Minute {
			cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, cleanupErr := store.Purge(cleanupCtx, cfg.Retention, 1000)
			cancel()
			lastCleanup = time.Now()
			if cleanupErr != nil {
				logger.Warn("outbox cleanup failed", "error", cleanupErr)
			}
		}
		delay := cfg.PollInterval
		if err != nil {
			delay = cfg.RetryDelay
		} else if n > 0 {
			continue
		}
		if !pause(ctx, delay) {
			break
		}
	}
	return nil
}
