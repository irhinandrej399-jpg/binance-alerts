// Пакет postgres содержит долговечную очередь Market Data.
package postgres

import (
	"context"
	_ "embed"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

//go:embed migrations/001_outbox.sql
var schema string

type Store struct{ Pool *pgxpool.Pool }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(736400)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate outbox: %w", err)
	}
	return tx.Commit(ctx)
}
func (s *Store) Append(ctx context.Context, r prices.Record) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO outbox(event_id,message_key,payload) VALUES($1,$2,$3) ON CONFLICT(event_id) DO NOTHING`, r.EventID, string(r.Key), r.Value)
	return err
}
func (s *Store) Deliver(ctx context.Context, limit int, send func(context.Context, prices.Record) error) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if err = tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(736401)").Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil
	}
	rows, err := tx.Query(ctx, `SELECT id,event_id,message_key,payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type item struct {
		id     int64
		record prices.Record
	}
	var items []item
	for rows.Next() {
		var v item
		var key string
		if err = rows.Scan(&v.id, &v.record.EventID, &key, &v.record.Value); err != nil {
			rows.Close()
			return 0, err
		}
		v.record.Key = []byte(key)
		items = append(items, v)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, v := range items {
		if err = ctx.Err(); err != nil {
			return 0, err
		}
		if err = send(ctx, v.record); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE outbox SET published_at=now() WHERE id=$1", v.id); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(items), nil
}
func (s *Store) Purge(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	if retention <= 0 || limit <= 0 {
		return 0, fmt.Errorf("retention and limit must be positive")
	}
	result, err := s.Pool.Exec(ctx, `DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE published_at < now()-($1 * interval '1 second') ORDER BY published_at LIMIT $2)`, retention.Seconds(), limit)
	return result.RowsAffected(), err
}
