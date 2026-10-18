package postgres

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"github.com/jackc/pgx/v5"
)

func (s *Store) ApplyTracking(ctx context.Context, c tracking.Change) error {
	var version int64
	var enabled bool
	err := s.Pool.QueryRow(ctx, `INSERT INTO instrument_tracking(symbol,enabled,version) VALUES($1,$2,$3)
 ON CONFLICT(symbol) DO UPDATE SET enabled=EXCLUDED.enabled,version=EXCLUDED.version,updated_at=now()
 WHERE instrument_tracking.version < EXCLUDED.version RETURNING version,enabled`, c.Symbol, c.Enabled, c.Version).Scan(&version, &enabled)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// Конкурирующая большая версия может успеть примениться до чтения; её не откатываем.
	if err = s.Pool.QueryRow(ctx, "SELECT version,enabled FROM instrument_tracking WHERE symbol=$1", c.Symbol).Scan(&version, &enabled); err != nil {
		return err
	}
	if version == c.Version && enabled != c.Enabled {
		return tracking.ErrVersionConflict
	}
	return nil
}
func (s *Store) TrackingSymbols(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, "SELECT symbol FROM instrument_tracking WHERE enabled ORDER BY symbol")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var symbols []string
	for rows.Next() {
		var symbol string
		if err = rows.Scan(&symbol); err != nil {
			return nil, err
		}
		symbols = append(symbols, symbol)
	}
	return symbols, rows.Err()
}
func (s *Store) RejectTracking(ctx context.Context, topic string, partition int32, offset int64, reason string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO tracking_rejections(topic,partition_id,offset_id,reason) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, topic, partition, offset, reason)
	return err
}
