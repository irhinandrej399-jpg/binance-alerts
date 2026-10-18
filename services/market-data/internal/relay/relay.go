// Пакет relay доставляет долговечные записи в брокер.
package relay

import (
	"context"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"fmt"
)

// Repository подтверждает доставку пакета только после успешных вызовов отправки.
type Repository interface {
	Deliver(context.Context, int, func(context.Context, prices.Record) error) (int, error)
}

// Publisher ожидает подтверждения брокера; ошибка может означать неизвестный результат.
type Publisher interface {
	Publish(context.Context, prices.Record) error
}

func Batch(ctx context.Context, repo Repository, publisher Publisher, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("batch limit must be positive")
	}
	return repo.Deliver(ctx, limit, func(ctx context.Context, r prices.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return publisher.Publish(ctx, r)
	})
}
