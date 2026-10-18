// Пакет prices проверяет сделки и сохраняет нормализованные цены в outbox.
package prices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var ErrInvalidMessage = errors.New("invalid market message")
var symbolPattern = regexp.MustCompile(`^[A-Z0-9]+$`)
var pricePattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// Event — контракт события PriceObserved версии 1.
type Event struct {
	SchemaVersion int    `json:"schema_version"`
	EventType     string `json:"event_type"`
	EventID       string `json:"event_id"`
	Source        string `json:"source"`
	Market        string `json:"market"`
	Symbol        string `json:"symbol"`
	Price         string `json:"price"`
	OccurredAtMS  int64  `json:"occurred_at_ms"`
	SourceEventID string `json:"source_event_id"`
}

// Record содержит стабильный идентификатор, ключ партиционирования и JSON события.
type Record struct {
	EventID string
	Key     []byte
	Value   []byte
}

// Outbox подтверждает долговечное сохранение и не создаёт дубликаты event_id.
type Outbox interface {
	Append(context.Context, Record) error
}

// Handle сохраняет одну проверенную сделку; публикация в Kafka выполняется отдельно.
func Handle(ctx context.Context, raw []byte, outbox Outbox) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var input struct {
		EventTime json.RawMessage `json:"E"`
		Type      string          `json:"e"`
		Symbol    string          `json:"s"`
		ID        *int64          `json:"a"`
		Price     string          `json:"p"`
		Time      *int64          `json:"T"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if input.Type != "aggTrade" || !symbolPattern.MatchString(input.Symbol) || input.ID == nil || *input.ID < 0 || input.Time == nil || *input.Time <= 0 || !pricePattern.MatchString(input.Price) || strings.Trim(input.Price, "0.") == "" {
		return ErrInvalidMessage
	}
	id := strconv.FormatInt(*input.ID, 10)
	event := Event{1, "PriceObserved", "binance:spot:aggTrade:" + input.Symbol + ":" + id, "binance", "spot", input.Symbol, input.Price, *input.Time, id}
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode price: %w", err)
	}
	if err := outbox.Append(ctx, Record{event.EventID, []byte(input.Symbol), value}); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	return nil
}
