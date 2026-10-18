// Пакет tracking обрабатывает желаемое состояние отслеживания торговых пар.
package tracking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

var ErrInvalidCommand = errors.New("invalid tracking command")
var ErrVersionConflict = errors.New("tracking version conflict")
var symbolPattern = regexp.MustCompile(`^[A-Z0-9]+$`)

type Change struct {
	Symbol  string
	Enabled bool
	Version int64
}
type Repository interface {
	ApplyTracking(context.Context, Change) error
}

func Handle(ctx context.Context, key, raw []byte, repo Repository) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var input struct {
		Schema  *int   `json:"schema_version"`
		Type    string `json:"event_type"`
		Symbol  string `json:"symbol"`
		Enabled *bool  `json:"enabled"`
		Version *int64 `json:"version"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCommand, err)
	}
	if input.Schema == nil || *input.Schema != 1 || input.Type != "InstrumentTrackingChanged" || !symbolPattern.MatchString(input.Symbol) || string(key) != input.Symbol || input.Enabled == nil || input.Version == nil || *input.Version <= 0 {
		return ErrInvalidCommand
	}
	return repo.ApplyTracking(ctx, Change{input.Symbol, *input.Enabled, *input.Version})
}
