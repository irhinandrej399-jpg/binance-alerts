package tracking_test

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/tracking"
	"testing"
)

const command = `{"schema_version":1,"event_type":"InstrumentTrackingChanged","symbol":"BTCUSDT","enabled":true,"version":9007199254740993}`

type repository struct {
	changes []tracking.Change
	ctx     context.Context
	err     error
}

func (r *repository) ApplyTracking(ctx context.Context, c tracking.Change) error {
	r.ctx = ctx
	r.changes = append(r.changes, c)
	return r.err
}
func TestHandlePreservesExactVersionAndFalse(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal([]byte(command), &fields)
		v, _ := json.Marshal(enabled)
		fields["enabled"] = v
		raw, _ := json.Marshal(fields)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := &repository{}
		if err := tracking.Handle(ctx, []byte("BTCUSDT"), raw, r); err != nil {
			t.Fatal(err)
		}
		if len(r.changes) != 1 || r.changes[0] != (tracking.Change{"BTCUSDT", enabled, 9007199254740993}) || r.ctx != ctx {
			t.Fatalf("changes=%+v", r.changes)
		}
	}
}
func TestHandleRejectsInvalidCommands(t *testing.T) {
	cases := []struct{ name, field, value string }{
		{"schema", "schema_version", "2"}, {"type", "event_type", `"Other"`}, {"symbol", "symbol", `"btc/usdt"`},
		{"version_zero", "version", "0"}, {"version_negative", "version", "-1"}, {"version_overflow", "version", "9223372036854775808"},
		{"version_string", "version", `"1"`}, {"version_fraction", "version", "1.5"}, {"enabled_string", "enabled", `"true"`},
	}
	for _, field := range []string{"schema_version", "event_type", "symbol", "enabled", "version"} {
		cases = append(cases, struct{ name, field, value string }{field + "_missing", field, ""}, struct{ name, field, value string }{field + "_null", field, "null"})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal([]byte(command), &fields)
			if tc.value == "" {
				delete(fields, tc.field)
			} else {
				fields[tc.field] = json.RawMessage(tc.value)
			}
			raw, _ := json.Marshal(fields)
			r := &repository{}
			err := tracking.Handle(context.Background(), []byte("BTCUSDT"), raw, r)
			if !errors.Is(err, tracking.ErrInvalidCommand) || len(r.changes) != 0 {
				t.Fatalf("err=%v changes=%v", err, r.changes)
			}
		})
	}
	for _, raw := range []string{"", `null`, `[]`, command + command} {
		r := &repository{}
		if err := tracking.Handle(context.Background(), []byte("BTCUSDT"), []byte(raw), r); !errors.Is(err, tracking.ErrInvalidCommand) || len(r.changes) != 0 {
			t.Fatalf("raw=%q err=%v", raw, err)
		}
	}
	for _, key := range []string{"", "ETHUSDT"} {
		r := &repository{}
		if err := tracking.Handle(context.Background(), []byte(key), []byte(command), r); !errors.Is(err, tracking.ErrInvalidCommand) || len(r.changes) != 0 {
			t.Fatalf("key=%q err=%v", key, err)
		}
	}
}
func TestHandlePreservesRepositoryErrors(t *testing.T) {
	for _, want := range []error{tracking.ErrVersionConflict, context.DeadlineExceeded, errors.New("database down")} {
		r := &repository{err: want}
		if err := tracking.Handle(context.Background(), []byte("BTCUSDT"), []byte(command), r); !errors.Is(err, want) {
			t.Fatalf("err=%v want=%v", err, want)
		}
	}
}
func TestCancelledHandleDoesNotWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &repository{}
	if err := tracking.Handle(ctx, []byte("BTCUSDT"), []byte(command), r); !errors.Is(err, context.Canceled) || len(r.changes) != 0 {
		t.Fatalf("err=%v", err)
	}
}
