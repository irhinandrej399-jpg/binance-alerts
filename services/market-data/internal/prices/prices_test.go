package prices_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"example.com/binance-alerts/services/market-data/internal/prices"
)

const validTrade = `{"e":"aggTrade","E":1770000000125,"s":"BTCUSDT","a":12345,"p":"60000.01000000","T":1770000000123}`

type recordingOutbox struct {
	messages  []prices.Record
	appendErr error
	onAppend  func(context.Context)
}

func (p *recordingOutbox) Append(ctx context.Context, m prices.Record) error {
	// Копируем байты, чтобы повторное использование буфера не изменило предыдущие сообщения.
	p.messages = append(p.messages, prices.Record{
		Key: append([]byte(nil), m.Key...), Value: append([]byte(nil), m.Value...),
	})
	if p.onAppend != nil {
		p.onAppend(ctx)
	}
	return p.appendErr
}

func decodeEvent(t *testing.T, m prices.Record) prices.Event {
	t.Helper()
	var e prices.Event
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("invalid event JSON: %v; value=%s", err, m.Value)
	}
	return e
}

func requireSaved(t *testing.T, raw string) (prices.Record, prices.Event) {
	t.Helper()
	p := &recordingOutbox{}
	if err := prices.Handle(context.Background(), []byte(raw), p); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(p.messages) != 1 {
		t.Fatalf("publications = %d, want 1", len(p.messages))
	}
	return p.messages[0], decodeEvent(t, p.messages[0])
}

func replaceField(t *testing.T, field, replacement string) string {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(validTrade), &fields); err != nil {
		t.Fatal(err)
	}
	if replacement == "" {
		delete(fields, field)
	} else {
		fields[field] = json.RawMessage(replacement)
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// S01 / MD-01: сравниваем содержимое JSON без учёта порядка полей.
func TestS01_NormalizesTradeAndSavesWithSymbolKey(t *testing.T) {
	m, _ := requireSaved(t, validTrade)
	if string(m.Key) != "BTCUSDT" {
		t.Fatalf("key = %q, want BTCUSDT", m.Key)
	}
	const wantJSON = `{"schema_version":1,"event_type":"PriceObserved","event_id":"binance:spot:aggTrade:BTCUSDT:12345","source":"binance","market":"spot","symbol":"BTCUSDT","price":"60000.01000000","occurred_at_ms":1770000000123,"source_event_id":"12345"}`
	var got, want map[string]any
	// Декодируем числа без float64, чтобы сохранить точность при сравнении контрактов.
	for _, item := range []struct {
		raw    []byte
		target *map[string]any
	}{
		{m.Value, &got}, {[]byte(wantJSON), &want},
	} {
		d := json.NewDecoder(strings.NewReader(string(item.raw)))
		d.UseNumber()
		if err := d.Decode(item.target); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event = %s, want %s", m.Value, wantJSON)
	}
}

func TestS02_PreservesDecimalPriceExactly(t *testing.T) {
	for _, price := range []string{"0.00000001", "60000.01000000", "9007199254740993.123456789012345678", "1", "0001.00"} {
		t.Run(price, func(t *testing.T) {
			_, event := requireSaved(t, replaceField(t, "p", fmt.Sprintf("%q", price)))
			if event.Price != price {
				t.Fatalf("price = %q, want %q", event.Price, price)
			}
		})
	}
}

func TestS03_RejectsMalformedOrNonTradeMessages(t *testing.T) {
	cases := map[string]string{
		"empty": "", "broken_json": `{"e":`, "null": `null`, "array": `[]`,
		"two_values":        validTrade + validTrade,
		"trailing_garbage":  validTrade + "garbage",
		"subscription_ack":  `{"result":null,"id":"subscribe-1"}`,
		"source_error":      `{"code":2,"msg":"Invalid request","id":"subscribe-1"}`,
		"other_event":       strings.Replace(validTrade, "aggTrade", "trade", 1),
		"combined_envelope": `{"stream":"btcusdt@aggTrade","data":` + validTrade + `}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) { requireRejected(t, raw) })
	}
}

func requireRejected(t *testing.T, raw string) {
	t.Helper()
	p := &recordingOutbox{}
	err := prices.Handle(context.Background(), []byte(raw), p)
	if !errors.Is(err, prices.ErrInvalidMessage) {
		t.Errorf("error = %v, want ErrInvalidMessage", err)
	}
	if len(p.messages) != 0 {
		t.Errorf("invalid input appended %d messages", len(p.messages))
	}
}

func TestS04_RejectsInvalidRequiredFields(t *testing.T) {
	for _, field := range []string{"e", "s", "a", "p", "T"} {
		for _, value := range []struct{ name, raw string }{{"missing", ""}, {"null", "null"}} {
			t.Run(field+"/"+value.name, func(t *testing.T) { requireRejected(t, replaceField(t, field, value.raw)) })
		}
	}
	cases := []struct{ name, field, value string }{
		{"event_number", "e", "1"}, {"symbol_number", "s", "123"},
		{"symbol_empty", "s", `""`}, {"symbol_lowercase", "s", `"btcusdt"`},
		{"symbol_separator", "s", `"BTC/USDT"`}, {"symbol_space", "s", `"BTC USDT"`},
		{"id_string", "a", `"12345"`}, {"id_negative", "a", "-1"},
		{"id_fraction", "a", "1.5"}, {"id_overflow", "a", "9223372036854775808"},
		{"id_boolean", "a", "true"},
		{"time_string", "T", `"1770000000123"`}, {"time_zero", "T", "0"},
		{"time_negative", "T", "-1"}, {"time_fraction", "T", "1.5"},
		{"time_overflow", "T", "9223372036854775808"}, {"time_boolean", "T", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { requireRejected(t, replaceField(t, tc.field, tc.value)) })
	}
}

func TestS05_RejectsInvalidPrices(t *testing.T) {
	for _, price := range []string{"", "0", "0.00000000", "-1", "+1", " 1", "1 ", "1e3", "NaN", "Infinity", ".5", "1.", "1,5"} {
		t.Run(fmt.Sprintf("string_%q", price), func(t *testing.T) {
			requireRejected(t, replaceField(t, "p", fmt.Sprintf("%q", price)))
		})
	}
	for _, value := range []string{"60000.01", "true", "[]", "{}"} {
		t.Run("non_string_"+value, func(t *testing.T) { requireRejected(t, replaceField(t, "p", value)) })
	}
}

func TestS06_PreservesSourceIDWithoutFloatConversion(t *testing.T) {
	for _, id := range []string{"0", "9007199254740993", "9223372036854775807"} {
		t.Run(id, func(t *testing.T) {
			_, e := requireSaved(t, replaceField(t, "a", id))
			if e.SourceEventID != id {
				t.Errorf("source ID = %q, want %q", e.SourceEventID, id)
			}
			if want := "binance:spot:aggTrade:BTCUSDT:" + id; e.EventID != want {
				t.Errorf("event ID = %q, want %q", e.EventID, want)
			}
		})
	}
}

func TestS07_SameSourceIDOnDifferentSymbolsHasDifferentIdentity(t *testing.T) {
	btcMessage, btc := requireSaved(t, validTrade)
	ethMessage, eth := requireSaved(t, replaceField(t, "s", `"ETHUSDT"`))
	if btc.SourceEventID != eth.SourceEventID {
		t.Fatal("fixture must use the same source ID")
	}
	if eth.EventID != "binance:spot:aggTrade:ETHUSDT:12345" {
		t.Errorf("ETH event ID = %q", eth.EventID)
	}
	if btc.EventID == eth.EventID {
		t.Error("different symbols share an event ID")
	}
	if string(btcMessage.Key) != "BTCUSDT" || string(ethMessage.Key) != "ETHUSDT" {
		t.Error("incorrect symbol keys")
	}
}

func TestS08_RepeatedInputIsSavedWithStableIdentity(t *testing.T) {
	p := &recordingOutbox{}
	for range 2 {
		if err := prices.Handle(context.Background(), []byte(validTrade), p); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if len(p.messages) != 2 {
		t.Fatalf("publications = %d, want 2", len(p.messages))
	}
	first, second := decodeEvent(t, p.messages[0]), decodeEvent(t, p.messages[1])
	if first.EventID == "" || first != second {
		t.Fatalf("duplicate changed identity or payload: %+v / %+v", first, second)
	}
}

func TestS09_LateTradeIsForwardedWithoutRewritingTime(t *testing.T) {
	p := &recordingOutbox{}
	late := strings.ReplaceAll(validTrade, "1770000000123", "1769999999123")
	late = strings.Replace(late, `"a":12345`, `"a":12344`, 1)
	for _, raw := range []string{validTrade, late} {
		if err := prices.Handle(context.Background(), []byte(raw), p); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if len(p.messages) != 2 {
		t.Fatalf("publications = %d, want 2", len(p.messages))
	}
	if e := decodeEvent(t, p.messages[1]); e.OccurredAtMS != 1769999999123 || e.SourceEventID != "12344" {
		t.Fatalf("late event changed: %+v", e)
	}
}

func TestS10_DistinctTradesWithUnchangedPriceAreBothSaved(t *testing.T) {
	p := &recordingOutbox{}
	for _, raw := range []string{validTrade, strings.Replace(validTrade, `"a":12345`, `"a":12346`, 1)} {
		if err := prices.Handle(context.Background(), []byte(raw), p); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	}
	if len(p.messages) != 2 {
		t.Fatalf("publications = %d, want 2", len(p.messages))
	}
	first, second := decodeEvent(t, p.messages[0]), decodeEvent(t, p.messages[1])
	if first.Price != second.Price || first.EventID == second.EventID {
		t.Fatalf("incorrect identities or prices: %+v / %+v", first, second)
	}
}

func TestS11_AppendErrorIsReturnedWithoutInternalRetry(t *testing.T) {
	unavailable := errors.New("broker acknowledgement unavailable")
	p := &recordingOutbox{appendErr: unavailable}
	err := prices.Handle(context.Background(), []byte(validTrade), p)
	if !errors.Is(err, unavailable) {
		t.Errorf("error = %v, want appender error", err)
	}
	if len(p.messages) != 1 {
		t.Fatalf("attempts = %d, want 1", len(p.messages))
	}
	failed := decodeEvent(t, p.messages[0])
	p.appendErr = nil
	if err := prices.Handle(context.Background(), []byte(validTrade), p); err != nil {
		t.Fatalf("explicit retry: %v", err)
	}
	if len(p.messages) != 2 {
		t.Fatalf("attempts after explicit retry = %d, want 2", len(p.messages))
	}
	retried := decodeEvent(t, p.messages[1])
	if failed.EventID == "" || failed != retried {
		t.Fatalf("explicit retry changed payload: %+v / %+v", failed, retried)
	}
}

func TestS12_PreCancelledContextDoesNotPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &recordingOutbox{}
	err := prices.Handle(ctx, []byte(validTrade), p)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if len(p.messages) != 0 {
		t.Errorf("cancelled call appended %d messages", len(p.messages))
	}
}

func TestS13_PassesContextToOutboxAndPreservesCancellationErrors(t *testing.T) {
	for _, appendErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(appendErr.Error(), func(t *testing.T) {
			deadline := time.Now().Add(time.Minute)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			p := &recordingOutbox{appendErr: appendErr, onAppend: func(got context.Context) {
				if got != ctx {
					t.Error("appender did not receive the original context")
				}
				if d, ok := got.Deadline(); !ok || !d.Equal(deadline) {
					t.Error("deadline was lost")
				}
			}}
			err := prices.Handle(ctx, []byte(validTrade), p)
			if !errors.Is(err, appendErr) {
				t.Errorf("error = %v, want %v", err, appendErr)
			}
			if len(p.messages) != 1 {
				t.Errorf("attempts = %d, want 1", len(p.messages))
			}
		})
	}
}

func TestMD03_AllowsUnusedSourceFieldsAndJSONWhitespace(t *testing.T) {
	raw := strings.TrimSuffix(validTrade, "}") + `,"q":"0.001","f":1,"l":2,"m":true,"future_field":{"nested":1}}`
	_, e := requireSaved(t, " \n"+raw+"\n ")
	if e.Symbol != "BTCUSDT" || e.Price != "60000.01000000" {
		t.Fatalf("unused fields affected output: %+v", e)
	}
}
