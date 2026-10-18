package config_test

import (
	"example.com/binance-alerts/services/market-data/internal/config"
	"testing"
	"time"
)

func load(values map[string]string) (config.Config, error) {
	return config.Load(func(key string) string { return values[key] })
}
func TestDefaultsAndSymbolNormalization(t *testing.T) {
	c, err := load(map[string]string{"DATABASE_URL": "postgres://test", "SYMBOLS": " btcusdt, ETHUSDT,btcusdt "})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Symbols) != 2 || c.Symbols[0] != "BTCUSDT" || c.Symbols[1] != "ETHUSDT" || c.HTTPAddr != "127.0.0.1:8080" || c.Retention != 24*time.Hour || c.BatchSize <= 0 {
		t.Fatalf("config=%+v", c)
	}
}
func TestRejectsInvalidConfiguration(t *testing.T) {
	cases := []map[string]string{
		{}, {"DATABASE_URL": "postgres://test", "SYMBOLS": ""},
		{"DATABASE_URL": "postgres://test", "SYMBOLS": "BTC/USDT"},
		{"DATABASE_URL": "postgres://test", "SYMBOLS": "BTCUSDT", "BATCH_SIZE": "0"},
		{"DATABASE_URL": "postgres://test", "SYMBOLS": "BTCUSDT", "POLL_INTERVAL": "-1s"},
		{"DATABASE_URL": "postgres://test", "SYMBOLS": "BTCUSDT", "OUTBOX_RETENTION": "bad"},
		{"DATABASE_URL": "postgres://test", "SYMBOLS": "BTCUSDT", "BINANCE_WS_URL": "https://bad"},
	}
	for i, values := range cases {
		if _, err := load(values); err == nil {
			t.Errorf("case %d accepted invalid config", i)
		}
	}
}
