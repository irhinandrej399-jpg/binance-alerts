// Пакет config читает настройки процесса из окружения.
package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL, HTTPAddr, BinanceURL, Topic          string
	Symbols, Brokers                                  []string
	BatchSize                                         int
	PollInterval, RetryDelay, Retention, BatchTimeout time.Duration
}

func Load(getenv func(string) string) (Config, error) {
	value := func(key, fallback string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return fallback
	}
	c := Config{DatabaseURL: value("DATABASE_URL", ""), HTTPAddr: value("HTTP_ADDR", "127.0.0.1:8080"), BinanceURL: value("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws"), Topic: value("KAFKA_TOPIC", "market.prices.v1")}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if _, _, err := net.SplitHostPort(c.HTTPAddr); err != nil {
		return c, fmt.Errorf("invalid HTTP_ADDR")
	}
	u, err := url.Parse(c.BinanceURL)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil {
		return c, fmt.Errorf("invalid BINANCE_WS_URL")
	}
	seen := map[string]bool{}
	symbolPattern := regexp.MustCompile(`^[A-Z0-9]+$`)
	for _, s := range strings.Split(getenv("SYMBOLS"), ",") {
		s = strings.ToUpper(strings.TrimSpace(s))
		if !symbolPattern.MatchString(s) {
			return c, fmt.Errorf("SYMBOLS must contain non-empty trading pairs")
		}
		if !seen[s] {
			c.Symbols = append(c.Symbols, s)
			seen[s] = true
		}
	}
	if len(c.Symbols) > 1024 {
		return c, fmt.Errorf("too many symbols for one connection")
	}
	for _, broker := range strings.Split(value("KAFKA_BROKERS", "localhost:19092"), ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return c, fmt.Errorf("empty Kafka broker")
		}
		c.Brokers = append(c.Brokers, broker)
	}
	c.BatchSize, err = strconv.Atoi(value("BATCH_SIZE", "10"))
	if err != nil || c.BatchSize <= 0 || c.BatchSize > 1000 {
		return c, fmt.Errorf("BATCH_SIZE must be between 1 and 1000")
	}
	for _, d := range []struct {
		key, fallback string
		target        *time.Duration
	}{
		{"POLL_INTERVAL", "100ms", &c.PollInterval}, {"RETRY_DELAY", "1s", &c.RetryDelay}, {"OUTBOX_RETENTION", "24h", &c.Retention}, {"BATCH_TIMEOUT", "60s", &c.BatchTimeout},
	} {
		parsed, err := time.ParseDuration(value(d.key, d.fallback))
		if err != nil || parsed <= 0 {
			return c, fmt.Errorf("%s must be a positive duration", d.key)
		}
		*d.target = parsed
	}
	return c, nil
}
