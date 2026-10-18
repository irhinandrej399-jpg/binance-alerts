// Пакет binance получает исходные сделки через WebSocket.
package binance

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"fmt"
	"github.com/gorilla/websocket"
	"strings"
	"time"
)

var ErrSubscription = errors.New("binance subscription rejected")

type Client struct {
	URL        string
	Symbols    []string
	Dialer     *websocket.Dialer
	RetryDelay time.Duration
	OnState    func(bool)
	OnError    func(error)
	Handle     func(context.Context, []byte) error
}

func (c *Client) state(ready bool) {
	if c.OnState != nil {
		c.OnState(ready)
	}
}
func (c *Client) report(err error) {
	if c.OnError != nil {
		c.OnError(err)
	}
}
func (c *Client) delay() time.Duration {
	if c.RetryDelay > 0 {
		return c.RetryDelay
	}
	return time.Second
}
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Session работает до разрыва соединения; Run отвечает за повторное подключение.
func (c *Client) Session(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(c.Symbols) == 0 || c.Handle == nil {
		return fmt.Errorf("symbols and handler are required")
	}
	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, response, err := dialer.DialContext(ctx, c.URL, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return err
	}
	defer conn.Close()
	defer c.state(false)
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopped:
		}
	}()
	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) })
	params := make([]string, len(c.Symbols))
	for i, symbol := range c.Symbols {
		params[i] = strings.ToLower(symbol) + "@aggTrade"
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err = conn.WriteJSON(struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     string   `json:"id"`
	}{"SUBSCRIBE", params, "market-data"}); err != nil {
		return err
	}
	subscribed := false
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		var envelope map[string]json.RawMessage
		if json.Unmarshal(raw, &envelope) == nil {
			if code, exists := envelope["code"]; exists {
				return fmt.Errorf("%w: code=%s", ErrSubscription, code)
			}
			if result, exists := envelope["result"]; exists {
				if string(envelope["id"]) == `"market-data"` && string(result) == "null" {
					subscribed = true
					c.state(true)
				}
				continue
			}
		}
		for {
			err = c.Handle(ctx, raw)
			if err == nil {
				c.state(subscribed)
				break
			}
			if errors.Is(err, prices.ErrInvalidMessage) {
				c.report(err)
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.state(false)
			c.report(fmt.Errorf("save current trade: %w", err))
			if err = wait(ctx, c.delay()); err != nil {
				return err
			}
		}
	}
}
func (c *Client) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.Session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.report(err)
		if err = wait(ctx, c.delay()); err != nil {
			return err
		}
	}
}
