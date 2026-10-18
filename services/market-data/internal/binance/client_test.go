package binance_test

import (
	"context"
	"encoding/json"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/binance"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func server(t *testing.T, f func(*websocket.Conn)) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		f(conn)
	}))
	t.Cleanup(s.Close)
	return "ws" + strings.TrimPrefix(s.URL, "http")
}
func TestSessionSubscribesSkipsAckAndRetriesCurrentSave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	subscribe := make(chan []string, 1)
	url := server(t, func(conn *websocket.Conn) {
		var msg struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		if msg.Method != "SUBSCRIBE" {
			t.Error("missing SUBSCRIBE")
		}
		subscribe <- msg.Params
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"result":null,"id":"market-data"}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"60000","T":1}`))
		_, _, _ = conn.ReadMessage()
	})
	calls := 0
	var states []bool
	c := &binance.Client{OnState: func(ready bool) { states = append(states, ready) }, URL: url, Symbols: []string{"BTCUSDT", "ETHUSDT"}, RetryDelay: time.Millisecond, Handle: func(_ context.Context, raw []byte) error {
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Error(err)
		}
		if msg["e"] != "aggTrade" {
			t.Error("control message reached handler")
		}
		calls++
		if calls == 1 {
			return errors.New("temporary db error")
		}
		cancel()
		return nil
	}}
	err := c.Session(ctx)
	if len(states) != 4 || !states[0] || states[1] || !states[2] || states[3] {
		t.Errorf("readiness transitions=%v, want [true false true false]", states)
	}
	if !errors.Is(err, context.Canceled) || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	select {
	case params := <-subscribe:
		if len(params) != 2 || params[0] != "btcusdt@aggTrade" || params[1] != "ethusdt@aggTrade" {
			t.Fatalf("params=%v", params)
		}
	default:
		t.Fatal("no subscription")
	}
}
func TestSessionIgnoresInvalidTradeAndContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := server(t, func(conn *websocket.Conn) {
		var request any
		if conn.ReadJSON(&request) != nil {
			return
		}
		for _, raw := range []string{`{"e":"aggTrade","p":"bad"}`, `{"e":"aggTrade","p":"1"}`} {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(raw))
		}
		_, _, _ = conn.ReadMessage()
	})
	calls := 0
	c := &binance.Client{URL: url, Symbols: []string{"BTCUSDT"}, Handle: func(context.Context, []byte) error {
		calls++
		if calls == 1 {
			return prices.ErrInvalidMessage
		}
		cancel()
		return nil
	}}
	_ = c.Session(ctx)
	if calls != 2 {
		t.Fatalf("handled=%d", calls)
	}
}
func TestRunReconnectsAndResubscribes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var connections atomic.Int32
	url := server(t, func(conn *websocket.Conn) {
		var request any
		if conn.ReadJSON(&request) != nil {
			return
		}
		n := connections.Add(1)
		if n == 1 {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"aggTrade"}`))
		_, _, _ = conn.ReadMessage()
	})
	c := &binance.Client{URL: url, Symbols: []string{"BTCUSDT"}, RetryDelay: time.Millisecond, Handle: func(context.Context, []byte) error { cancel(); return nil }}
	err := c.Run(ctx)
	if !errors.Is(err, context.Canceled) || connections.Load() < 2 {
		t.Fatalf("connections=%d err=%v", connections.Load(), err)
	}
}
func TestSessionSourceErrorDoesNotReachHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := server(t, func(conn *websocket.Conn) {
		var request any
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"code":2,"msg":"invalid subscription"}`))
		_, _, _ = conn.ReadMessage()
	})
	calls := 0
	c := &binance.Client{URL: url, Symbols: []string{"BTCUSDT"}, Handle: func(context.Context, []byte) error { calls++; return nil }}
	if err := c.Session(ctx); !errors.Is(err, binance.ErrSubscription) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestSessionRespondsToPingAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := server(t, func(conn *websocket.Conn) {
		var request any
		if conn.ReadJSON(&request) != nil {
			return
		}
		conn.SetPongHandler(func(payload string) error {
			if payload != "heartbeat" {
				t.Error("pong payload changed")
			}
			return conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"aggTrade"}`))
		})
		if err := conn.WriteControl(websocket.PingMessage, []byte("heartbeat"), time.Now().Add(time.Second)); err != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
	})
	handled := false
	c := &binance.Client{URL: url, Symbols: []string{"BTCUSDT"}, Handle: func(context.Context, []byte) error { handled = true; cancel(); return nil }}
	err := c.Session(ctx)
	if !errors.Is(err, context.Canceled) || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
