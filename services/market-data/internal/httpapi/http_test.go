package httpapi_test

import (
	"context"
	"errors"
	"example.com/binance-alerts/services/market-data/internal/httpapi"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name, path, method string
		readyErr           error
		want               int
	}{
		{"live", "/healthz", "GET", errors.New("db down"), 200},
		{"ready", "/readyz", "GET", nil, 200},
		{"not_ready", "/readyz", "GET", errors.New("database credential must not leak"), 503},
		{"wrong_method", "/healthz", "POST", nil, 405},
		{"unknown", "/unknown", "GET", nil, 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := httpapi.New(func(context.Context) error { return tc.readyErr })
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d", w.Code, tc.want)
			}
			if tc.want == 503 && w.Body.String() != "not ready\n" {
				t.Fatalf("dependency details leaked: %q", w.Body.String())
			}
			if tc.want == http.StatusOK && w.Body.String() != "ok\n" {
				t.Fatalf("body=%q", w.Body.String())
			}
		})
	}
}
