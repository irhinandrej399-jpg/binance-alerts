// Пакет httpapi предоставляет технические HTTP-проверки сервиса.
package httpapi

import (
	"context"
	"github.com/gorilla/mux"
	"net/http"
	"time"
)

func New(ready func(context.Context) error) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}).Methods(http.MethodGet)
	r.HandleFunc("/readyz", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}).Methods(http.MethodGet)
	return r
}
