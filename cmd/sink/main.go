// Command sink is a throwaway webhook receiver for testing delivery.go.
// It logs every request and fails a fraction of them on purpose.
//
//	go run ./cmd/sink                 # listens on :9000, ~30% failures
//	PORT=9001 FAIL_RATE=0.5 go run ./cmd/sink
//
// Force an outcome per request with a query param: ?status=500 or ?sleep=5s.
package main

import (
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := envOr("PORT", "9000")
	failRate, _ := strconv.ParseFloat(envOr("FAIL_RATE", "0.3"), 64)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		if d, err := time.ParseDuration(r.URL.Query().Get("sleep")); err == nil {
			time.Sleep(d)
		}

		status := http.StatusOK
		if s := r.URL.Query().Get("status"); s != "" {
			status, _ = strconv.Atoi(s)
		} else if rand.Float64() < failRate {
			status = http.StatusInternalServerError
		}

		log.Printf("%s %s -> %d | event=%q sig=%q body=%s",
			r.Method, r.URL.Path, status,
			r.Header.Get("X-Event-Type"), r.Header.Get("X-Signature"), body)

		w.WriteHeader(status)
		io.WriteString(w, http.StatusText(status))
	})

	log.Printf("sink listening on :%s (fail rate %.0f%%)", port, failRate*100)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
