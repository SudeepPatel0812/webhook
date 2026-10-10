// Command sink is a fake customer endpoint for testing delivery. Each URL path
// is a scenario, so different endpoints can behave differently in one run:
//
//	/ok/...      healthy: 200 99%, 500 1%
//	/mixed/...   realistic mix (the default for any other path): mostly 200,
//	             some 204 (empty body), 500, 503, 404, dropped connections,
//	             slow responses, and the odd timeout
//	/slow/...    always 200, after 0.3-2.5s
//	/down/...    always 503 (trips the circuit breaker)
//	/gone/...    always 410 (permanent: the delivery goes straight to dead)
//
// Override the /mixed weights with SINK_MIX, e.g. SINK_MIX="200:90,500:10".
// Outcomes: any status code, "drop" (close the connection with no response),
// "slow" (200 after 0.3-2.5s), "timeout" (no answer for 12s, longer than the
// delivery worker's 10s limit). Force one request with ?status=500 or
// ?sleep=5s.
//
// It prints a summary every 5s instead of a line per request, so it can take
// tens of thousands of requests without flooding the console.
//
//	go run ./cmd/sink                 # listens on :9000
package main

import (
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type weighted struct {
	outcome string
	weight  int
}

var scenarios = map[string][]weighted{
	"ok":    {{"200", 99}, {"500", 1}},
	"mixed": {{"200", 775}, {"204", 60}, {"500", 80}, {"503", 30}, {"404", 20}, {"drop", 20}, {"slow", 12}, {"timeout", 3}},
	"slow":  {{"slow", 1}},
	"down":  {{"503", 1}},
	"gone":  {{"410", 1}},
}

func main() {
	port := envOr("PORT", "9000")
	if m := os.Getenv("SINK_MIX"); m != "" {
		scenarios["mixed"] = parseMix(m)
	}

	stats := newStats()
	go stats.report(5 * time.Second)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		scenario := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]
		if _, ok := scenarios[scenario]; !ok {
			scenario = "mixed"
		}

		if d, err := time.ParseDuration(r.URL.Query().Get("sleep")); err == nil {
			time.Sleep(d)
		}
		outcome := r.URL.Query().Get("status")
		if outcome == "" {
			outcome = pick(scenarios[scenario])
		}
		stats.add(scenario, outcome)

		switch outcome {
		case "drop":
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close() // the client sees the connection close with no response
					return
				}
			}
			w.WriteHeader(http.StatusBadGateway)
		case "slow":
			time.Sleep(time.Duration(300+rand.IntN(2200)) * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		case "timeout":
			time.Sleep(12 * time.Second)
			w.WriteHeader(http.StatusOK)
		default:
			code, err := strconv.Atoi(outcome)
			if err != nil {
				code = http.StatusOK
			}
			w.WriteHeader(code)
			if code != http.StatusNoContent {
				io.WriteString(w, http.StatusText(code))
			}
		}
	})

	log.Printf("sink listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func pick(ws []weighted) string {
	total := 0
	for _, w := range ws {
		total += w.weight
	}
	n := rand.IntN(total)
	for _, w := range ws {
		if n < w.weight {
			return w.outcome
		}
		n -= w.weight
	}
	return ws[len(ws)-1].outcome
}

func parseMix(s string) []weighted {
	var ws []weighted
	for _, part := range strings.Split(s, ",") {
		outcome, weight, ok := strings.Cut(strings.TrimSpace(part), ":")
		n, err := strconv.Atoi(weight)
		if !ok || err != nil || n <= 0 {
			log.Fatalf("SINK_MIX: bad entry %q, want outcome:weight", part)
		}
		ws = append(ws, weighted{outcome, n})
	}
	return ws
}

type stats struct {
	mu     sync.Mutex
	counts map[string]int
	total  int
	last   int
}

func newStats() *stats { return &stats{counts: map[string]int{}} }

func (s *stats) add(scenario, outcome string) {
	s.mu.Lock()
	s.counts[scenario+" "+outcome]++
	s.total++
	s.mu.Unlock()
}

// report prints the request rate and the running count per scenario and
// outcome, but only when something arrived since the last report.
func (s *stats) report(every time.Duration) {
	for range time.Tick(every) {
		s.mu.Lock()
		if s.total == s.last {
			s.mu.Unlock()
			continue
		}
		rate := float64(s.total-s.last) / every.Seconds()
		s.last = s.total
		keys := make([]string, 0, len(s.counts))
		for k := range s.counts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, " %s=%d", strings.ReplaceAll(k, " ", ":"), s.counts[k])
		}
		total := s.total
		s.mu.Unlock()
		log.Printf("%.0f req/s, %d total |%s", rate, total, b.String())
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
