// Command loadgen fires N synthetic events at the ingest API using a pool of
// workers. Each event gets a unique Idempotency-Key (per run), so re-running
// produces fresh events rather than all-duplicates.
//
//	go run ./cmd/loadgen -n 5000 -c 20
//	go run ./cmd/loadgen -n 1 -dry            # print a sample event, send nothing
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var eventTypes = []string{"user.created", "user.updated", "order.paid", "order.refunded", "email.bounced"}

func main() {
	var (
		url  = flag.String("url", "http://localhost:8080", "base URL of the webhook service")
		n    = flag.Int("n", 1000, "number of events to send")
		conc = flag.Int("c", 10, "concurrent workers")
		apps = flag.Int("apps", 1, "spread events across application IDs 1..apps")
		dry  = flag.Bool("dry", false, "print one generated event and exit without sending")
	)
	flag.Parse()

	runID := time.Now().Unix()

	if *dry {
		body, key := build(runID, 0, *apps)
		fmt.Printf("Idempotency-Key: %s\n%s\n", key, body)
		return
	}

	endpoint := *url + "/v1/events"
	client := &http.Client{Timeout: 10 * time.Second}

	var sent, dup, failed int64
	jobs := make(chan int, *conc)
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				switch send(client, endpoint, runID, i, *apps) {
				case http.StatusAccepted:
					atomic.AddInt64(&sent, 1)
				case http.StatusOK:
					atomic.AddInt64(&dup, 1)
				default:
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("done in %s: %d accepted, %d duplicate, %d failed (%.0f req/s)\n",
		elapsed.Round(time.Millisecond), sent, dup, failed, float64(*n)/elapsed.Seconds())
	if failed > 0 {
		os.Exit(1)
	}
}

// send posts one event and returns the HTTP status code (0 on transport error).
func send(c *http.Client, endpoint string, runID int64, i, apps int) int {
	body, key := build(runID, i, apps)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		log.Printf("build request: %v", err)
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)

	resp, err := c.Do(req)
	if err != nil {
		log.Printf("event %d: %v", i, err)
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Printf("event %d: %s %s", i, resp.Status, bytes.TrimSpace(msg))
	}
	return resp.StatusCode
}

// build returns the JSON body and Idempotency-Key for event i of this run.
func build(runID int64, i, apps int) ([]byte, string) {
	key := "loadgen-" + strconv.FormatInt(runID, 10) + "-" + strconv.Itoa(i)
	ev := map[string]any{
		"application_id": int64(i%apps + 1),
		"event_type":     eventTypes[rand.Intn(len(eventTypes))],
		"payload": map[string]any{
			"seq": i,
			"id":  rand.Int63(),
			"ts":  time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	body, _ := json.Marshal(ev)
	return body, key
}
