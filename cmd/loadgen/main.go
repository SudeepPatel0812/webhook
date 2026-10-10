// Command loadgen fires N synthetic events at the ingest API using a pool of
// workers. Each event gets a unique Idempotency-Key (per run), so re-running
// produces fresh events rather than all-duplicates.
//
//	go run ./cmd/loadgen -n 5000 -c 20 -apps 1,2,4
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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var eventTypes = []string{"user.created", "user.updated", "order.paid", "order.refunded", "email.bounced"}

func main() {
	var (
		url     = flag.String("url", "http://localhost:8081", "base URL of the webhook service")
		n       = flag.Int("n", 1000, "number of events to send")
		conc    = flag.Int("c", 10, "concurrent workers")
		appList = flag.String("apps", "1", "comma-separated application IDs to spread events across")
		dry     = flag.Bool("dry", false, "print one generated event and exit without sending")
	)
	flag.Parse()

	var apps []int64
	for _, s := range strings.Split(*appList, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			log.Fatalf("-apps: %q is not an application id", s)
		}
		apps = append(apps, id)
	}

	runID := time.Now().Unix()

	if *dry {
		body, key := build(runID, 0, apps)
		fmt.Printf("Idempotency-Key: %s\n%s\n", key, body)
		return
	}

	endpoint := *url + "/v1/events"
	// Keep one idle connection per worker. The default (2 per host) makes
	// every other request open a new connection, which at tens of thousands
	// of requests runs the machine out of local ports.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = *conc
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}

	var sent, dup, failed int64
	jobs := make(chan int, *conc)
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				switch send(client, endpoint, runID, i, apps) {
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
func send(c *http.Client, endpoint string, runID int64, i int, apps []int64) int {
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
	// Read the body to the end: Go only reuses a connection whose body was
	// fully read. Without this every request opened a new TCP connection,
	// which at 50k requests overflowed the OS connection queue.
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// build returns the JSON body and Idempotency-Key for event i of this run.
func build(runID int64, i int, apps []int64) ([]byte, string) {
	key := "loadgen-" + strconv.FormatInt(runID, 10) + "-" + strconv.Itoa(i)
	ev := map[string]any{
		"application_id": apps[i%len(apps)],
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
