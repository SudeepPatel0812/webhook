// Package telemetry holds the service's metrics and tracing setup.
//
// The split between the two is deliberate. Metrics carry only labels with a
// small, fixed set of values (route, query name, outcome, status class), so
// Prometheus stays cheap and the dashboards stay readable. Anything with many
// values — delivery id, endpoint id, event id, error text — goes on spans
// instead, and histograms attach the trace id as an exemplar so a point on a
// latency chart links to the exact trace behind it.
package telemetry

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracer creates every span in the service. It delegates to whatever provider
// Setup installs, so it is safe to use from package-level code.
var Tracer = otel.Tracer("webhook")

// Setup installs the W3C trace-context propagator and, when otlpEndpoint is
// set, a tracer provider that batches spans to it. With no endpoint, tracing
// stays off: spans don't record, nothing is exported, and metrics still work.
//
// Sampling follows the standard OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG
// environment variables, which the SDK reads itself. The default keeps every
// trace, which is right locally; in production set
// OTEL_TRACES_SAMPLER=parentbased_traceidratio and OTEL_TRACES_SAMPLER_ARG=0.1.
func Setup(ctx context.Context, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if otlpEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	// OTEL_EXPORTER_OTLP_ENDPOINT is the collector's base URL; traces go to
	// /v1/traces under it, as the OTLP spec defines for that variable.
	url := strings.TrimRight(otlpEndpoint, "/") + "/v1/traces"
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(url))
	if err != nil {
		return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", "webhook-service")))
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// TraceParent returns the W3C traceparent header value for the span in ctx, or
// "" if there is none. It is how a trace crosses the service's async hops: the
// value is stored on the outbox and deliveries rows and sent as a Kafka header.
func TraceParent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c["traceparent"]
}

// FromTraceParent returns ctx carrying the span context in tp, so a span
// started from it joins that trace. An empty or malformed tp returns ctx
// unchanged, and the next span starts a new trace.
func FromTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}

// TraceID returns the trace id for the span in ctx, or "" if it isn't sampled.
// Used to put the trace id on log lines.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsSampled() {
		return ""
	}
	return sc.TraceID().String()
}

// Observe records v on h. When ctx holds a sampled span, the trace id is
// attached as an exemplar, which is what lets Grafana link a point on a
// latency chart to the trace that produced it.
func Observe(ctx context.Context, h prometheus.Observer, v float64) {
	if id := TraceID(ctx); id != "" {
		if eo, ok := h.(prometheus.ExemplarObserver); ok {
			eo.ObserveWithExemplar(v, prometheus.Labels{"trace_id": id})
			return
		}
	}
	h.Observe(v)
}
