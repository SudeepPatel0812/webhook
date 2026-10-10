package telemetry

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// QueryTracer instruments every query that goes through pgx: Exec, Query,
// QueryRow, and the begin/commit/rollback a transaction sends.
//
// Each query is identified by the `-- name: <name>` comment on the first line
// of its SQL, so metrics and spans say "claim_deliveries", not a wall of SQL.
// The SQL text and the arguments are never recorded: the text is a constant
// you can find by its name, and the arguments hold customer payloads.
//
// Every query is counted and timed. A span is created only when the query runs
// inside a span that is already recording. That keeps the background loops
// from producing a trace for every idle poll: the claim query that runs every
// 2s and finds nothing shows up in the metrics, but not as thousands of
// one-span traces.
type QueryTracer struct{}

type queryKey struct{}

type queryState struct {
	name  string
	start time.Time
	span  trace.Span
}

func (QueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	st := &queryState{name: queryName(data.SQL), start: time.Now()}
	if trace.SpanFromContext(ctx).IsRecording() {
		ctx, st.span = Tracer.Start(ctx, "db "+st.name,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("db.system.name", "postgresql"),
				attribute.String("db.query.summary", st.name),
			))
	}
	return context.WithValue(ctx, queryKey{}, st)
}

func (QueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	st, ok := ctx.Value(queryKey{}).(*queryState)
	if !ok {
		return
	}
	outcome := "ok"
	if data.Err != nil {
		outcome = "error"
	}
	DBQueries.WithLabelValues(st.name, outcome).Inc()
	Observe(ctx, DBQueryDuration.WithLabelValues(st.name), time.Since(st.start).Seconds())

	if st.span == nil {
		return
	}
	st.span.SetAttributes(attribute.Int64("db.response.rows_affected", data.CommandTag.RowsAffected()))
	if data.Err != nil {
		var pgErr *pgconn.PgError
		if errors.As(data.Err, &pgErr) {
			st.span.SetAttributes(attribute.String("db.response.status_code", pgErr.Code))
		}
		st.span.RecordError(data.Err)
		st.span.SetStatus(codes.Error, data.Err.Error())
	}
	st.span.End()
}

// queryName reads the `-- name:` comment from the start of sql. Transaction
// control statements keep their own name; anything else unnamed is grouped
// under "unnamed" so a missing comment can't create unbounded label values.
func queryName(sql string) string {
	s := strings.TrimSpace(sql)
	if rest, ok := strings.CutPrefix(s, "-- name:"); ok {
		name, _, _ := strings.Cut(rest, "\n")
		return strings.TrimSpace(name)
	}
	switch kw := strings.ToLower(strings.TrimSuffix(s, ";")); kw {
	case "begin", "commit", "rollback":
		return kw
	}
	return "unnamed"
}
