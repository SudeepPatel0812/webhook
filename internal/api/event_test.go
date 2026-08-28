package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"webhook/internal/domain"
	"webhook/internal/repository"
)

type fakeInserter struct {
	err    error
	called bool
	got    domain.Event
}

func (f *fakeInserter) Insert(_ context.Context, e domain.Event) error {
	f.called = true
	f.got = e
	return f.err
}

func TestEventHandler_Ingest(t *testing.T) {
	const valid = `{"application_id":1,"event_type":"payment.succeeded","payload":{"amount":100}}`

	tests := []struct {
		name       string
		body       string
		key        string
		insertErr  error
		wantStatus int
		wantStored bool
	}{
		{"accepted", valid, "key-1", nil, http.StatusAccepted, true},
		{"idempotent replay", valid, "key-1", repository.ErrDuplicate, http.StatusOK, true},
		{"malformed json", `{`, "key-1", nil, http.StatusBadRequest, false},
		{"missing idempotency key", valid, "", nil, http.StatusUnprocessableEntity, false},
		{"missing event_type", `{"application_id":1,"payload":{}}`, "key-1", nil, http.StatusUnprocessableEntity, false},
		{"non-positive application_id", `{"application_id":0,"event_type":"x","payload":{}}`, "key-1", nil, http.StatusUnprocessableEntity, false},
		{"store failure", valid, "key-1", errors.New("boom"), http.StatusInternalServerError, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeInserter{err: tc.insertErr}
			h := NewEventHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))

			r := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(tc.body))
			if tc.key != "" {
				r.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()

			h.Ingest(w, r)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if store.called != tc.wantStored {
				t.Errorf("store called = %v, want %v", store.called, tc.wantStored)
			}
		})
	}
}
