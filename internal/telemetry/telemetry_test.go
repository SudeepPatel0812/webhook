package telemetry

import "testing"

func TestQueryName(t *testing.T) {
	for sql, want := range map[string]string{
		"-- name: claim_deliveries\nWITH busy AS (...)":  "claim_deliveries",
		"\n\t-- name:  insert_event  \n\t\tWITH x AS ()": "insert_event",
		"begin":                    "begin",
		"COMMIT;":                  "commit",
		"rollback":                 "rollback",
		"SELECT 1":                 "unnamed",
		"UPDATE x SET y = 1 -- no": "unnamed",
	} {
		if got := queryName(sql); got != want {
			t.Errorf("queryName(%q) = %q, want %q", sql, got, want)
		}
	}
}

func TestStatusClass(t *testing.T) {
	for _, c := range []struct {
		code   int
		failed bool
		want   string
	}{
		{200, false, "2xx"}, {204, false, "2xx"}, {404, false, "4xx"},
		{503, false, "5xx"}, {0, true, "network"}, {0, false, "network"},
	} {
		if got := StatusClass(c.code, c.failed); got != c.want {
			t.Errorf("StatusClass(%d, %v) = %q, want %q", c.code, c.failed, got, c.want)
		}
	}
}
