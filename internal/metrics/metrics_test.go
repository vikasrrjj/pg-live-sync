package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	RegisterHandlers(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	cases := []struct {
		path           string
		beforeReady    int
		afterReady     int
		bodyContains   string
		metricsContain string
	}{
		{"/healthz", 200, 200, "ok", ""},
		{"/readyz", 503, 200, "ready", ""},
		{"/metrics", 200, 200, "", "cdc_ready"},
	}

	for _, tc := range cases {
		SetReady(false)
		resp, err := server.Client().Get(server.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		if resp.StatusCode != tc.beforeReady {
			t.Fatalf("GET %s before ready: status=%d want %d", tc.path, resp.StatusCode, tc.beforeReady)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if tc.bodyContains != "" && !strings.Contains(string(body), tc.bodyContains) {
			t.Fatalf("GET %s body=%q, want substring %q", tc.path, body, tc.bodyContains)
		}
		if tc.metricsContain != "" && !strings.Contains(string(body), tc.metricsContain) {
			t.Fatalf("GET %s metrics missing %q", tc.path, tc.metricsContain)
		}

		SetReady(true)
		resp, err = server.Client().Get(server.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		if resp.StatusCode != tc.afterReady {
			t.Fatalf("GET %s after ready: status=%d want %d", tc.path, resp.StatusCode, tc.afterReady)
		}
		resp.Body.Close()
	}
}

func TestSetLagUpdatesHeadroom(t *testing.T) {
	SetLag(10, 100)
	if got := testutil.ToFloat64(LagSeconds); got != 10 {
		t.Fatalf("lag = %v, want 10", got)
	}
	if got := testutil.ToFloat64(RetentionHeadroomSeconds); got != 90 {
		t.Fatalf("headroom = %v, want 90", got)
	}
}