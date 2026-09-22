// Package metrics exposes Prometheus instrumentation shared by the source
// reader and destination writer. Both binaries serve /metrics, /healthz and
// /readyz, but each registers its own values against the package-level metrics
// below so operators get a consistent view of the pipeline regardless of which
// process they scrape.
package metrics

import (
	"log"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is the custom register used by the pipeline. It is separate from
// the default registry so tests and imported libraries do not leak collectors.
var Registry = prometheus.NewRegistry()

var (
	MessagesProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_messages_processed_total",
		Help: "Total CDC events processed, labeled by operation.",
	}, []string{"operation"})

	MessagesPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cdc_messages_published_total",
		Help: "Total CDC events the source reader has published to Kafka.",
	})

	TransactionsAcked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cdc_transactions_acked_total",
		Help: "Total source transactions whose Kafka produce results are durable.",
	})

	Errors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_errors_total",
		Help: "Total errors, labeled by kind.",
	}, []string{"kind"})

	DeadLetters = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cdc_dead_letters_total",
		Help: "Total events quarantined to the destination dead-letter queue.",
	})

	LagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_lag_seconds",
		Help: "Seconds between the event's source commit time and now.",
	})

	RetentionHeadroomSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_retention_headroom_seconds",
		Help: "Estimated seconds of source/destination retention budget remaining; negative means data loss risk if the stoppage continues.",
	})

	PendingEvents = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_pending_events",
		Help: "Number of events buffered in the current destination transaction.",
	})

	GroupAgeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_group_age_seconds",
		Help: "Seconds since the first event of the currently open destination transaction.",
	})

	BatchDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cdc_batch_duration_seconds",
		Help:    "Duration of destination apply batches.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})

	BatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cdc_batch_size",
		Help:    "Number of events in a destination apply batch.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	Ready = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_ready",
		Help: "1 if the component is ready, 0 otherwise.",
	})
)

func init() {
	Registry.MustRegister(
		MessagesProcessed,
		MessagesPublished,
		TransactionsAcked,
		Errors,
		DeadLetters,
		LagSeconds,
		RetentionHeadroomSeconds,
		PendingEvents,
		GroupAgeSeconds,
		BatchDurationSeconds,
		BatchSize,
		Ready,
	)
}

var ready atomic.Bool

// SetReady toggles the /readyz endpoint and the cdc_ready gauge.
func SetReady(isReady bool) {
	ready.Store(isReady)
	if isReady {
		Ready.Set(1)
	} else {
		Ready.Set(0)
	}
}

// IsReady reports the current readiness state.
func IsReady() bool {
	return ready.Load()
}

// SetLag updates the lag gauge and the retention-headroom gauge using the
// configured retention budget in seconds.
func SetLag(lagSeconds, retentionSeconds float64) {
	LagSeconds.Set(lagSeconds)
	RetentionHeadroomSeconds.Set(retentionSeconds - lagSeconds)
}

// RegisterHandlers wires /metrics, /healthz and /readyz onto the supplied mux.
// Tests can use it with httptest; production binaries use StartServer.
func RegisterHandlers(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if IsReady() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	})
}

// StartServer runs an HTTP server in a goroutine that exposes /metrics,
// /healthz and /readyz. The server is fire-and-forget; failures are logged.
func StartServer(addr string) {
	mux := http.NewServeMux()
	RegisterHandlers(mux)

	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("metrics server listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("error metrics server: %v", err)
		}
	}()
}
