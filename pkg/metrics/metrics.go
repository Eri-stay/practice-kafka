package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var FinalEmailOutcome = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "email_final_outcome_total",
		Help: "Total number of unique emails that reached a terminal state",
	},
	[]string{"outcome"},
)

var DeliveryAttempts = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "email_delivery_attempts_total",
		Help: "Total number of SMTP delivery attempts made by server",
	},
	[]string{"status"},
)

var SMTPRequestDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "smtp_request_duration_seconds",
		Help:    "Duration of SMTP DialAndSend operations",
		Buckets: prometheus.DefBuckets, // 0.005s to 10s
	},
)

var DispatcherBatchSize = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "dispatcher_batch_size",
		Help:    "Number of emails picked up by the dispatcher per tick",
		Buckets: []float64{0, 1, 5, 10, 50, 100},
	},
	[]string{"type"}, // "pending" or "failed"
)

func StartMetricsServer(port string) {
	if port == "" {
		port = "8080"
	}
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting metrics server on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Printf("Metrics server failed: %v", err)
	}
}
