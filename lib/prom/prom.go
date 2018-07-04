package prom

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	statConns = "overlord_proxy_conns"
	statErr   = "overlord_proxy_err"
	statHit   = "overlord_proxy_hit"
	statMiss  = "overlord_proxy_miss"

	statProxyTimer   = "overlord_proxy_timer"
	statHandlerTimer = "overlord_proxy_handler_timer"
)

var (
	conns        *prometheus.GaugeVec
	gerr         *prometheus.GaugeVec
	hit          *prometheus.CounterVec
	miss         *prometheus.CounterVec
	proxyTimer   *prometheus.HistogramVec
	handlerTimer *prometheus.HistogramVec

	clusterLabels        = []string{"cluster"}
	clusterNodeLabels    = []string{"cluster", "node"}
	clusterNodeErrLabels = []string{"cluster", "node", "cmd", "error"}
	clusterCmdLabels     = []string{"cluster", "cmd"}
	clusterNodeCmdLabels = []string{"cluster", "node", "cmd"}
)

// Init init prometheus.
func Init() {
	conns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: statConns,
			Help: statConns,
		}, clusterLabels)
	prometheus.MustRegister(conns)
	gerr = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: statErr,
			Help: statErr,
		}, clusterNodeErrLabels)
	prometheus.MustRegister(gerr)
	hit = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: statHit,
			Help: statHit,
		}, clusterNodeLabels)
	prometheus.MustRegister(hit)
	miss = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: statMiss,
			Help: statMiss,
		}, clusterNodeLabels)
	prometheus.MustRegister(miss)
	proxyTimer = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    statProxyTimer,
			Help:    statProxyTimer,
			Buckets: prometheus.LinearBuckets(0, 10, 1),
		}, clusterCmdLabels)
	prometheus.MustRegister(proxyTimer)
	handlerTimer = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    statHandlerTimer,
			Help:    statHandlerTimer,
			Buckets: prometheus.LinearBuckets(0, 10, 1),
		}, clusterNodeCmdLabels)
	prometheus.MustRegister(handlerTimer)
	// metrics
	metrics()
}

func metrics() {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		h := promhttp.Handler()
		h.ServeHTTP(w, r)
	})
}

// ProxyTime log timing information (in milliseconds).
func ProxyTime(cluster, node string, ts int64) {
	if proxyTimer == nil {
		return
	}
	proxyTimer.WithLabelValues(cluster, node).Observe(float64(ts))
}

// HandleTime log timing information (in milliseconds).
func HandleTime(cluster, node, cmd string, ts int64) {
	if handlerTimer == nil {
		return
	}
	handlerTimer.WithLabelValues(cluster, node, cmd).Observe(float64(ts))
}

// ErrIncr increments one stat error counter.
func ErrIncr(cluster, node, cmd, err string) {
	if gerr == nil {
		return
	}
	gerr.WithLabelValues(cluster, node, cmd, err).Inc()
}

// ConnIncr increments one stat error counter.
func ConnIncr(cluster string) {
	if conns == nil {
		return
	}
	conns.WithLabelValues(cluster).Inc()
}

// ConnDecr decrements one stat error counter.
func ConnDecr(cluster string) {
	if conns == nil {
		return
	}
	conns.WithLabelValues(cluster).Dec()
}

// Hit increments one stat hit counter.
func Hit(cluster, node string) {
	if hit == nil {
		return
	}
	hit.WithLabelValues(cluster, node).Inc()
}

// Miss decrements one stat miss counter.
func Miss(cluster, node string) {
	if miss == nil {
		return
	}
	miss.WithLabelValues(cluster, node).Inc()
}
