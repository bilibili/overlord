package prom

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	statConns    = "overlord_proxy_conns"
	statErr      = "overlord_proxy_err"
	statVersions = "overlord_proxy_version"

	statProxyTimer   = "overlord_proxy_timer"
	statHandlerTimer = "overlord_proxy_handler_timer"
)

var (
	conns        *prometheus.GaugeVec
	versions     *prometheus.GaugeVec
	gerr         *prometheus.GaugeVec
	proxyTimer   *prometheus.HistogramVec
	handlerTimer *prometheus.HistogramVec

	clusterLabels        = []string{"cluster"}
	clusterNodeErrLabels = []string{"cluster", "node", "cmd", "error"}
	clusterCmdLabels     = []string{"cluster", "cmd"}
	clusterNodeCmdLabels = []string{"cluster", "node", "cmd"}
	versionLabels        = []string{"version"}
	// On Prom switch
	On = true
)

// Init init prometheus.
func Init() {
	conns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: statConns,
			Help: statConns,
		}, clusterLabels)
	prometheus.MustRegister(conns)
	versions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: statVersions,
			Help: statVersions,
		}, versionLabels)
	prometheus.MustRegister(versions)
	gerr = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: statErr,
			Help: statErr,
		}, clusterNodeErrLabels)
	prometheus.MustRegister(gerr)
	proxyTimer = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    statProxyTimer,
			Help:    statProxyTimer,
			Buckets: []float64{1000, 2000, 4000, 10000},
		}, clusterCmdLabels)

	prometheus.MustRegister(proxyTimer)
	handlerTimer = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    statHandlerTimer,
			Help:    statHandlerTimer,
			Buckets: []float64{1000, 2000, 4000, 10000},
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

// VersionState set current versioin state.
func VersionState(version string) {
	if versions == nil {
		return
	}
	versions.WithLabelValues(version).Set(1)
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
