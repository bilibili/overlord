package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"overlord/platform/monitor/exporter/redis_exporter/exporter"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

var (
	namespace     = flag.String("namespace", "redis", "Namespace for metrics")
	checkKeys     = flag.String("check-keys", "", "Comma separated list of keys to export value and length/size")
	listenAddress = flag.String("web.listen-address", ":9121", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")

	// VERSION, BUILD_DATE, GIT_COMMIT are filled in by the build script
	VERSION     = "<<< filled in by build >>>"
	BUILD_DATE  = "<<< filled in by build >>>"
	COMMIT_SHA1 = "<<< filled in by build >>>"
)

func main() {
	flag.Parse()

	http.HandleFunc(*metricPath, func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()

		target := params.Get("target")
		password := params.Get("password")

		addrs := strings.Split(target, ",")
		passwords := strings.Split(password, ",")
		aliases := strings.Split("", ",")

		if target == "" {
			http.Error(w, "Target parameter is missing", 400)
			return
		}

		exp, err := exporter.NewRedisExporter(
			exporter.RedisHost{Addrs: addrs, Passwords: passwords, Aliases: aliases},
			*namespace,
			*checkKeys,
			"")
		if err != nil {
			fmt.Println(err)
		}

		buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "redis_exporter_build_info",
			Help: "redis exporter build_info",
		}, []string{"version", "commit_sha", "build_date", "golang_version"})
		buildInfo.WithLabelValues(VERSION, COMMIT_SHA1, BUILD_DATE, runtime.Version()).Set(1)

		registry := prometheus.NewRegistry()
		_ = registry.Register(exp)
		_ = registry.Register(buildInfo)

		mfs, err := registry.Gather()
		if err != nil {
			fmt.Println(err)
		}

		var buf bytes.Buffer
		for _, mf := range mfs {
			if _, err := expfmt.MetricFamilyToText(&buf, mf); err != nil {
				fmt.Println(err)
			}
		}
		_, _ = w.Write(buf.Bytes())

	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`
<html>
<head><title>Redis Exporter v` + VERSION + `</title></head>
<body>
<h1>Redis Exporter v` + VERSION + `</h1>
<p><a href='` + *metricPath + `'>Metrics</a></p>
</body>
</html>
						`))
	})

	fmt.Printf("Providing metrics at %s%s", *listenAddress, *metricPath)
	fmt.Println(http.ListenAndServe(*listenAddress, nil))
}
