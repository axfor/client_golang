// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Exposing increments instead of cumulative values.
//
// # What this is shaped like
//
// One process measures two things that want different treatment:
//
//   - Framework metrics. A few dozen series, fixed: no per-request labels, so
//     the count does not grow. Watched on a dashboard, wanted every ten seconds.
//   - Per-key metrics. One set per API key, so hundreds of thousands of series.
//     They feed billing, so they are wanted complete rather than fresh, and once
//     a minute is enough.
//
// # Two constraints settle the shape
//
// One: an endpoint that reports increments has one consumer. What it reports is
// what accrued since it last delivered successfully, and that baseline is one
// baseline. A second scraper on the same endpoint takes increments the first one
// then never sees -- and nothing fails, the numbers are just quietly short. So
// each scraper gets its own endpoint.
//
// Two: an aggregator adds up what a counter reports and reads a gauge as it
// stands. Those are different operations, and the exposition format carries no
// type for it to switch on, so it is told which metric is which -- by name, in a
// list somebody keeps up to date. The asymmetry is what makes that bad: a
// counter added later matches a pattern like acg_.+ and is right, a gauge added
// later matches the same pattern, is silently accumulated, and nothing fails.
//
// Naming conventions do not save it either. A metric called
// requests_concurrent_total is a gauge whose name ends in _total, and the rule
// usually recommended for this -- everything ending in _total or _bucket is a
// counter -- gets it wrong. It is below, so the output shows what that costs.
//
// TypeLabel puts the type on the sample instead, and the aggregation is then
// selected by what the metric is:
//
//   - match: '{_metric_type!="gauge"}'
//     drop_input_labels: [_metric_type]
//     outputs: [sum_samples_total]
//   - match: '{_metric_type="gauge"}'
//     drop_input_labels: [_metric_type]
//     outputs: [sum_samples]
//
// Two rules, written once, that no metric added later changes. The label is
// dropped before the aggregator groups, so it does not reach what is stored, and
// a label repeated on every sample is 0.2% of a zstd-compressed exposition.
//
// # The endpoints
//
//	/metrics             framework, cumulative        10s, scrape config unchanged
//	/metrics/usage       per-key, increments + type   60s, one metrics_path line
//	/metrics/cumulative  per-key, cumulative          for debugging, consumes nothing
//
// The framework endpoint is a plain handler, not a delta one: a few dozen fixed
// series gain nothing from reporting increments, and making them increments
// would mean aggregating them too. A plain handler consumes no increments, so it
// sits beside the delta endpoint safely.
//
// The debugging endpoint is there because an exposer's own Handler answers every
// request with a delta -- ?delta=0 is read by delta.Enable, the other way to
// wire this up, and an exposer mounted by hand never sees it.
//
//	go run ./examples/delta
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/promhttp/delta"
)

func main() {
	// Change tracking attaches to instances as a Vec creates them, so this has
	// to come before any metric is created -- instances that already exist are
	// invisible to it. Where the order cannot be arranged, because some package
	// registers metrics in its init, set Options.Gatherer instead: everything in
	// that registry is then covered, at the cost of gathering all of it on every
	// scrape.
	trk := prometheus.EnableChangeTracking()

	// Everything from here to the exposers is the application's own: these are
	// the metrics it would define and write either way, and nothing about them
	// changes to report increments.
	frameworkReg, usageReg, m := defineMetrics()

	// Increments() is the combination this package recommends for feeding an
	// aggregator: report what accrued since the last delivered scrape, say
	// nothing at all about an instance nothing wrote, and let one that has gone
	// quiet for half an hour go. Assembling it by hand from the fields is how it
	// goes wrong, so it is one call.
	//
	// Note what is not in it: GenLabel. That belongs to the other scheme, where
	// the sidecar reports cumulative values and the aggregator differences them
	// -- there a deleted and recreated instance restarts at zero and looks like
	// a counter reset, and the generation makes it a new series instead. An
	// increment is already right across a restart, so the label would buy
	// nothing and would have to be aggregated away again. Setting both panics.
	opts := delta.Increments()
	opts.TypeLabel = "_metric_type"

	usage := delta.NewTracked(trk, opts)
	cumulative := promhttp.HandlerFor(usageReg, promhttp.HandlerOpts{})
	framework := promhttp.HandlerFor(frameworkReg, promhttp.HandlerOpts{})

	serve := func(route, key string, n int) {
		for range n {
			m.routerRequests.WithLabelValues(route).Inc()
			m.inFlight.WithLabelValues(key).Inc()
			m.requests.WithLabelValues(key).Inc()
			m.latency.WithLabelValues(key).Observe(0.05)
			m.inFlight.WithLabelValues(key).Dec()
		}
	}

	// Three framework scrapes to one usage scrape, the way a 10s job and a 60s
	// job interleave. The framework endpoint is cumulative and consumes nothing,
	// so the usage scrape that follows still reports every request.
	serve("/chat", "key-a", 3)
	show(framework, "/metrics", "framework scrape 1 of 3 -- cumulative, consumes nothing")

	serve("/chat", "key-a", 2)
	serve("/embed", "key-b", 1)
	show(framework, "/metrics", "framework scrape 2 of 3")

	serve("/chat", "key-a", 1)
	show(framework, "/metrics", "framework scrape 3 of 3")

	show(usage.Handler(), "/metrics/usage",
		"usage: all 7 requests, none eaten by the framework scrapes -- and every sample says what it is")

	show(usage.Handler(), "/metrics/usage",
		"usage with nothing written in between: only the gauges, which are current values and cannot be left out")

	show(cumulative, "/metrics/cumulative",
		"a plain cumulative read, for debugging -- touches no baseline")

	serve("/chat", "key-a", 4)
	show(usage.Handler(), "/metrics/usage",
		"the debugging read above did not eat these 4 either")

	mux := http.NewServeMux()
	mux.Handle("/metrics", framework)
	mux.Handle("/metrics/usage", usage.Handler())
	mux.Handle("/metrics/cumulative", cumulative)

	fmt.Println("\nserving on :9090")
	fmt.Println("  /metrics             framework, cumulative      -- the 10s job, scrape config unchanged")
	fmt.Println("  /metrics/usage       per-key, increments + type -- the 60s job, one metrics_path line")
	fmt.Println("  /metrics/cumulative  cumulative, debugging only -- touches no baseline")
	log.Fatal(http.ListenAndServe(":9090", mux))
}

// show scrapes a handler and prints the sample lines, histogram buckets left
// out, so the increments are easy to follow.
func show(h http.Handler, target, what string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))

	var lines []string
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") || strings.Contains(line, "_bucket{") {
			continue
		}
		lines = append(lines, "  "+line)
	}
	sort.Strings(lines)
	fmt.Printf("\n== %s\n", what)
	if len(lines) == 0 {
		fmt.Println("  (nothing)")
		return
	}
	fmt.Println(strings.Join(lines, "\n"))
}

// metrics is what the application measures. None of it is delta-aware: these
// are ordinary Vecs, written with ordinary Inc and Observe calls, and they read
// the same whether or not increments are being reported.
type metrics struct {
	routerRequests *prometheus.CounterVec
	requests       *prometheus.CounterVec
	latency        *prometheus.HistogramVec
	inFlight       *prometheus.GaugeVec
}

func defineMetrics() (framework, usage *prometheus.Registry, m metrics) {
	framework, usage = prometheus.NewRegistry(), prometheus.NewRegistry()

	// Framework metrics: labels that do not grow with traffic.
	m.routerRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "router_requests_total",
		Help: "Requests handled, by route.",
	}, []string{"route"})
	framework.MustRegister(m.routerRequests)

	// Per-key metrics. Note requests_concurrent_total: a gauge whose name ends
	// in _total, which is exactly the case a naming convention gets wrong.
	m.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "usage_requests_total",
		Help: "Requests served, by API key.",
	}, []string{"apikey_id"})
	m.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "usage_duration_seconds",
		Help:    "Time to serve a request, by API key.",
		Buckets: []float64{0.1, 1},
	}, []string{"apikey_id"})
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "usage_requests_concurrent_total",
		Help: "Requests in flight, by API key.",
	}, []string{"apikey_id"})
	usage.MustRegister(m.requests, m.latency, m.inFlight)
	return framework, usage, m
}
