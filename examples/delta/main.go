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

// A minimal program exposing increments instead of cumulative values.
//
// The whole change is the delta.Enable call below. Nothing else in this file --
// the metrics, the handler, the Inc and Observe calls -- differs from what it
// would be without it, and no scrape config has to change either: once enabled,
// the handler promhttp already hands out answers with increments.
//
// Run it and watch three scrapes of the same handler:
//
//	go run ./examples/delta
//
// then leave it running and scrape it yourself:
//
//	curl -s localhost:8080/metrics          # increments, and consumes them
//	curl -s 'localhost:8080/metrics?delta=0' # cumulative, consumes nothing
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
	// The one call that turns this on. It has to come before any metric is
	// created: tracking attaches to instances as a Vec creates them, so metrics
	// that already exist are invisible to it. When the order cannot be arranged
	// -- metrics registered in some package's init, say -- set Options.Gatherer
	// to the registry instead and everything in it is covered, at the cost of
	// gathering all of it on every scrape.
	delta.Enable(delta.Options{
		// Report what accrued since the last delivered scrape, and clear it.
		// Without this the exposition is still change-only, but the values are
		// the cumulative ones.
		ReportIncrements: true,
		// An instance nothing wrote is not reported at all. Gauges are the
		// exception: a current value left out would be a gap, not a saving, so
		// every gauge goes out on every scrape.
		DisableHeartbeat: true,
		// Drop an instance that has not changed for this many scrapes, so a key
		// that stops being used stops costing memory. A gauge is only dropped at
		// zero, so an in-flight counter is not deleted mid-request.
		IdleScrapes: 30,
		// Stamp the instance's creation time, so a deleted and recreated instance
		// is a new series rather than a counter that appears to go backwards.
		GenLabel: "gen",
	})

	// From here on down, nothing is delta-aware.
	reg := prometheus.NewRegistry()
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "demo_requests_total",
		Help: "Requests served, by route.",
	}, []string{"route"})
	inFlight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "demo_requests_in_flight",
		Help: "Requests currently being served, by route.",
	}, []string{"route"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "demo_request_duration_seconds",
		Help:    "Time to serve a request, by route.",
		Buckets: []float64{0.01, 0.1, 1},
	}, []string{"route"})
	reg.MustRegister(requests, inFlight, latency)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	serve := func(route string, n int) {
		for range n {
			inFlight.WithLabelValues(route).Inc()
			requests.WithLabelValues(route).Inc()
			latency.WithLabelValues(route).Observe(0.05)
			inFlight.WithLabelValues(route).Dec()
		}
	}

	serve("/a", 3)
	serve("/b", 1)
	show(handler, "/metrics", "scrape 1: everything written so far")

	serve("/a", 2)
	show(handler, "/metrics", "scrape 2: only /a moved, and only by 2")

	show(handler, "/metrics", "scrape 3: nothing moved, so only the gauges are here")

	show(handler, "/metrics?delta=0", "?delta=0: the ordinary cumulative exposition, which consumes nothing")

	serve("/b", 4)
	show(handler, "/metrics", "scrape 4: the cumulative read above did not eat /b's 4")

	fmt.Println("serving on :8080, try: curl -s localhost:8080/metrics")
	http.Handle("/metrics", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// show scrapes the handler and prints the sample lines, counters and gauges
// only, so the increments are easy to follow.
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
