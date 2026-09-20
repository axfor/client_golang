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

// Exposing increments instead of cumulative values, to three scrapers at once.
//
// # What this is shaped like
//
// One process measures things of three different shapes, and each wants
// something different from a scraper:
//
//   - Framework metrics. A few dozen series, fixed: no per-request labels, so
//     the count does not grow. Watched on a dashboard, wanted every ten seconds.
//   - Per-key counters and histograms. One set per API key, so hundreds of
//     thousands of series. They feed billing, so they are wanted complete rather
//     than fresh, and once a minute is enough.
//   - Per-key gauges. Requests in flight, a breaker's state, a config version:
//     current values, not something that accumulates.
//
// # Why three endpoints
//
// Two constraints, and between them they settle the shape.
//
// One: an endpoint that reports increments has one consumer. What it reports is
// what accrued since it last delivered successfully, and that baseline is one
// baseline. A second scraper on the same endpoint takes increments the first one
// then never sees -- and nothing fails, the numbers are just quietly short.
//
// Two: an aggregator adds up what a counter reports and reads a gauge as it
// stands. Those are different operations, and the exposition format carries no
// type for it to switch on, so it has to be told which metric is which -- by
// name, in a list somebody has to keep up to date. The asymmetry is what makes
// that bad: a counter added later matches a pattern like acg_.+ and is right, a
// gauge added later matches the same pattern and is silently accumulated, and
// nothing fails there either.
//
// So: one endpoint per consumer, and the two kinds split by Options.Only, which
// puts the knowledge of which is which back in the only place that has it.
//
//	/metrics              framework, cumulative   10s   plain promhttp
//	/metrics/usage        counters + histograms   60s   delta.NewTracked, Accumulating
//	/metrics/usage-gauges gauges                  60s   delta.New, Current
//
// The aggregator then picks the arithmetic by which endpoint it scraped:
//
//   - match: '{job="usage"}'         outputs: [sum_samples_total]
//   - match: '{job="usage-gauges"}'  outputs: [sum_samples]
//
// No metric name appears anywhere in that config, and a metric added later
// lands on the right endpoint by its own type.
//
// # Why the three are built differently
//
// The tracked exposer finds what to report by draining a list of the instances
// written since the last scrape, which is what makes it hold up at a million
// series. That list is shared, so a second tracked exposer would drain it from
// the first -- which is why the gauge endpoint gathers instead. It walks its
// registry every scrape and compares, which at a few thousand gauges costs
// nothing and touches no dirty list.
//
// The framework endpoint is not a delta exposer at all. A few dozen fixed series
// gain nothing from reporting increments, and making them increments would mean
// aggregating them too -- a second thing to configure for no benefit. A plain
// handler consumes no increments, so it sits beside the other two safely, and
// the ten-second job's scrape config does not change at all.
//
// # Reading cumulative values
//
// An exposer's own Handler answers every request with a delta. ?delta=0 is read
// by delta.Enable, the other way to wire this up; an exposer mounted by hand
// never sees it. Anything that wants cumulative values gets a plain promhttp
// handler of its own, which touches no baseline and is safe to curl next to a
// running scraper.
//
//	go run ./examples/delta
//
//	curl -s localhost:9090/metrics               framework, cumulative
//	curl -s localhost:9090/metrics/usage         counters + histograms, increments
//	curl -s localhost:9090/metrics/usage-gauges  gauges, current values
//	curl -s localhost:9090/metrics/cumulative    the same per-key metrics, cumulative, for debugging
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

	frameworkReg := prometheus.NewRegistry()
	usageReg := prometheus.NewRegistry()

	// Framework metrics: labels that do not grow with traffic.
	routerRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "router_requests_total",
		Help: "Requests handled, by route.",
	}, []string{"route"})
	frameworkReg.MustRegister(routerRequests)

	// Per-key metrics: two that accumulate, one that is read as it stands.
	usageRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "usage_requests_total",
		Help: "Requests served, by API key.",
	}, []string{"apikey_id"})
	usageTokens := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "usage_tokens_total",
		Help: "Tokens consumed, by API key.",
	}, []string{"apikey_id"})
	usageInFlight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "usage_requests_in_flight",
		Help: "Requests in flight, by API key.",
	}, []string{"apikey_id"})
	usageReg.MustRegister(usageRequests, usageTokens, usageInFlight)

	// Increments() is the combination this package recommends for feeding an
	// aggregator: report what accrued since the last delivered scrape, say
	// nothing at all about an instance nothing wrote, and let one that has gone
	// quiet for half an hour go. Assembling it by hand from the eight fields is
	// how it goes wrong, so it is one call.
	//
	// Note what is not here: GenLabel. It belongs to the other scheme, where the
	// sidecar reports cumulative values and the aggregator differences them --
	// there a deleted and recreated instance restarts at zero and looks like a
	// counter reset, and the generation makes it a new series instead. An
	// increment is already right across a restart, so the label would buy
	// nothing and would have to be aggregated away again. Setting both panics.
	opts := delta.Increments()

	counters := delta.NewTracked(trk, opts.WithOnly(delta.Accumulating))
	gauges := delta.New(usageReg, delta.Options{DisableHeartbeat: true}.WithOnly(delta.Current))
	cumulative := promhttp.HandlerFor(usageReg, promhttp.HandlerOpts{})
	framework := promhttp.HandlerFor(frameworkReg, promhttp.HandlerOpts{})

	serve := func(route, key string, n int) {
		for range n {
			routerRequests.WithLabelValues(route).Inc()
			usageInFlight.WithLabelValues(key).Inc()
			usageRequests.WithLabelValues(key).Inc()
			usageTokens.WithLabelValues(key).Add(1500)
			usageInFlight.WithLabelValues(key).Dec()
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

	show(counters.Handler(), "/metrics/usage",
		"usage: all 7 requests, none eaten by the framework scrapes -- and no gauge here")

	show(gauges.Handler(), "/metrics/usage-gauges",
		"gauges: current values only, no counter here")

	show(counters.Handler(), "/metrics/usage",
		"usage again with nothing written in between: empty, because the gauges went to the other endpoint")

	show(cumulative, "/metrics/cumulative",
		"a plain cumulative read, for debugging -- touches no baseline")

	serve("/chat", "key-a", 4)
	show(counters.Handler(), "/metrics/usage",
		"the debugging read above did not eat these 4 either")

	mux := http.NewServeMux()
	mux.Handle("/metrics", framework)
	mux.Handle("/metrics/usage", counters.Handler())
	mux.Handle("/metrics/usage-gauges", gauges.Handler())
	mux.Handle("/metrics/cumulative", cumulative)

	fmt.Println("\nserving on :9090")
	fmt.Println("  /metrics               framework, cumulative        -- the 10s job, scrape config unchanged")
	fmt.Println("  /metrics/usage         counters + histograms, delta -- sum_samples_total")
	fmt.Println("  /metrics/usage-gauges  gauges, current values       -- sum_samples")
	fmt.Println("  /metrics/cumulative    cumulative, debugging only, touches no baseline")
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
