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

// Exposing increments instead of cumulative values, to two scrapers at once.
//
// # What this is shaped like
//
// One process measures two quite different things:
//
//   - Framework metrics. A few dozen series, fixed: no per-request labels, so
//     the count does not grow. Watched on a dashboard, so they are wanted every
//     ten seconds.
//   - Usage metrics. One set per API key, so hundreds of thousands of series.
//     They feed billing, so they are wanted complete rather than fresh, and once
//     a minute is enough.
//
// Serving both from one /metrics makes the cheap job expensive: a scraper pulls
// the whole body and only then drops what it does not keep, so the ten-second
// job moves the per-key payload six times a minute to keep its few dozen series.
//
// # Why two endpoints and not one
//
// Increments have one rule: an endpoint has one consumer. What it reports is
// what accrued since it last delivered successfully, and that baseline is one
// baseline. A second scraper on the same endpoint takes increments the first one
// then never sees -- and nothing fails, the numbers are just quietly short.
//
// So each scraper gets its own endpoint with its own exposer, and each exposer
// keeps its own baselines. The rule then holds by construction rather than by
// everyone remembering it.
//
//	/metrics        framework metrics   every 10s   delta.New over its registry
//	/metrics/usage  usage metrics       every 60s   delta.NewTracked over the tracker
//
// # Why the two are built differently
//
// The tracked exposer finds what to report by draining a list of the instances
// written since the last scrape, which is what makes it hold up at a million
// series. That list is shared, so two tracked exposers would drain it from each
// other.
//
// The Gather exposer walks everything every scrape and compares. At a few dozen
// series that costs nothing, and it never touches the dirty list -- so the two
// coexist. The rule is: the big one is tracked, the small ones gather.
//
// # Reading cumulative values
//
// An exposer's own Handler answers every request with a delta -- ?delta=0 is
// read by delta.Enable, which is the other way to wire this up, and an exposer
// mounted by hand never sees it. So mount a plain promhttp handler for anything
// that wants cumulative values. It gathers the registry and touches none of the
// exposer's baselines, so it is safe to curl next to a running scraper.
//
//	go run ./examples/delta
//
//	curl -s localhost:9090/metrics             framework, increments
//	curl -s localhost:9090/metrics/usage       usage, increments
//	curl -s localhost:9090/metrics/cumulative  usage, cumulative, for debugging
//
// # One thing to expect on the usage endpoint
//
// Change tracking is process-wide, so the framework metrics are tracked too and
// the usage endpoint reports them as well. They are a few dozen series and the
// scraper that reads this endpoint drops them, so it costs nothing -- but it is
// what the output below shows.
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

	// Usage metrics: one set per API key.
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

	opts := delta.Options{
		// Report what accrued since the last delivered scrape. Without this the
		// exposition is still change-only, but the values are cumulative.
		ReportIncrements: true,
		// An instance nothing wrote is not reported at all. Gauges are the
		// exception: a current value left out is a gap, not a saving, so every
		// gauge goes out on every scrape.
		DisableHeartbeat: true,
		// Drop an instance that has not changed for this many scrapes, so a key
		// that stops being used stops costing memory. A gauge is only dropped at
		// zero, so an in-flight count is not deleted mid-request.
		IdleScrapes: 30,
		// Stamp the instance's creation time, so a deleted and recreated instance
		// is a new series rather than a counter that appears to go backwards.
		// Aggregate this label away downstream: it is there to keep the
		// arithmetic right on the way in, not to be stored.
		GenLabel: "gen",
	}

	framework := delta.New(frameworkReg, opts)
	usage := delta.NewTracked(trk, opts)

	serve := func(route, key string, n int) {
		for range n {
			routerRequests.WithLabelValues(route).Inc()
			usageInFlight.WithLabelValues(key).Inc()
			usageRequests.WithLabelValues(key).Inc()
			usageTokens.WithLabelValues(key).Add(1500)
			usageInFlight.WithLabelValues(key).Dec()
		}
	}

	// The two endpoints are independent: what one scraper takes is not taken
	// from the other. Below, the framework endpoint is read three times while
	// the usage endpoint is read once -- the way a 10s job and a 60s job
	// interleave -- and the usage scrape still reports every request.
	serve("/chat", "key-a", 3)
	show(framework.Handler(), "/metrics", "framework scrape 1 of 3")

	serve("/chat", "key-a", 2)
	serve("/embed", "key-b", 1)
	show(framework.Handler(), "/metrics", "framework scrape 2 of 3")

	serve("/chat", "key-a", 1)
	show(framework.Handler(), "/metrics", "framework scrape 3 of 3")

	show(usage.Handler(), "/metrics/usage",
		"usage scrape: all 7 requests are here, none eaten by the three framework scrapes")

	show(usage.Handler(), "/metrics/usage",
		"usage scrape with nothing written in between: only the gauges")

	cumulative := promhttp.HandlerFor(usageReg, promhttp.HandlerOpts{})
	show(cumulative, "/metrics/cumulative",
		"a plain cumulative read, for debugging -- touches no baseline")

	serve("/chat", "key-a", 4)
	show(usage.Handler(), "/metrics/usage",
		"the debugging read above did not eat these 4 either")

	mux := http.NewServeMux()
	mux.Handle("/metrics", framework.Handler())
	mux.Handle("/metrics/usage", usage.Handler())
	// A plain cumulative endpoint, for anything that is not a delta scraper.
	mux.Handle("/metrics/cumulative", cumulative)

	fmt.Println("\nserving on :9090")
	fmt.Println("  /metrics                 framework, increments  -- the 10s job")
	fmt.Println("  /metrics/usage           usage, increments      -- the 60s job")
	fmt.Println("  /metrics/cumulative      cumulative, debugging only, touches no baseline")
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
