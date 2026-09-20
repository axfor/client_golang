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

package delta

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// The built-in endpoint. Delta exposition is off until Enable is called, and
// while it is off nothing here costs anything: promhttp's handlers ask one
// atomic pointer whether it is on.
//
// Once enabled, the handlers from promhttp.Handler and promhttp.HandlerFor
// answer every scrape with a delta one, so neither the exposing process nor the
// scrape config has anything to mount or set: enabling it here is the whole
// decision, and /metrics is what the scraper already reads.
//
// The consequence is that /metrics consumes increments, and only one scraper may
// read it -- a second one, or somebody with curl, takes increments the first one
// will then never see. Whoever needs the cumulative values instead asks for them
// explicitly:
//
//	/metrics?delta=0
//
// which is the ordinary exposition and consumes nothing, so it stays usable for
// debugging next to a delta scraper.

// QueryParam is the query parameter that selects the exposition. It is not
// needed for a delta scrape once Enable has been called -- that is what the
// handlers do by default -- and "0", "false" or "no" asks for the cumulative
// exposition instead.
const QueryParam = "delta"

type server interface {
	Serve(http.ResponseWriter, *http.Request) ScrapeStats
}

var endpoint atomic.Pointer[server]

// Enable turns on delta exposition for the handlers promhttp hands out. It is
// the single switch for the whole feature: off by default, and one call turns on
// change tracking, the built-in endpoint and, with Options.IdleScrapes, idle
// cleanup, without any further wiring.
//
// Call it before creating any metric. Tracking attaches to instances as a Vec
// creates them, so instances that already exist are invisible to it. When that
// is not possible, set Options.Gatherer and the slower Gatherer-based path is
// used instead, which sees everything in that registry whenever it is enabled.
//
// Only one scraper may use the endpoint: delivery state is global, so a second
// one would eat the first one's increments.
func Enable(opts Options) {
	var s server
	if opts.Gatherer != nil {
		s = New(opts.Gatherer, opts)
	} else {
		s = NewTracked(prometheus.EnableChangeTracking(), opts)
	}
	endpoint.Store(&s)
}

// Disable turns the built-in endpoint off again. Change tracking, once on, stays
// on: instances created while it was on keep their hook.
func Disable() { endpoint.Store(nil) }

// Enabled reports whether the built-in endpoint is on.
func Enabled() bool { return endpoint.Load() != nil }

// ServeIfRequested answers a delta scrape and reports whether it did. The
// handlers in promhttp call it first and carry on with an ordinary scrape when
// it reports false.
func ServeIfRequested(w http.ResponseWriter, r *http.Request) bool {
	s := endpoint.Load()
	if s == nil || !requested(r) {
		return false
	}
	(*s).Serve(w, r)
	return true
}

// requested reports whether this scrape is to be answered with a delta. Once
// the endpoint is on that is every scrape, unless it opts out.
func requested(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return true
	}
	switch r.URL.Query().Get(QueryParam) {
	case "0", "false", "no":
		return false
	}
	return true
}
