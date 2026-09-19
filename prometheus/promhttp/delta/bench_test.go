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
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// discard counts what a scrape writes without keeping it.
type discard struct {
	h http.Header
	n int64
}

func (d *discard) Header() http.Header {
	if d.h == nil {
		d.h = http.Header{}
	}
	return d.h
}
func (d *discard) Write(p []byte) (int, error) { d.n += int64(len(p)); return len(p), nil }
func (d *discard) WriteHeader(int)             {}

// One scrape of the tracked path, with the share of keys that a minute of real
// traffic touches. Gauges go out every scrape whether or not they were written,
// so the cost of a scrape is set by the registered keys, not the active ones --
// which is why the active share is a parameter here.
func BenchmarkTrackedServe(b *testing.B) {
	for _, keys := range []int{1000, 10000} {
		for _, activePct := range []int{2, 100} {
			b.Run("keys="+strconv.Itoa(keys)+"/active="+strconv.Itoa(activePct)+"%", func(b *testing.B) {
				trk := prometheus.NewChangeTracker()
				prometheus.SetChangeTracker(trk)
				defer prometheus.SetChangeTracker(nil)
				suffix := strconv.Itoa(keys*100 + activePct)
				c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bc" + suffix + "_total", Help: "c"}, []string{"key", "zone"})
				g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "bg" + suffix, Help: "g"}, []string{"key", "zone"})
				h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
					Name: "bh" + suffix, Help: "h", Buckets: []float64{5, 10, 25, 50, 100, 250},
				}, []string{"key", "zone"})

				ids := make([]string, keys)
				for i := range ids {
					ids[i] = "key-" + strconv.Itoa(i)
					c.WithLabelValues(ids[i], "z").Inc()
					g.WithLabelValues(ids[i], "z").Set(float64(i % 7))
					h.WithLabelValues(ids[i], "z").Observe(float64(i % 300))
				}
				exp := NewTracked(trk, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 30, GenLabel: "gen"})
				req := httptest.NewRequest("GET", "/metrics/delta", nil)
				exp.Serve(&discard{}, req) // drain what start-up left pending

				active := keys * activePct / 100
				var w discard
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					for k := range active {
						id := ids[(i*active+k)%keys]
						c.WithLabelValues(id, "z").Inc()
						h.WithLabelValues(id, "z").Observe(float64(k % 300))
					}
					w.n = 0
					exp.Serve(&w, req)
				}
				b.ReportMetric(float64(w.n), "bytes/scrape")
			})
		}
	}
}
