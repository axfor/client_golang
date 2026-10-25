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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// A caller looks an instance up, idle cleanup deletes it, and the caller writes
// to it: that write is usage like any other and must be reported, once, whether
// or not a later lookup has created a new instance with the same labels by then.

type deletedFixture struct {
	t   *testing.T
	exp *TrackedExposer
	got map[string]float64 // everything reported, summed over the scrapes
}

func newDeletedFixture(t *testing.T, reg *prometheus.Registry, trk *prometheus.ChangeTracker) *deletedFixture {
	opts := Increments()
	opts.Scope = reg
	opts.IdleScrapes = 2
	opts.HeartbeatScrapes = 1 // every instance looked at for idleness every scrape
	return &deletedFixture{t: t, exp: NewTracked(trk, opts), got: map[string]float64{}}
}

// scrape reports one scrape into got, and fails if any series is in it twice.
func (f *deletedFixture) scrape() {
	f.t.Helper()
	rec := httptest.NewRecorder()
	f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	seen := map[string]bool{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		series := line[:strings.LastIndexByte(line, ' ')]
		if seen[series] {
			f.t.Fatalf("series %s twice in one scrape:\n%s", series, rec.Body.String())
		}
		seen[series] = true
	}
	for k, v := range parseCounters(f.t, rec.Body.String()) {
		f.got[k] += v
	}
}

// deleteIdle scrapes until idle cleanup has deleted m.
func (f *deletedFixture) deleteIdle(m prometheus.Metric) {
	f.t.Helper()
	for i := 0; i < 10; i++ {
		if prometheus.TrackerSlot(m) == nil {
			return
		}
		f.scrape()
	}
	f.t.Fatalf("idle cleanup did not delete the instance")
}

func trackedForTest(t *testing.T) (*prometheus.ChangeTracker, *prometheus.Registry) {
	trk := prometheus.NewChangeTracker()
	prometheus.SetChangeTracker(trk)
	t.Cleanup(func() { prometheus.SetChangeTracker(nil) })
	return trk, prometheus.NewRegistry()
}

func TestTrackedWriteAfterIdleDeletionIsReported(t *testing.T) {
	trk, reg := trackedForTest(t)
	tokens := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tokens_total", Help: "h"}, []string{"key"})
	reg.MustRegister(tokens)
	f := newDeletedFixture(t, reg, trk)

	held := tokens.WithLabelValues("a")
	held.Add(3)
	f.deleteIdle(held)
	held.Add(5) // looked up before the deletion, written after
	for i := 0; i < 3; i++ {
		f.scrape()
	}
	tokens.WithLabelValues("a").Add(1)
	f.scrape()
	if got := f.got[`tokens{key="a"}`]; got != 9 {
		t.Fatalf("reported %v tokens for key a; want 9, the 3, the 5 written after the deletion, and the 1 after that", got)
	}
}

func TestTrackedWriteAfterIdleDeletionGoesToTheNewInstance(t *testing.T) {
	trk, reg := trackedForTest(t)
	tokens := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tokens_total", Help: "h"}, []string{"key"})
	reg.MustRegister(tokens)
	f := newDeletedFixture(t, reg, trk)

	held := tokens.WithLabelValues("a")
	held.Add(3)
	f.deleteIdle(held)
	tokens.WithLabelValues("a").Add(2) // a later lookup creates a new instance
	held.Add(5)                        // and the one looked up before writes to the old
	for i := 0; i < 3; i++ {
		f.scrape()
	}
	held.Add(4) // and again
	for i := 0; i < 3; i++ {
		f.scrape()
	}
	if got := f.got[`tokens{key="a"}`]; got != 14 {
		t.Fatalf("reported %v tokens for key a; want 14: 3, then 2 and 5, then 4", got)
	}
}

func TestTrackedHistogramObservedAfterIdleDeletionIsReported(t *testing.T) {
	for _, newer := range []bool{false, true} {
		trk, reg := trackedForTest(t)
		lat := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lat", Help: "h", Buckets: []float64{1, 10}}, []string{"key"})
		reg.MustRegister(lat)
		f := newDeletedFixture(t, reg, trk)

		held := lat.WithLabelValues("a").(prometheus.Histogram)
		held.Observe(0.5)
		f.deleteIdle(held)
		if newer {
			lat.WithLabelValues("a").Observe(20)
		}
		held.Observe(5)
		held.Observe(0.25)
		for i := 0; i < 3; i++ {
			f.scrape()
		}
		want := map[string]float64{`lat_count{key="a"}`: 3, `lat_sum{key="a"}`: 5.75, `lat_bucket{key="a",le="1"}`: 2, `lat_bucket{key="a",le="10"}`: 3, `lat_bucket{key="a",le="+Inf"}`: 3}
		if newer {
			want = map[string]float64{`lat_count{key="a"}`: 4, `lat_sum{key="a"}`: 25.75, `lat_bucket{key="a",le="1"}`: 2, `lat_bucket{key="a",le="10"}`: 3, `lat_bucket{key="a",le="+Inf"}`: 4}
		}
		for k, v := range want {
			if f.got[k] != v {
				t.Errorf("newer instance %v: %s reported %v; want %v; all %v", newer, k, f.got[k], v, f.got)
			}
		}
	}
}
