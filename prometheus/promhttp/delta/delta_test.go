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
	"context"
	"math/rand"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus"
)

type fixture struct {
	reg *prometheus.Registry
	exp *Exposer
	c   *prometheus.CounterVec
	g   *prometheus.GaugeVec
	h   *prometheus.HistogramVec
}

func newFixture(opts Options) *fixture {
	f := &fixture{reg: prometheus.NewRegistry()}
	f.c = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	f.g = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "g", Help: "g"}, []string{"key"})
	f.h = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h", Help: "h", Buckets: []float64{5, 10, 25, 50, 100, 250}}, []string{"key"})
	f.reg.MustRegister(f.c, f.g, f.h)
	if opts.IdleScrapes > 0 && opts.Delete == nil {
		opts.Delete = func(name string, labels prometheus.Labels) {
			switch name {
			case "c_total":
				f.c.Delete(labels)
			case "g":
				f.g.Delete(labels)
			case "h":
				f.h.Delete(labels)
			}
		}
	}
	f.exp = New(f.reg, opts)
	return f
}

// scrape runs one scrape and returns series -> value, with histogram buckets
// expanded as name_bucket{...,le="x"}. With fail set, the request is cancelled
// before anything is written, which stands in for a scrape that never arrives.
func (f *fixture) scrape(t *testing.T, fail bool) (map[string]float64, ScrapeStats) {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics/delta", nil)
	if fail {
		ctx, cancel := context.WithCancel(req.Context())
		cancel()
		req = req.WithContext(ctx)
	}
	rec := httptest.NewRecorder()
	st := f.exp.Serve(rec, req)

	out := map[string]float64{}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := parser.TextToMetricFamilies(strings.NewReader(rec.Body.String()))
	if err != nil {
		t.Fatalf("cannot parse response: %s\n%s", err, rec.Body.String())
	}
	for name, mf := range mfs {
		for _, m := range mf.Metric {
			var lbl []string
			for _, l := range m.Label {
				lbl = append(lbl, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(lbl)
			base := name + "{" + strings.Join(lbl, ",") + "}"
			switch {
			case m.Counter != nil:
				out[base] = m.Counter.GetValue()
			case m.Gauge != nil:
				out[base] = m.Gauge.GetValue()
			case m.Histogram != nil:
				h := m.Histogram
				out[name+"_sum{"+strings.Join(lbl, ",")+"}"] = h.GetSampleSum()
				out[name+"_count{"+strings.Join(lbl, ",")+"}"] = float64(h.GetSampleCount())
				for _, b := range h.Bucket {
					out[name+"_bucket{"+strings.Join(lbl, ",")+",le="+strconv.FormatFloat(b.GetUpperBound(), 'g', -1, 64)+"}"] = float64(b.GetCumulativeCount())
				}
			}
		}
	}
	return out, st
}

func countPrefix(m map[string]float64, prefix string) int {
	n := 0
	for k := range m {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

func sumPrefix(m map[string]float64, prefix string) float64 {
	var s float64
	for k, v := range m {
		if strings.HasPrefix(k, prefix) {
			s += v
		}
	}
	return s
}

// Delta mode reports what accrued since the last delivered scrape, leaves out
// what was not written, and always reports the current value of a gauge.
func TestReportIncrements(t *testing.T) {
	f := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true})
	f.c.WithLabelValues("a").Add(5)
	f.c.WithLabelValues("a").Add(0.5)
	f.h.WithLabelValues("a").Observe(7)
	f.h.WithLabelValues("a").Observe(7)
	f.h.WithLabelValues("a").Observe(7)
	f.g.WithLabelValues("a").Set(9)

	m, st := f.scrape(t, false)
	if !st.Delivered {
		t.Fatalf("should have been delivered: %+v", st)
	}
	for k, want := range map[string]float64{
		`c_total{key=a}`:          5.5,
		`h_count{key=a}`:          3,
		`h_sum{key=a}`:            21,
		`h_bucket{key=a,le=10}`:   3,
		`h_bucket{key=a,le=+Inf}`: 3,
		`g{key=a}`:                9,
	} {
		if m[k] != want {
			t.Fatalf("scrape 1: %s should be %v, got %v: %v", k, want, m[k], m)
		}
	}

	f.c.WithLabelValues("a").Add(2)
	f.h.WithLabelValues("a").Observe(30)
	m, _ = f.scrape(t, false)
	if m[`c_total{key=a}`] != 2 || m[`h_count{key=a}`] != 1 || m[`h_sum{key=a}`] != 30 || m[`h_bucket{key=a,le=25}`] != 0 {
		t.Fatalf("scrape 2 should report increments only: %v", m)
	}

	m, _ = f.scrape(t, false)
	if countPrefix(m, "c_total{") != 0 || countPrefix(m, "h_") != 0 || m[`g{key=a}`] != 9 {
		t.Fatalf("with no writes, counters and histograms are left out and the gauge is not: %v", m)
	}
}

// An undelivered scrape keeps its increments for the next one, which reports them
// together with whatever accrued since.
func TestReportIncrementsUndelivered(t *testing.T) {
	f := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true})
	f.c.WithLabelValues("a").Add(5)
	f.h.WithLabelValues("a").Observe(7)
	if _, st := f.scrape(t, true); st.Delivered {
		t.Fatalf("should not have been delivered")
	}
	f.c.WithLabelValues("a").Add(1)
	m, _ := f.scrape(t, false)
	if m[`c_total{key=a}`] != 6 || m[`h_count{key=a}`] != 1 {
		t.Fatalf("the undelivered 5 should roll into the next scrape: %v", m)
	}
}

// Concurrent writes interleaved with scrapes, some of which are not delivered:
// the increments that went out have to sum to exactly the number of writes.
//
// Idle deletion is off here. It is not coordinated with concurrent writes, so a
// write landing on a child as it is removed is lost (see Options.IdleScrapes)
// and conservation is not a property this can assert. TestIdleDeletion covers
// deletion itself.
func TestReportIncrementsConserved(t *testing.T) {
	f := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true})
	const writers, perWriter, keys = 8, 20000, 40
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < perWriter; i++ {
				// the active key set slides: old keys go idle and are deleted, then come
				// round again and are written to once more
				k := strconv.Itoa((i/2000 + rnd.Intn(4)) % keys)
				f.c.WithLabelValues(k).Inc()
				f.h.WithLabelValues(k).Observe(float64(rnd.Intn(300)))
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	rnd := rand.New(rand.NewSource(99))
	var counter, observations float64
	var scrapes, failed, deleted int
	collect := func(fail bool) {
		m, st := f.scrape(t, fail)
		scrapes++
		deleted += st.Deleted
		if !st.Delivered {
			failed++
			return
		}
		counter += sumPrefix(m, "c_total{")
		observations += sumPrefix(m, "h_count{")
	}
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
			collect(rnd.Intn(3) == 0)
		}
	}
	during := scrapes
	for i := 0; i < 20; i++ {
		collect(false)
	}
	if during < 20 || failed == 0 {
		t.Fatalf("not interleaved enough: %d scrapes while writing, %d not delivered, %d dropped", during, failed, deleted)
	}
	t.Logf("%d scrapes while writing, %d not delivered, %d dropped", during, failed, deleted)
	if want := float64(writers * perWriter); counter != want || observations != want {
		t.Fatalf("delivered increments should sum to the %v writes: counter %v, histogram observations %v", want, counter, observations)
	}
}

// Change-only mode without deltas: an unchanged series is topped up every
// HeartbeatScrapes scrapes, carrying its cumulative value.
func TestChangeOnlyWithHeartbeat(t *testing.T) {
	f := newFixture(Options{HeartbeatScrapes: 2})
	f.c.WithLabelValues("a").Add(4)
	if m, _ := f.scrape(t, false); m[`c_total{key=a}`] != 4 {
		t.Fatalf("scrape 1 should report the cumulative 4: %v", m)
	}
	seen := 0
	for i := 0; i < 4; i++ {
		m, st := f.scrape(t, false)
		if _, ok := m[`c_total{key=a}`]; ok {
			seen++
			if m[`c_total{key=a}`] != 4 {
				t.Fatalf("a heartbeat top-up carries the cumulative 4: %v", m)
			}
			if st.Heartbeat == 0 {
				t.Fatalf("this scrape should count as a heartbeat top-up: %+v", st)
			}
		}
	}
	if seen == 0 || seen == 4 {
		t.Fatalf("an unchanged series should be topped up every 2 scrapes, got %d in 4", seen)
	}
}

// An idle series leaves the state table, and the callback removes the child from
// its Vec as well.
func TestIdleDeletion(t *testing.T) {
	var deleted []string
	f := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2})
	f.exp.opts.Delete = func(name string, labels prometheus.Labels) {
		deleted = append(deleted, name+"/"+labels["key"])
		if name == "c_total" {
			f.c.Delete(labels)
		}
	}
	f.c.WithLabelValues("a").Inc()
	f.scrape(t, false)
	for i := 0; i < 4; i++ {
		f.scrape(t, false)
	}
	if len(deleted) == 0 {
		t.Fatalf("an idle series should be dropped")
	}
	f.c.WithLabelValues("a").Inc()
	m, _ := f.scrape(t, false)
	if m[`c_total{key=a}`] != 1 {
		t.Fatalf("recreated after deletion it starts from zero and reports 1: %v", m)
	}
}

func TestNegotiateEncoding(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"gzip", "gzip"},
		{"zstd, gzip", "zstd"},
		{"gzip, zstd", "zstd"},
		{"zstd;q=0, gzip", "gzip"},
		{"identity", ""},
		{"", ""},
		{"gzipx", ""},
	} {
		if got := negotiateEncoding(c.in); got != c.want {
			t.Fatalf("Accept-Encoding %q: got %q, want %q", c.in, got, c.want)
		}
	}
}

// Once Enable has been called the handlers answer with a delta by default, so
// that turning this on is a decision the exposing process makes alone and no
// scrape config has to change. Asking for "0", "false" or "no" gets the
// cumulative exposition back.
func TestDeltaIsTheDefaultOnceEnabled(t *testing.T) {
	for _, tc := range []struct {
		target string
		want   bool
	}{
		{"/metrics", true},
		{"/metrics?delta=1", true},
		{"/metrics?delta=true", true},
		{"/metrics?delta=yes", true},
		{"/metrics?other=0", true},
		{"/metrics?delta=", true},
		{"/metrics?delta=0", false},
		{"/metrics?delta=false", false},
		{"/metrics?delta=no", false},
	} {
		if got := requested(httptest.NewRequest("GET", tc.target, nil)); got != tc.want {
			t.Errorf("%s: delta requested = %v, want %v", tc.target, got, tc.want)
		}
	}
	// A scrape that arrives without a parsed URL is still a delta scrape: the
	// default cannot depend on being able to read the query.
	if !requested(nil) {
		t.Error("a request with no URL should still take the default")
	}
}
