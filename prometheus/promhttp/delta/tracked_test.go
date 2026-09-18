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

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus"
)

type trackedFixture struct {
	exp *TrackedExposer
	trk *prometheus.ChangeTracker
	c   *prometheus.CounterVec
	g   *prometheus.GaugeVec
	h   *prometheus.HistogramVec
}

func newTrackedFixture(t *testing.T, opts Options) *trackedFixture {
	// One tracker per test, so tests cannot take each other's dirty instances.
	// Children are created lazily, so it has to stay set for the whole test and
	// cannot be cleared once the Vecs exist.
	f := &trackedFixture{trk: prometheus.NewChangeTracker()}
	prometheus.SetChangeTracker(f.trk)
	t.Cleanup(func() { prometheus.SetChangeTracker(nil) })
	suffix := strconv.Itoa(rand.Int())
	f.c = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c" + suffix + "_total", Help: "c"}, []string{"key"})
	f.g = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "g" + suffix, Help: "g"}, []string{"key"})
	f.h = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h" + suffix, Help: "h", Buckets: []float64{5, 10, 25, 50, 100, 250}}, []string{"key"})
	f.exp = NewTracked(f.trk, opts)
	return f
}

func (f *trackedFixture) scrape(t *testing.T, fail bool) (map[string]float64, ScrapeStats) {
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
		short := name
		if i := strings.IndexAny(name, "0123456789"); i > 0 {
			short = name[:i] + name[strings.LastIndexAny(name, "0123456789")+1:]
		}
		for _, m := range mf.Metric {
			var lbl []string
			for _, l := range m.Label {
				lbl = append(lbl, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(lbl)
			key := short + "{" + strings.Join(lbl, ",") + "}"
			switch {
			case m.Counter != nil:
				out[key] = m.Counter.GetValue()
			case m.Gauge != nil:
				out[key] = m.Gauge.GetValue()
			case m.Histogram != nil:
				out[short+"_sum{"+strings.Join(lbl, ",")+"}"] = m.Histogram.GetSampleSum()
				out[short+"_count{"+strings.Join(lbl, ",")+"}"] = float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return out, st
}

// Delta mode reports only the instances written this scrape, carrying what
// accrued since the last delivered one.
func TestTrackedReportIncrements(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	f.c.WithLabelValues("a").Add(5)
	f.h.WithLabelValues("a").Observe(7)
	f.h.WithLabelValues("a").Observe(7)
	f.g.WithLabelValues("a").Set(9)

	m, st := f.scrape(t, false)
	if m[`c_total{key=a}`] != 5 || m[`h_count{key=a}`] != 2 || m[`h_sum{key=a}`] != 14 || m[`g{key=a}`] != 9 {
		t.Fatalf("scrape 1 should report the values written: %v (%+v)", m, st)
	}

	f.c.WithLabelValues("a").Add(2)
	m, _ = f.scrape(t, false)
	if m[`c_total{key=a}`] != 2 || len(m) != 1 {
		t.Fatalf("scrape 2 wrote only the counter and should report its increment of 2: %v", m)
	}

	m, st = f.scrape(t, false)
	if len(m) != 0 || st.Samples != 0 {
		t.Fatalf("a scrape with no writes should report nothing: %v (%+v)", m, st)
	}
}

// An undelivered scrape keeps its increments for the next one.
func TestTrackedUndelivered(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	f.c.WithLabelValues("a").Add(5)
	if _, st := f.scrape(t, true); st.Delivered {
		t.Fatalf("should not have been delivered")
	}
	f.c.WithLabelValues("a").Add(1)
	m, _ := f.scrape(t, false)
	if m[`c_total{key=a}`] != 6 {
		t.Fatalf("the undelivered 5 should roll into the next scrape: %v", m)
	}
}

// Concurrent writes interleaved with scrapes, some of which are not delivered:
// the increments that went out sum to exactly the number of writes.
//
// Idle deletion is off here: it is not coordinated with concurrent writes, so a
// write landing on a child as it is removed is lost (see Options.IdleScrapes).
// TestTrackedIdleDeletion covers deletion itself.
func TestTrackedConserved(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, HeartbeatScrapes: 2})
	const writers, perWriter, keys = 8, 20000, 40
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < perWriter; i++ {
				k := strconv.Itoa((i/2000 + rnd.Intn(4)) % keys)
				f.c.WithLabelValues(k).Inc()
				f.h.WithLabelValues(k).Observe(float64(rnd.Intn(300)))
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	rnd := rand.New(rand.NewSource(7))
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
		t.Fatalf("not interleaved enough: %d scrapes while writing, %d not delivered", during, failed)
	}
	t.Logf("%d scrapes while writing, %d not delivered, %d dropped", during, failed, deleted)
	if want := float64(writers * perWriter); counter != want || observations != want {
		t.Fatalf("delivered increments should sum to the %v writes: counter %v, histogram observations %v", want, counter, observations)
	}
}

// A scrape touches only what was written: with 1000 keys registered and 3 written,
// the output has exactly those 3.
func TestTrackedOnlyTouchesChanged(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	for i := 0; i < 1000; i++ {
		f.c.WithLabelValues(strconv.Itoa(i)).Inc()
	}
	if _, st := f.scrape(t, false); st.Samples != 1000 {
		t.Fatalf("scrape 1 should report all 1000 new instances: %+v", st)
	}
	for _, k := range []string{"3", "17", "999"} {
		f.c.WithLabelValues(k).Inc()
	}
	m, st := f.scrape(t, false)
	if st.Samples != 3 || len(m) != 3 {
		t.Fatalf("scrape 2 wrote 3 keys and should report 3 series: %d, %v", st.Samples, m)
	}
}

// An idle instance is deleted, and writing to it again starts from zero, so what
// is reported is the increment since it was recreated. No Delete callback is set
// here: with none, the instance is removed from its Vec by DeleteTracked.
func TestTrackedIdleDeletion(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	f.c.WithLabelValues("a").Add(3)
	if m, _ := f.scrape(t, false); m[`c_total{key=a}`] != 3 {
		t.Fatalf("scrape 1 should report 3: %v", m)
	}
	deleted := 0
	for i := 0; i < 5; i++ {
		_, st := f.scrape(t, false)
		deleted += st.Deleted
	}
	if deleted == 0 {
		t.Fatalf("an instance idle for more than 2 scrapes should be deleted")
	}
	if n := children(f.c); n != 0 {
		t.Fatalf("the child should be gone from the Vec, %d left", n)
	}

	// Recreated from zero: what is reported is 4 and not the 7 a surviving child
	// would hold.
	f.c.WithLabelValues("a").Add(4)
	m, _ := f.scrape(t, false)
	if m[`c_total{key=a}`] != 4 {
		t.Fatalf("recreated after deletion it starts from zero and reports 4: %v", m)
	}
	if v := collectOne(t, f.c); v != 4 {
		t.Fatalf("the recreated child should hold 4, not %v", v)
	}
}

func children(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 1024)
	c.Collect(ch)
	close(ch)
	return len(ch)
}

func collectOne(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1024)
	c.Collect(ch)
	close(ch)
	var out dto.Metric
	for m := range ch {
		if err := m.Write(&out); err != nil {
			t.Fatal(err)
		}
		return out.GetCounter().GetValue()
	}
	t.Fatal("no child collected")
	return 0
}

// Options.Delete, when set, is used instead of DeleteTracked and is told which
// series went idle.
func TestTrackedIdleDeletionCallback(t *testing.T) {
	var got []string
	f := newTrackedFixture(t, Options{
		ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1,
		Delete: func(name string, labels prometheus.Labels) {
			got = append(got, name+"/"+labels["key"])
		},
	})
	f.c.WithLabelValues("a").Add(3)
	for i := 0; i < 6; i++ {
		f.scrape(t, false)
	}
	if len(got) == 0 {
		t.Fatalf("the Delete callback should have been called for the idle series")
	}
	for _, g := range got {
		if !strings.HasSuffix(g, "/a") {
			t.Fatalf("callback got an unexpected series: %v", got)
		}
	}
}
