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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"

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

// Delta mode reports the counters and histograms written this scrape, carrying
// what accrued since the last delivered one. Gauges ride along every scrape,
// written or not, because a current value cannot be accumulated.
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
	if m[`c_total{key=a}`] != 2 || m[`g{key=a}`] != 9 || len(m) != 2 {
		t.Fatalf("scrape 2 wrote only the counter: want its increment of 2 plus the gauge, got %v", m)
	}

	m, st = f.scrape(t, false)
	if len(m) != 1 || m[`g{key=a}`] != 9 || st.Samples != 1 {
		t.Fatalf("a scrape with no writes should report the gauge and nothing else: %v (%+v)", m, st)
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

// A gauge is a current value, not something that accumulates, so a scrape that
// left it out would leave the consumer with a gap rather than a saving. Every
// gauge is reported every scrape, whether or not it was written and whether or
// not heartbeats are on. Counters and histograms stay change-only.
func TestGaugesAreAlwaysReported(t *testing.T) {
	f := newTrackedFixture(t, Options{DisableHeartbeat: true, ReportIncrements: true})
	for _, key := range []string{"a", "b", "c"} {
		f.c.WithLabelValues(key).Inc()
		f.g.WithLabelValues(key).Set(7)
	}
	if got, _ := f.scrape(t, false); len(got) != 6 {
		t.Fatalf("first scrape: got %d series, want 6: %v", len(got), got)
	}

	// Only "a" moves. Its counter increment is reported; b and c contribute no
	// counter at all, but all three gauges have to be there.
	f.c.WithLabelValues("a").Inc()
	f.g.WithLabelValues("a").Set(9)
	got, st := f.scrape(t, false)
	want := map[string]float64{"a": 9, "b": 7, "c": 7}
	for key, w := range want {
		if v, ok := got["g{key="+key+"}"]; !ok || v != w {
			t.Errorf("gauge for key %q: got %v (present %v), want %v", key, v, ok, w)
		}
	}
	if _, ok := got["c_total{key=b}"]; ok {
		t.Errorf("unchanged counter for key b was reported: %v", got)
	}
	if st.Gauges != 2 {
		t.Errorf("Gauges = %d, want 2 (b and c topped up; a came off the dirty list)", st.Gauges)
	}
}

// A gauge that is idle at zero is still deleted, and is not reported one last
// time on the way out.
func TestIdleGaugeIsNotToppedUpWhileBeingDropped(t *testing.T) {
	f := newTrackedFixture(t, Options{DisableHeartbeat: true, ReportIncrements: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	f.g.WithLabelValues("a").Set(0)
	f.g.WithLabelValues("b").Set(5)
	f.scrape(t, false)

	var deleted int
	var last map[string]float64
	for range 4 {
		var st ScrapeStats
		last, st = f.scrape(t, false)
		deleted += st.Deleted
		if st.Deleted > 0 {
			if _, ok := last["g{key=a}"]; ok {
				t.Errorf("the dropped gauge was reported on the scrape that dropped it: %v", last)
			}
		}
	}
	if deleted == 0 {
		t.Fatalf("the idle zero gauge was never dropped")
	}
	if _, ok := last["g{key=a}"]; ok {
		t.Errorf("the dropped gauge came back after deletion: %v", last)
	}
	if v, ok := last["g{key=b}"]; !ok || v != 5 {
		t.Errorf("the non-zero gauge should survive and keep being reported: got %v (present %v)", v, ok)
	}
}

// What a long scrape outage does to a series that was written once and then
// went quiet. The round counter advances on every attempt, delivered or not,
// and idle cleanup runs whether or not the response got out, so a series can
// age out while the increment it is holding has never been delivered.
//
// This is not about the outage being survivable -- an outage longer than
// IdleScrapes is already outside what the design promises -- but about which
// way it fails: silently losing the increment, or keeping it.
func TestOutageLongerThanIdleScrapes(t *testing.T) {
	f := newTrackedFixture(t, Options{
		ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 3, HeartbeatScrapes: 1,
	})
	f.c.WithLabelValues("a").Add(5)

	// Every scrape fails, for longer than the idle threshold.
	var deleted int
	for range 6 {
		_, st := f.scrape(t, true)
		if st.Delivered {
			t.Fatal("the scrape should not have been delivered")
		}
		deleted += st.Deleted
	}

	got, _ := f.scrape(t, false)
	t.Logf("dropped during the outage: %d; first delivered scrape after it: %v", deleted, got)
	if got[`c_total{key=a}`] != 5 {
		t.Errorf("the 5 written before the outage was lost: got %v, want c_total{key=a}=5", got)
	}
}

// A dto is needed only while an instance is being written: every instance goes
// through the one scratch, and a family's values are kept only while that family
// is being encoded. Keeping a dto per instance written was a quarter of the heap
// of a sidecar at 1.6M instances, first per instance and then on free lists
// sized to the most a scrape ever wrote, so nothing sized by that may outlive
// the scrape beyond arrays that fall back.
func TestNoDTOIsKeptBetweenScrapes(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	const keys = 200
	for i := range keys {
		f.c.WithLabelValues(strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false) // every counter is new, so every one is written

	held := func() (rows, nums int) {
		f.exp.mu.Lock()
		defer f.exp.mu.Unlock()
		if len(f.exp.fam.rows) != 0 || len(f.exp.fam.nums) != 0 || f.exp.fam.mf != nil {
			t.Fatalf("a family is still held after the scrape: %d rows, %d values", len(f.exp.fam.rows), len(f.exp.fam.nums))
		}
		return cap(f.exp.fam.rows), cap(f.exp.fam.nums)
	}
	beforeRows, beforeNums := held()
	if beforeRows < keys {
		t.Fatalf("the family arrays hold room for %d rows after a scrape that wrote %d; expected the room to be kept for now", beforeRows, keys)
	}

	// From here on only two counters move. The room has to fall back towards
	// that: the first scrape after start-up writes every instance, and arrays
	// left at that size would be the per-instance cost this is meant to avoid.
	for range 40 {
		f.c.WithLabelValues("0").Inc()
		f.c.WithLabelValues("1").Inc()
		f.scrape(t, false)
	}
	afterRows, afterNums := held()
	// Down to the floor below which arrays are not worth shrinking.
	if afterRows > minShrink || afterNums > minShrink {
		t.Errorf("after 40 scrapes writing two instances the family arrays still hold room for %d rows and %d values, down from %d and %d",
			afterRows, afterNums, beforeRows, beforeNums)
	}

	// And the values still come out right through the shared scratch.
	f.c.WithLabelValues("7").Add(3)
	m, _ := f.scrape(t, false)
	if m[`c_total{key=7}`] != 3 {
		t.Errorf("the wrong increment came out: %v", m[`c_total{key=7}`])
	}
}

// Stamping the generation on the dto and keeping rebase bases are things only
// some configurations do, so those fields live in a struct allocated when one of
// them first needs it. A configuration that needs neither must not end up with
// one per instance: that is 96 bytes on every series, which is what moving them
// out of the entry was for.
func TestNoSideStructWhenNothingNeedsOne(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	for i := range 50 {
		f.c.WithLabelValues(strconv.Itoa(i)).Inc()
		f.h.WithLabelValues(strconv.Itoa(i)).Observe(1)
	}
	for range 3 {
		f.scrape(t, false)
	}

	var with, total int
	for i := range 50 {
		for _, m := range []any{f.c.WithLabelValues(strconv.Itoa(i)), f.h.WithLabelValues(strconv.Itoa(i))} {
			if en := entryFor(t, m); en != nil {
				total++
				if en.extra != nil {
					with++
				}
			}
		}
	}
	if total == 0 {
		t.Fatal("no entries to check")
	}
	if with != 0 {
		t.Errorf("%d of %d entries carry a side struct; reporting increments needs none of what it holds", with, total)
	}
}

// And the configuration that does need one gets it, once it does: a generation
// only exists after a gap has moved it, so configuring RebaseAfterGap is not
// itself enough.
func TestSideStructAppearsOnceTheGenerationMoves(t *testing.T) {
	f := newTrackedFixture(t, Options{
		DisableHeartbeat: true, IdleScrapes: 30, GenLabel: "gen",
		RebaseAfterGap: 30 * time.Minute, HeartbeatScrapes: 1,
	})
	now := time.Unix(1000, 0)
	f.exp.rb.now = func() time.Time { return now }

	f.c.WithLabelValues("a").Add(60)
	f.scrape(t, false)
	if en := entryFor(t, f.c.WithLabelValues("a")); en == nil || en.extra != nil {
		t.Fatal("no gap has happened yet, so there is no generation to keep a base for")
	}

	now = time.Unix(1000+46*60, 0) // longer than RebaseAfterGap
	f.c.WithLabelValues("a").Add(30)
	if _, st := f.scrape(t, false); !st.Rebased {
		t.Fatalf("46 minutes since the last delivery: the generation should have moved (%+v)", st)
	}
	if en := entryFor(t, f.c.WithLabelValues("a")); en == nil || en.extra == nil {
		t.Error("the generation moved and the entry has nowhere to keep its base")
	}
}

// Two endpoints over the same metrics, split by what a consumer does with them:
// one carries what it adds up, the other what it reads as it stands. An
// aggregator then picks the arithmetic by which endpoint it scraped, instead of
// being handed a list of metric names that has to be kept up to date.
func TestKindSplitsTheExposition(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, Only: Accumulating})
	// The gauge endpoint gathers rather than tracks: the dirty list is one list,
	// and a second tracked exposer would drain it from the first. See
	// TestTwoTrackedExposersDrainEachOther.
	reg := prometheus.NewRegistry()
	reg.MustRegister(f.c, f.g, f.h)
	gauges := New(reg, Options{DisableHeartbeat: true, Only: Current})

	f.c.WithLabelValues("a").Add(5)
	f.h.WithLabelValues("a").Observe(1)
	f.g.WithLabelValues("a").Set(7)

	acc, _ := f.scrape(t, false)
	for name := range acc {
		if strings.HasPrefix(name, "g{") {
			t.Errorf("the accumulating endpoint carried a gauge: %v", acc)
		}
	}
	if acc[`c_total{key=a}`] != 5 {
		t.Errorf("counter = %v, want 5: %v", acc[`c_total{key=a}`], acc)
	}

	req := httptest.NewRequest("GET", "/metrics/gauges", nil)
	rec := httptest.NewRecorder()
	gauges.Serve(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, "g") || !strings.Contains(body, "7") {
		t.Errorf("the current-value endpoint should carry the gauge:\n%s", body)
	}
	for _, name := range []string{"c_total", "h_sum", "h_count"} {
		if strings.Contains(body, name) {
			t.Errorf("the current-value endpoint carried %s:\n%s", name, body)
		}
	}
}

// A metric added later lands on the right endpoint by its own type, which is
// the point: no list of names anywhere has to be updated.
func TestAMetricAddedLaterLandsOnTheRightEndpoint(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, Only: Accumulating})
	reg := prometheus.NewRegistry()
	reg.MustRegister(f.c, f.g, f.h)
	gauges := New(reg, Options{DisableHeartbeat: true, Only: Current})
	f.c.WithLabelValues("a").Inc()
	f.scrape(t, false)
	gauges.Serve(httptest.NewRecorder(), httptest.NewRequest("GET", "/g", nil))

	// Two new metrics, one of each kind, created after both endpoints exist.
	suffix := strconv.Itoa(rand.Int())
	late := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "late" + suffix + "_total", Help: "c"}, []string{"key"})
	lateG := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lateg" + suffix, Help: "g"}, []string{"key"})
	reg.MustRegister(late, lateG)
	late.WithLabelValues("a").Add(3)
	lateG.WithLabelValues("a").Set(9)

	acc, _ := f.scrape(t, false)
	accHas := false
	for name := range acc {
		if strings.HasPrefix(name, "late_total{") {
			accHas = true
		}
		if strings.HasPrefix(name, "lateg{") {
			t.Errorf("the new gauge landed on the accumulating endpoint: %v", acc)
		}
	}
	if !accHas {
		t.Errorf("the new counter did not land on the accumulating endpoint: %v", acc)
	}

	rec := httptest.NewRecorder()
	gauges.Serve(rec, httptest.NewRequest("GET", "/g", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "lateg"+suffix) {
		t.Errorf("the new gauge did not land on the current-value endpoint:\n%s", body)
	}
	if strings.Contains(body, "late"+suffix+"_total") {
		t.Errorf("the new counter landed on the current-value endpoint:\n%s", body)
	}
}

// Why the gauge endpoint gathers instead of tracking: the dirty list is one
// list, and whichever tracked exposer scrapes first takes it. This is the
// constraint behind "the big one is tracked, the small ones gather".
func TestTwoTrackedExposersDrainEachOther(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	second := NewTracked(f.trk, Options{ReportIncrements: true, DisableHeartbeat: true})

	f.c.WithLabelValues("a").Add(5)
	if got, _ := f.scrape(t, false); got[`c_total{key=a}`] != 5 {
		t.Fatalf("the first exposer should report the 5: %v", got)
	}

	rec := httptest.NewRecorder()
	st := second.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	if st.Samples != 0 {
		t.Errorf("the second exposer saw %d samples; the first one already took them", st.Samples)
	}
}

// TypeLabel carries the metric's type on every sample, so an aggregator selects
// what to do by what the metric is rather than by a list of names somebody
// maintains. It goes where it sorts, like every label this package adds.
func TestTypeLabel(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, TypeLabel: "_metric_type"})
	f.c.WithLabelValues("a").Inc()
	f.g.WithLabelValues("a").Set(3)
	f.h.WithLabelValues("a").Observe(1)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	f.exp.Serve(rec, req)
	body := rec.Body.String()

	want := map[string]string{"c": "counter", "g": "gauge", "h": "histogram"}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		open := strings.Index(line, "{")
		if open < 0 {
			t.Errorf("no labels at all on %q", line)
			continue
		}
		kind := want[line[:1]]
		if kind == "" {
			t.Errorf("unexpected series %q", line)
			continue
		}
		if !strings.Contains(line, `_metric_type="`+kind+`"`) {
			t.Errorf("%q should carry _metric_type=%q", line, kind)
		}
		// Sorted: the metric's own label is "key", and "_" sorts before "k".
		if i := strings.Index(line, "_metric_type="); i > strings.Index(line, "key=") {
			t.Errorf("the type label is out of sorted order: %q", line)
		}
	}
}

// The type label and the generation coexist, both in their sorted places. The
// generation is for the scheme that reports cumulative values, so that is the
// one this uses.
func TestTypeLabelAlongsideGeneration(t *testing.T) {
	// The type label is added first and the generation second, so a type label
	// that sorts after the generation is only placed right if the two are sorted
	// before being written. Both names are the caller's to choose.
	f := newTrackedFixture(t, Options{
		DisableHeartbeat: true, IdleScrapes: 30, GenLabel: "agen", TypeLabel: "ztype",
	})
	f.c.WithLabelValues("a").Inc()

	rec := httptest.NewRecorder()
	f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	var line string
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "c") && !strings.HasPrefix(l, "#") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no counter in:\n%s", body)
	}
	if !strings.Contains(line, `ztype="counter"`) || !strings.Contains(line, `agen="`) {
		t.Fatalf("both labels should be there: %q", line)
	}
	// "agen" < "key" < "ztype", while the type is added before the generation:
	// this only holds if the two are sorted before being placed.
	a := strings.Index(line, "agen=")
	b := strings.Index(line, "key=")
	c := strings.Index(line, "ztype=")
	if !(a < b && b < c) {
		t.Errorf("labels out of sorted order: %q", line)
	}
}

// Tracked has to follow what the exposer is actually holding. A count taken once
// and cached reads the same forever, which looks right on every dashboard and
// hides exactly the thing it is there to show: whether idle cleanup is running.
func TestTrackedCountFollowsIdleDeletion(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	if n := f.exp.Tracked(); n != 0 {
		t.Fatalf("a fresh exposer holds %d instances, want 0", n)
	}

	for _, k := range []string{"a", "b", "c"} {
		f.c.WithLabelValues(k).Add(1)
	}
	f.scrape(t, false)
	if n := f.exp.Tracked(); n != 3 {
		t.Fatalf("after three instances were reported: %d, want 3", n)
	}

	// Keep writing to one of them, so the other two age out and it does not.
	for i := 0; i < 5; i++ {
		f.c.WithLabelValues("a").Add(1)
		f.scrape(t, false)
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("after two of three aged out: %d, want 1", n)
	}
}

// The scrape scratch has to let go of what it pointed at. Truncating a slice
// leaves its backing array intact, and the first scrape after start-up sizes
// these to every instance in the process: idle cleanup then drops an entry from
// state and from its Vec while a stale pointer in the scratch keeps it, its
// labels, its dto and the instance itself on the heap for good. Nothing fails
// when that happens -- the numbers stay right, the process just never gives the
// memory back -- so it is asserted rather than measured.
func TestScrapeScratchHoldsNothingAfterTheScrape(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	// A wide first scrape, the way a process looks after warm-up: every
	// instance is new, so every one is written and the scratch grows to fit.
	for i := range 200 {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)

	// Then let all but one age out, so the scratch is far larger than the live
	// set and any pointer left past the length is to something already dropped.
	for range 5 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("%d instances still tracked, want 1", n)
	}

	f.exp.mu.Lock()
	defer f.exp.mu.Unlock()
	for i, s := range f.exp.slots {
		full := s.picks[:cap(s.picks)]
		for j := len(s.picks); j < len(full); j++ {
			if full[j].m != nil || full[j].en != nil {
				t.Fatalf("slots[%d] still points at an instance at index %d, past its length %d (cap %d)",
					i, j, len(s.picks), cap(s.picks))
			}
		}
	}
	rows := f.exp.fam.rows[:cap(f.exp.fam.rows)]
	for j := len(f.exp.fam.rows); j < len(rows); j++ {
		if rows[j].en != nil || rows[j].labels != nil {
			t.Fatalf("the family rows still point at an entry at index %d, past their length %d (cap %d)",
				j, len(f.exp.fam.rows), cap(f.exp.fam.rows))
		}
	}
	full := f.exp.taken[:cap(f.exp.taken)]
	for j := len(f.exp.taken); j < len(full); j++ {
		if full[j] != nil {
			t.Fatalf("taken still holds a metric at index %d, past its length %d (cap %d)",
				j, len(f.exp.taken), cap(f.exp.taken))
		}
	}
}

// entryFor returns the entry the exposer keeps for m, a child of one of the
// fixture's Vecs, or nil when it has none.
func entryFor(t *testing.T, m any) *entry {
	t.Helper()
	pm, ok := m.(prometheus.Metric)
	if !ok {
		t.Fatalf("%T is not a prometheus.Metric", m)
	}
	en, _ := entryOf(pm)
	return en
}

// An entry is kept in its instance's tracker slot, so it has to go when the
// instance goes -- dropped as idle, or deleted from its Vec by the application,
// which the map from instance to entry this replaced kept for good -- and
// nothing else may hold it once it has.
func TestEntryGoesWithItsInstance(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	for _, k := range []string{"keep", "idle", "deleted"} {
		f.c.WithLabelValues(k).Inc()
	}
	f.scrape(t, false)
	weakOf := func(k string) weak.Pointer[entry] {
		en := entryFor(t, f.c.WithLabelValues(k))
		if en == nil {
			t.Fatalf("%s has no entry after it was reported", k)
		}
		return weak.Make(en)
	}
	idle, deleted := weakOf("idle"), weakOf("deleted")
	f.c.DeleteLabelValues("deleted")

	for range 5 {
		f.c.WithLabelValues("keep").Inc()
		f.scrape(t, false)
	}
	runtime.GC()
	if idle.Value() != nil {
		t.Error("the entry of an instance dropped as idle is still reachable")
	}
	if deleted.Value() != nil {
		t.Error("the entry of an instance the application deleted from its Vec is still reachable")
	}

	f.c.WithLabelValues("keep").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=keep}`] != 2 {
		t.Fatalf("the instance that stayed should report the increment since the last scrape: %v", m)
	}
}

// With Options.Delete the child leaves its Vec only if the callback removes it.
// One that stays has to start over as a new instance, the way it did when its
// entry left a map, not carry on from the entry idle cleanup was done with.
func TestIdleEntryIsLetGoWhenDeleteKeepsTheChild(t *testing.T) {
	var deleted []string
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1,
		Delete: func(family string, labels prometheus.Labels) { deleted = append(deleted, labels["key"]) }})
	f.c.WithLabelValues("keep").Inc()
	f.c.WithLabelValues("idle").Inc()
	f.scrape(t, false)
	old := weak.Make(entryFor(t, f.c.WithLabelValues("idle")))

	for range 5 {
		f.c.WithLabelValues("keep").Inc()
		f.scrape(t, false)
	}
	if len(deleted) != 1 || deleted[0] != "idle" {
		t.Fatalf("Delete was called for %v, want [idle]", deleted)
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracked %d after idle cleanup, want 1", n)
	}
	runtime.GC()
	if old.Value() != nil {
		t.Error("the entry idle cleanup was done with is still held by the child the callback kept")
	}
}
