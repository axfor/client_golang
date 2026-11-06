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
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

// Idle cleanup on the Gather path: what it deletes through, what it keeps when
// it cannot delete, and what becomes of the state of series that are gone.

type testLog struct{ lines []string }

func (l *testLog) Println(v ...any) { l.lines = append(l.lines, fmt.Sprintln(v...)) }

// newGatherFixture is a fixture on the Gather path whose idle cleanup deletes
// through Options.Vecs when vecs is set, and through nothing otherwise. wrap,
// when set, wraps the registry's Gatherer.
func newGatherFixture(opts Options, vecs bool, wrap func(prometheus.Gatherer) prometheus.Gatherer) *fixture {
	f := &fixture{reg: prometheus.NewRegistry()}
	f.c = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	f.g = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "g", Help: "g"}, []string{"key"})
	f.h = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h", Help: "h", Buckets: []float64{5, 10, 25, 50, 100, 250}}, []string{"key"})
	f.reg.MustRegister(f.c, f.g, f.h)
	if vecs {
		opts.Vecs = []Vec{f.c, f.g, f.h}
	}
	var g prometheus.Gatherer = f.reg
	if wrap != nil {
		g = wrap(g)
	}
	f.exp = New(g, opts)
	return f
}

// scrapes runs n scrapes and returns what they reported, summed per series, and
// their stats, summed.
func (f *fixture) scrapes(t *testing.T, n int) (map[string]float64, ScrapeStats) {
	t.Helper()
	sum, total := map[string]float64{}, ScrapeStats{}
	for range n {
		m, st := f.scrape(t, false)
		for k, v := range m {
			sum[k] += v
		}
		total.Deleted += st.Deleted
		total.Forgotten += st.Forgotten
	}
	return sum, total
}

// With nothing to delete through, an idle series stays, and so does what it
// delivered. Dropping that alone made the next scrape report the whole value
// again, every IdleScrapes+1 scrapes: 100 written, 300 reported over 10.
func TestGatherIdleWithNothingToDeleteThroughKeepsState(t *testing.T) {
	log := &testLog{}
	f := newGatherFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, ErrorLog: log}, false, nil)
	f.c.WithLabelValues("a").Add(100)
	if m, st := f.scrapes(t, 10); m[`c_total{key=a}`] != 100 || st.Deleted != 0 {
		t.Fatalf("reported %v for 100 written, %d deletions; want 100 and none", m[`c_total{key=a}`], st.Deleted)
	}
	f.c.WithLabelValues("a").Add(100)
	if m, _ := f.scrapes(t, 10); m[`c_total{key=a}`] != 100 {
		t.Fatalf("reported %v for a second 100 written; want 100", m[`c_total{key=a}`])
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracked %d, want 1: the series stays", n)
	}
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], "c_total") {
		t.Fatalf("logged %q; want c_total named once", log.lines)
	}
}

// Through Options.Vecs an idle series leaves its Vec and the exposer, and one
// written again starts from zero.
func TestGatherIdleDeletionThroughVecs(t *testing.T) {
	f := newGatherFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2}, true, nil)
	f.c.WithLabelValues("a").Add(3)
	f.h.WithLabelValues("a").Observe(7)
	f.g.WithLabelValues("a").Set(0)
	if _, st := f.scrapes(t, 5); st.Deleted != 3 {
		t.Fatalf("%d deletions, want 3", st.Deleted)
	}
	if n := children(f.c) + children(f.g) + children(f.h); n != 0 {
		t.Fatalf("%d children left in the Vecs, want none", n)
	}
	if n := f.exp.Tracked(); n != 0 {
		t.Fatalf("tracked %d, want none", n)
	}
	f.c.WithLabelValues("a").Add(4)
	if m, _ := f.scrape(t, false); m[`c_total{key=a}`] != 4 {
		t.Fatalf("recreated after deletion it starts from zero and reports 4: %v", m)
	}
}

// A Vec with const labels is deleted from too: they are in the labels the
// scrape gathered, and Vec.Delete, which Options.Delete is usually written
// with, rejects them.
func TestGatherVecsDeleteDespiteConstLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c", ConstLabels: prometheus.Labels{"region": "r1"}}, []string{"key"})
	reg.MustRegister(c)
	f := &fixture{reg: reg, c: c, exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, Vecs: []Vec{c}})}
	c.WithLabelValues("a").Inc()
	if _, st := f.scrapes(t, 5); st.Deleted != 1 || children(c) != 0 {
		t.Fatalf("%d deletions, %d children left; want 1 and none", st.Deleted, children(c))
	}
}

// A write that lands after Gather read an idle series and before idle cleanup
// deletes it keeps the child, and the next scrape reports it. Through
// Options.Delete it was lost with the child.
func TestGatherIdleDeletionKeepsAWriteSinceGather(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*fixture)
		vec   func(*fixture) prometheus.Collector
		key   string
		want  float64
		undo  func(*fixture) // what makes it deletable again, if anything
	}{
		{"counter", func(f *fixture) { f.c.WithLabelValues("a").Add(5) }, func(f *fixture) prometheus.Collector { return f.c }, `c_total{key=a}`, 5, nil},
		{"histogram", func(f *fixture) { f.h.WithLabelValues("a").Observe(7) }, func(f *fixture) prometheus.Collector { return f.h }, `h_count{key=a}`, 1, nil},
		// a gauge is only deleted at zero, see deletable
		{"gauge", func(f *fixture) { f.g.WithLabelValues("a").Inc() }, func(f *fixture) prometheus.Collector { return f.g }, `g{key=a}`, 1, func(f *fixture) { f.g.WithLabelValues("a").Dec() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var between func()
			f := newGatherFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2}, true, func(g prometheus.Gatherer) prometheus.Gatherer {
				return prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
					mfs, err := g.Gather()
					if between != nil {
						between()
						between = nil
					}
					return mfs, err
				})
			})
			f.c.WithLabelValues("a").Add(100)
			f.h.WithLabelValues("a").Observe(1)
			f.g.WithLabelValues("a").Set(0)
			f.scrapes(t, 3) // written in the first; idle after the fourth

			between = func() { tc.write(f) }
			if _, st := f.scrape(t, false); st.Deleted != 2 {
				t.Fatalf("%d deletions in the scrape idle cleanup ran in, want the 2 series not written to", st.Deleted)
			}
			if n := children(tc.vec(f)); n != 1 {
				t.Fatalf("the child written to after Gather was deleted")
			}
			if m, _ := f.scrape(t, false); m[tc.key] != tc.want {
				t.Fatalf("%s reported %v after the write, want %v: %v", tc.key, m[tc.key], tc.want, m)
			}
			// idle again later, it goes then
			if tc.undo != nil {
				tc.undo(f)
			}
			if _, st := f.scrapes(t, 5); st.Deleted != 1 || children(tc.vec(f)) != 0 {
				t.Fatalf("%d deletions, %d children left once idle again; want 1 and none", st.Deleted, children(tc.vec(f)))
			}
		})
	}
}

// An idle series of a family with no Vec in Options.Vecs is kept, and reported
// right: a family left out, or a plain Counter, which no Vec can delete. The
// family is logged once.
func TestGatherFamilyWithoutVecIsKept(t *testing.T) {
	log := &testLog{}
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	other := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "other_total", Help: "o"}, []string{"key"})
	plain := prometheus.NewCounter(prometheus.CounterOpts{Name: "plain_total", Help: "p"})
	reg.MustRegister(c, other, plain)
	f := &fixture{reg: reg, exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, ErrorLog: log, Vecs: []Vec{c}})}
	c.WithLabelValues("a").Add(1)
	other.WithLabelValues("a").Add(9)
	plain.Add(7)

	m, st := f.scrapes(t, 12)
	for k, want := range map[string]float64{`c_total{key=a}`: 1, `other_total{key=a}`: 9, `plain_total{}`: 7} {
		if m[k] != want {
			t.Errorf("%s reported %v over 12 scrapes, want %v", k, m[k], want)
		}
	}
	if st.Deleted != 1 || children(c) != 0 || children(other) != 1 {
		t.Errorf("%d deletions, %d and %d children left; want only c_total deleted", st.Deleted, children(c), children(other))
	}
	if n := f.exp.Tracked(); n != 2 {
		t.Errorf("tracked %d, want the 2 that cannot be deleted", n)
	}
	got := strings.Join(log.lines, "")
	if len(log.lines) != 2 || !strings.Contains(got, "other_total") || !strings.Contains(got, "plain_total") {
		t.Errorf("logged %q; want other_total and plain_total named once each", log.lines)
	}
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: no panic", what)
		}
	}()
	f()
}

func TestGatherVecsMisuse(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	del := func(string, prometheus.Labels) {}
	mustPanic(t, "Vecs and Delete", func() { New(reg, Options{Vecs: []Vec{c}, Delete: del}) })
	mustPanic(t, "two Vecs of one family", func() { New(reg, Options{Vecs: []Vec{c, c}}) })
	exp := New(reg, Options{Delete: del})
	mustPanic(t, "Replace with Vecs where Delete is set", func() { exp.Replace(reg, c) })
}

// A registry replaced through Replace starts over: a series the new one has
// again is reported from zero, not against what the old one delivered, the old
// one's state goes at once, and idle cleanup deletes from the new Vecs.
func TestGatherReplace(t *testing.T) {
	newReg := func() (*prometheus.Registry, *prometheus.CounterVec) {
		reg := prometheus.NewRegistry()
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
		reg.MustRegister(c)
		return reg, c
	}
	regA, a := newReg()
	a.WithLabelValues("x").Add(1000)
	a.WithLabelValues("y").Add(1)
	f := &fixture{exp: New(regA, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, Vecs: []Vec{a}})}
	f.scrape(t, false)

	regB, b := newReg()
	b.WithLabelValues("z").Add(5)
	f.exp.Replace(regB, b)
	if n := f.exp.Tracked(); n != 0 {
		t.Fatalf("tracked %d after Replace, want none", n)
	}
	if m, _ := f.scrape(t, false); m[`c_total{key=z}`] != 5 || len(m) != 1 {
		t.Fatalf("the new registry reports %v, want only z at 5", m)
	}

	regA2, a2 := newReg()
	a2.WithLabelValues("x").Add(1)
	f.exp.Replace(regA2, a2)
	if m, _ := f.scrape(t, false); m[`c_total{key=x}`] != 1 {
		t.Fatalf("x is back with 1 written and reports %v, want 1, not 1 less what the old x delivered", m[`c_total{key=x}`])
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracked %d, want only x", n)
	}
	if _, st := f.scrapes(t, 5); st.Deleted != 1 || children(a2) != 0 {
		t.Fatalf("%d deletions, %d children left; idle cleanup should delete from the new registry's Vec", st.Deleted, children(a2))
	}
}

// A registry replaced without Replace, and replaced back before the old series
// were forgotten: a counter or classic histogram whose value went backwards is
// a new instance, and reports from zero. It was reported against what the old
// instance delivered: 1 - 1000 = -999.
func TestGatherRebuiltInstanceStartsFromZero(t *testing.T) {
	var cur *prometheus.Registry
	mk := func() (*prometheus.CounterVec, *prometheus.HistogramVec) {
		cur = prometheus.NewRegistry()
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h", Help: "h", Buckets: []float64{5, 250}}, []string{"key"})
		cur.MustRegister(c, h)
		return c, h
	}
	g := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return cur.Gather() })
	f := &fixture{exp: New(g, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 30})}

	c, h := mk()
	c.WithLabelValues("x").Add(1000)
	for range 50 {
		h.WithLabelValues("x").Observe(1)
	}
	h.WithLabelValues("y").Observe(1)
	f.scrape(t, false)
	mk()
	f.scrape(t, false)

	c, h = mk()
	c.WithLabelValues("x").Add(1)
	h.WithLabelValues("x").Observe(200) // fewer observations than before
	h.WithLabelValues("y").Observe(200) // more, but fewer at or below 5
	h.WithLabelValues("y").Observe(200)
	m, _ := f.scrape(t, false)
	for k, want := range map[string]float64{
		`c_total{key=x}`: 1,
		`h_count{key=x}`: 1, `h_sum{key=x}`: 200, `h_bucket{key=x,le=5}`: 0, `h_bucket{key=x,le=250}`: 1,
		`h_count{key=y}`: 2, `h_sum{key=y}`: 400, `h_bucket{key=y,le=5}`: 0, `h_bucket{key=y,le=250}`: 2,
	} {
		if m[k] != want {
			t.Errorf("%s reported %v, want %v", k, m[k], want)
		}
	}
}

// The state of a series the Gatherer no longer returns -- its registry
// replaced without Replace -- is let go after IdleScrapes scrapes. It was kept
// for the life of the process, as only a series gathered in the scrape was
// looked at for idleness.
func TestGatherForgetsSeriesNoLongerGathered(t *testing.T) {
	var cur *prometheus.Registry
	mk := func() *prometheus.CounterVec {
		cur = prometheus.NewRegistry()
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
		cur.MustRegister(c)
		return c
	}
	g := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return cur.Gather() })
	a := mk()
	// The family is in Vecs, which is what says a series missing from a scrape
	// is gone; that the Vec given is a's, not b's, makes no difference to it.
	f := &fixture{exp: New(g, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, Vecs: []Vec{a}})}
	a.WithLabelValues("x").Add(3)
	a.WithLabelValues("y").Add(4)
	f.scrape(t, false)

	b := mk()
	for range 2 {
		b.WithLabelValues("z").Inc()
		if _, st := f.scrape(t, false); st.Forgotten != 0 {
			t.Fatalf("forgot %d series before they were gone for 2 scrapes", st.Forgotten)
		}
	}
	b.WithLabelValues("z").Inc()
	if _, st := f.scrape(t, false); st.Forgotten != 2 {
		t.Fatalf("forgot %d series gone for 3 scrapes, want 2", st.Forgotten)
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracked %d, want only z", n)
	}
}

// A Gather that fails says nothing about which series are gone, so a series it
// misses keeps its state, and is not reported again in full once it is back.
func TestGatherErrorsForgetNothing(t *testing.T) {
	fail := false
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	reg.MustRegister(c)
	g := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		if fail {
			return nil, errors.New("collector failed")
		}
		return reg.Gather()
	})
	// with c in Vecs, so that a scrape that misses x could take it for gone
	f := &fixture{exp: New(g, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, Vecs: []Vec{c}})}
	c.WithLabelValues("x").Add(3)
	f.scrape(t, false)
	fail = true
	if _, st := f.scrapes(t, 5); st.Forgotten != 0 {
		t.Fatalf("forgot %d series on scrapes whose Gather failed", st.Forgotten)
	}
	fail = false
	c.WithLabelValues("x").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=x}`] != 2 {
		t.Fatalf("reported %v once Gather recovered, want the 2 written since", m[`c_total{key=x}`])
	}
}

// A summary or native histogram observed every scrape is not idle. Neither is
// differenced, and nothing recorded that one was written, so idle cleanup
// deleted it, and everything it held, every IdleScrapes+1 scrapes.
func TestBusyPassThroughSeriesAreNotIdle(t *testing.T) {
	t.Run("gather", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		s := prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "s", Help: "s"}, []string{"key"})
		nh := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "nh", Help: "h", NativeHistogramBucketFactor: 1.1}, []string{"key"})
		reg.MustRegister(s, nh)
		f := &fixture{reg: reg, exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, Vecs: []Vec{s, nh}})}
		deleted := 0
		for range 8 {
			s.WithLabelValues("a").Observe(1)
			nh.WithLabelValues("a").Observe(1)
			_, st := f.scrape(t, false)
			deleted += st.Deleted
		}
		if deleted != 0 {
			t.Fatalf("%d deletions of series observed every scrape", deleted)
		}
		if _, st := f.scrapes(t, 5); st.Deleted != 2 || children(s)+children(nh) != 0 {
			t.Fatalf("%d deletions once they stopped, want 2", st.Deleted)
		}
	})
	t.Run("tracked", func(t *testing.T) {
		f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
		nh := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "nh", Help: "h", NativeHistogramBucketFactor: 1.1}, []string{"key"})
		deleted := 0
		for range 8 {
			nh.WithLabelValues("a").Observe(1)
			_, st := f.scrape(t, false)
			deleted += st.Deleted
		}
		if deleted != 0 {
			t.Fatalf("%d deletions of a native histogram observed every scrape", deleted)
		}
		for range 5 {
			_, st := f.scrape(t, false)
			deleted += st.Deleted
		}
		if deleted != 1 || children(nh) != 0 {
			t.Fatalf("%d deletions once it stopped, want 1", deleted)
		}
	})
}

type collectorFunc struct {
	d *prometheus.Desc
	f func(chan<- prometheus.Metric)
}

func (c collectorFunc) Describe(ch chan<- *prometheus.Desc) { ch <- c.d }
func (c collectorFunc) Collect(ch chan<- prometheus.Metric) { c.f(ch) }

// A collector other than a Vec may leave a series out of a scrape, without an
// error, and bring it back counting on from where it was: it keeps its state
// however long it was away, and is not reported again in full.
func TestGatherSeriesLeftOutKeepsState(t *testing.T) {
	for _, withVecs := range []bool{false, true} {
		t.Run(fmt.Sprint("vecs=", withVecs), func(t *testing.T) {
			desc := prometheus.NewDesc("conn_bytes_total", "per connected client", []string{"client"}, nil)
			value, present := 1000.0, true
			reg := prometheus.NewRegistry()
			reg.MustRegister(collectorFunc{desc, func(ch chan<- prometheus.Metric) {
				if present {
					ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, "x")
				}
			}})
			opts := Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2}
			if withVecs {
				// Vecs for another family: the series left out is still not
				// taken for gone
				other := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "other_total", Help: "o"}, []string{"k"})
				reg.MustRegister(other)
				opts.Vecs = []Vec{other}
			}
			f := &fixture{exp: New(reg, opts)}
			sum, st := f.scrapes(t, 1)
			present = false
			_, st2 := f.scrapes(t, 4)
			present, value = true, 1005
			sum2, st3 := f.scrapes(t, 1)
			if got := sum["conn_bytes_total{client=x}"] + sum2["conn_bytes_total{client=x}"]; got != 1005 {
				t.Fatalf("reported %v in all for a counter that reached 1005", got)
			}
			if n := st.Forgotten + st2.Forgotten + st3.Forgotten; n != 0 {
				t.Fatalf("forgot %d series", n)
			}
		})
	}
}

// An Options.Delete that leaves an idle child in place -- it does not know the
// family, or its Vec rejects the labels -- does not cost the child its state:
// the next scrape finds the same instance and carries on from where it was.
func TestGatherDeleteThatLeavesTheChildKeepsState(t *testing.T) {
	reg := prometheus.NewRegistry()
	cost := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cost_total", Help: "c"}, []string{"key"})
	reg.MustRegister(cost)
	calls, log := 0, &testLog{}
	f := &fixture{exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, ErrorLog: log,
		Delete: func(string, prometheus.Labels) { calls++ }})}
	cost.WithLabelValues("a").Add(100)
	sum, _ := f.scrapes(t, 10)
	if calls < 2 {
		t.Fatalf("Delete called %d times in 10 scrapes, want it offered the series again once idle again", calls)
	}
	cost.WithLabelValues("a").Add(1)
	sum2, _ := f.scrapes(t, 1)
	if sum["cost_total{key=a}"] != 100 || sum2["cost_total{key=a}"] != 1 {
		t.Fatalf("reported %v then %v for 100 then 1 written", sum["cost_total{key=a}"], sum2["cost_total{key=a}"])
	}
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracked %d, want 1", n)
	}
	if len(log.lines) != 1 || !strings.Contains(log.lines[0], "left idle series of cost_total in place") {
		t.Fatalf("logged %q, want one line on cost_total", log.lines)
	}
}

// One that does delete the child lets the state go, and a child created anew
// under the same labels before the next scrape is told apart by its creation
// time even once it holds more than the old one delivered.
func TestGatherDeleteThatDeletesLetsGo(t *testing.T) {
	reg := prometheus.NewRegistry()
	cost := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cost_total", Help: "c"}, []string{"key"})
	reg.MustRegister(cost)
	f := &fixture{exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2,
		Delete: func(_ string, l prometheus.Labels) { cost.Delete(l) }})}
	cost.WithLabelValues("a").Add(100)
	sum, renewed := map[string]float64{}, false
	for range 10 {
		m, st := f.scrape(t, false)
		sum["a"] += m["cost_total{key=a}"]
		if st.Deleted > 0 && !renewed {
			cost.WithLabelValues("a").Add(150) // a new child
			renewed = true
		}
	}
	if sum["a"] != 250 {
		t.Fatalf("reported %v for 100 written to one child and 150 to its successor", sum["a"])
	}
}

// After Replace, an Options.Delete still bound to the old registry deletes
// nothing in the new one, which keeps its state; what was logged once under
// the old registry is logged again under the new.
func TestGatherReplaceWithStaleDelete(t *testing.T) {
	mk := func() (*prometheus.Registry, *prometheus.CounterVec) {
		reg := prometheus.NewRegistry()
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
		reg.MustRegister(c)
		return reg, c
	}
	regA, a := mk()
	log := &testLog{}
	f := &fixture{exp: New(regA, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, ErrorLog: log,
		Delete: func(_ string, l prometheus.Labels) {
			if l["key"] != "k" { // one it leaves in place, logged under A
				a.Delete(l)
			}
		}})}
	a.WithLabelValues("k").Inc()
	a.WithLabelValues("z").Inc()
	f.scrapes(t, 6)
	if n := children(a); n != 1 {
		t.Fatalf("%d children left in the old registry, want only k", n)
	}

	regB, b := mk()
	f.exp.Replace(regB)
	b.WithLabelValues("x").Add(50)
	sum, _ := f.scrapes(t, 8)
	b.WithLabelValues("x").Inc()
	sum2, _ := f.scrapes(t, 1)
	if sum["c_total{key=x}"] != 50 || sum2["c_total{key=x}"] != 1 {
		t.Fatalf("reported %v then %v for 50 then 1 written", sum["c_total{key=x}"], sum2["c_total{key=x}"])
	}
	if n := children(b); n != 1 {
		t.Fatalf("%d children left in the new registry, want 1", n)
	}
	var left int
	for _, l := range log.lines {
		if strings.Contains(l, "left idle series of c_total") {
			left++
		}
	}
	if left != 2 {
		t.Fatalf("logged %q, want a line for each registry", log.lines)
	}
}

// countingVec counts what idle cleanup asks of a Vec.
type countingVec struct {
	*prometheus.CounterVec
	calls int
}

func (v *countingVec) DeleteCollectedIf(l []*dto.LabelPair, ok func(prometheus.Metric) bool) bool {
	v.calls++
	return v.CounterVec.DeleteCollectedIf(l, ok)
}

// A Vec in Options.Vecs that does not hold a child -- curried to other values,
// or not the one registered -- cannot delete it: that is logged once, and the
// child is tried again after IdleScrapes more scrapes, not every scrape.
func TestGatherVecThatDoesNotHoldTheChild(t *testing.T) {
	for _, tc := range []struct {
		name string
		vec  func(*prometheus.CounterVec) *prometheus.CounterVec
		held int // how many of the two children the Vec holds
	}{
		{"curried", func(c *prometheus.CounterVec) *prometheus.CounterVec {
			return c.MustCurryWith(prometheus.Labels{"tier": "free"})
		}, 1},
		{"unregistered", func(*prometheus.CounterVec) *prometheus.CounterVec {
			return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key", "tier"})
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key", "tier"})
			reg.MustRegister(c)
			v, log := &countingVec{CounterVec: tc.vec(c)}, &testLog{}
			f := &fixture{exp: New(reg, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, ErrorLog: log, Vecs: []Vec{v}})}
			c.WithLabelValues("a", "paid").Inc()
			c.WithLabelValues("a", "free").Inc()
			_, st := f.scrapes(t, 13)

			if n := children(c); n != 2-tc.held {
				t.Fatalf("%d children left, want %d", n, 2-tc.held)
			}
			// idle first after 3 scrapes, then every 3 more: 4 tries in 13
			// scrapes for each series the Vec cannot delete, not 10
			if tries := v.calls - st.Deleted; tries > 4*(2-tc.held) {
				t.Fatalf("tried %d times to delete the %d series the Vec does not hold", tries, 2-tc.held)
			}
			if len(log.lines) != 1 || !strings.Contains(log.lines[0], "does not hold it") {
				t.Fatalf("logged %q, want one line", log.lines)
			}
		})
	}
}

// A histogram served as a gauge histogram is idle when its count has not moved,
// as decide sees it, and is deleted as such.
func TestGatherGaugeHistogramIsDeleted(t *testing.T) {
	asGauge := func(g prometheus.Gatherer) prometheus.Gatherer {
		return prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
			mfs, err := g.Gather()
			for _, mf := range mfs {
				if mf.GetName() == "h" {
					mf.Type = dto.MetricType_GAUGE_HISTOGRAM.Enum()
				}
			}
			return mfs, err
		})
	}
	f := newGatherFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2}, true, asGauge)
	f.h.WithLabelValues("a").Observe(7)
	var deleted int
	for range 6 {
		rec := httptest.NewRecorder()
		deleted += f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics/delta", nil)).Deleted
	}
	if deleted != 1 || children(f.h) != 0 {
		t.Fatalf("deleted %d, %d children left; want the idle gauge histogram deleted", deleted, children(f.h))
	}
}
