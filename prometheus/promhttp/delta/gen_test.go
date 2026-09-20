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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// GenLabel puts the creation time on counters and histograms, so an instance
// deleted as idle and written to again is a different series. Gauges never
// carry it.
func TestGenLabel(t *testing.T) {
	f := newTrackedFixture(t, Options{DisableHeartbeat: true, GenLabel: "gen", IdleScrapes: 2, HeartbeatScrapes: 1})
	now := time.Unix(1000, 0)
	f.exp.rb.now = func() time.Time { return now }

	f.c.WithLabelValues("a").Add(3)
	f.g.WithLabelValues("a").Set(7)
	m, _ := f.scrape(t, false)
	if m[`c_total{gen=1000,key=a}`] != 3 {
		t.Fatalf("counter should carry gen=1000: %v", m)
	}
	if m[`g{key=a}`] != 7 {
		t.Fatalf("a gauge must not carry the generation label: %v", m)
	}

	// let it go idle and be deleted, then write it again in a later second
	deleted := 0
	for i := 0; i < 6; i++ {
		_, st := f.scrape(t, false)
		deleted += st.Deleted
	}
	if deleted == 0 {
		t.Fatalf("the idle counter should have been deleted")
	}
	now = time.Unix(2000, 0)
	f.c.WithLabelValues("a").Add(4)
	m, _ = f.scrape(t, false)
	if m[`c_total{gen=2000,key=a}`] != 4 {
		t.Fatalf("the rebuilt counter should be a new series gen=2000 reporting 4: %v", m)
	}
	if _, ok := m[`c_total{gen=1000,key=a}`]; ok {
		t.Fatalf("the old generation should be gone: %v", m)
	}
}

// After a gap longer than RebaseAfterGap the next scrape moves to a new
// generation and reports only what was never delivered; after that it keeps
// accumulating under that generation and does not move again.
func TestRebaseAfterGap(t *testing.T) {
	f := newFixture(Options{GenLabel: "gen", RebaseAfterGap: 30 * time.Minute, HeartbeatScrapes: 1})
	now := time.Unix(1000, 0)
	f.exp.rb.now = func() time.Time { return now }

	f.c.WithLabelValues("a").Add(60)
	m, st := f.scrape(t, false)
	if m[`c_total{gen=1000,key=a}`] != 60 || st.Rebased {
		t.Fatalf("first scrape reports the cumulative 60 under its own generation: %v (%+v)", m, st)
	}

	// the last scrape before the gap is not delivered, so its 70 is not committed
	f.c.WithLabelValues("a").Add(10)
	if _, st = f.scrape(t, true); st.Rebased || st.Delivered {
		t.Fatalf("not delivered and not yet time to rebase: %+v", st)
	}

	// 46 minutes of gap, during which another 30 arrives; never delivered: 10 + 30
	now = time.Unix(1000+46*60, 0)
	f.c.WithLabelValues("a").Add(30)
	m, st = f.scrape(t, false)
	if !st.Rebased {
		t.Fatalf("46 minutes since the last delivery: the generation should move (%+v)", st)
	}
	gen := "3760"
	if got := m[`c_total{gen=`+gen+`,key=a}`]; got != 40 {
		t.Fatalf("under the new generation it reports only the never-delivered 40, got %v: %v", got, m)
	}
	for k := range m {
		if strings.Contains(k, "gen=1000") {
			t.Fatalf("the old generation should be gone: %v", m)
		}
	}

	// afterwards it keeps accumulating under the same generation, without moving again
	f.c.WithLabelValues("a").Add(5)
	m, st = f.scrape(t, false)
	if st.Rebased || m[`c_total{gen=`+gen+`,key=a}`] != 45 {
		t.Fatalf("it should keep accumulating to 45 under gen=%s without moving: %v (%+v)", gen, m, st)
	}
}

// A scrape that moved the generation but was not delivered keeps that same
// generation next time: it may well have reached the aggregator, and moving
// again would make it count the new generation twice.
func TestRebaseKeepsGenerationWhenUndelivered(t *testing.T) {
	f := newFixture(Options{GenLabel: "gen", RebaseAfterGap: 30 * time.Minute, HeartbeatScrapes: 1})
	now := time.Unix(1000, 0)
	f.exp.rb.now = func() time.Time { return now }

	f.c.WithLabelValues("a").Add(60)
	f.scrape(t, false)

	now = time.Unix(1000+46*60, 0)
	f.c.WithLabelValues("a").Add(40)
	if _, st := f.scrape(t, true); !st.Rebased {
		t.Fatalf("the generation should move on this scrape")
	}
	now = time.Unix(1000+92*60, 0)
	f.c.WithLabelValues("a").Add(1)
	m, st := f.scrape(t, false)
	if st.Rebased {
		t.Fatalf("it must not move again before a delivery: %+v", st)
	}
	if got := m[`c_total{gen=3760,key=a}`]; got != 41 {
		t.Fatalf("it should keep gen=3760 and report 41, got %v: %v", got, m)
	}
}

func TestOptionsAreChecked(t *testing.T) {
	for _, c := range []struct {
		name string
		opts Options
	}{
		{"RebaseAfterGap without GenLabel", Options{RebaseAfterGap: time.Minute}},
		{"ReportIncrements with RebaseAfterGap", Options{ReportIncrements: true, GenLabel: "gen", RebaseAfterGap: time.Minute}},
		{"DisableHeartbeat without increments, idle cleanup or GenLabel", Options{DisableHeartbeat: true}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s should panic", c.name)
				}
			}()
			New(nil, c.opts)
		}()
	}
}

// The Delete callback looks a child up in its Vec, which has no generation
// label, so the labels it is handed must not carry one.
func TestDeleteCallbackGetsLabelsWithoutGeneration(t *testing.T) {
	var got []prometheus.Labels
	f := newTrackedFixture(t, Options{
		DisableHeartbeat: true, GenLabel: "gen",
		IdleScrapes: 2, HeartbeatScrapes: 1,
		Delete: func(_ string, labels prometheus.Labels) { got = append(got, labels) },
	})
	f.c.WithLabelValues("a").Add(1)
	for i := 0; i < 6; i++ {
		f.scrape(t, false)
	}
	if len(got) == 0 {
		t.Fatalf("the idle counter should have been handed to the callback")
	}
	for _, l := range got {
		if _, ok := l["gen"]; ok {
			t.Fatalf("the callback must not receive the generation label: %v", l)
		}
		if l["key"] != "a" {
			t.Fatalf("the callback should receive the caller's labels: %v", l)
		}
	}
}
