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

import "testing"

// A long-running request holds the in-flight gauge at a non-zero value across
// several scrapes without writing it again. Deleting it there would leave the
// Dec that ends the request to land on an instance rebuilt from zero.
func TestGaugeHeldNonZeroSurvivesIdleCleanup(t *testing.T) {
	// change-tracking path
	ft := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	ft.g.WithLabelValues("inflight").Inc()
	deleted := 0
	for i := 0; i < 6; i++ {
		_, st := ft.scrape(t, false)
		deleted += st.Deleted
	}
	ft.g.WithLabelValues("inflight").Dec()
	m, _ := ft.scrape(t, false)
	if deleted != 0 || m[`g{key=inflight}`] != 0 {
		t.Fatalf("tracked path: %d deletions while held at 1, gauge ends at %v (want 0 and 0)", deleted, m[`g{key=inflight}`])
	}

	// Gather path
	fg := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2})
	fg.g.WithLabelValues("inflight").Inc()
	deleted = 0
	for i := 0; i < 6; i++ {
		_, st := fg.scrape(t, false)
		deleted += st.Deleted
	}
	fg.g.WithLabelValues("inflight").Dec()
	m, _ = fg.scrape(t, false)
	if deleted != 0 || m[`g{key=inflight}`] != 0 {
		t.Fatalf("gather path: %d deletions while held at 1, gauge ends at %v (want 0 and 0)", deleted, m[`g{key=inflight}`])
	}
}

// A gauge that is back at zero and then goes idle is still deleted.
func TestGaugeAtZeroIsStillDeleted(t *testing.T) {
	f := newFixture(Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2})
	f.g.WithLabelValues("done").Inc()
	f.scrape(t, false)
	f.g.WithLabelValues("done").Dec() // back to zero
	deleted := 0
	for i := 0; i < 6; i++ {
		_, st := f.scrape(t, false)
		deleted += st.Deleted
	}
	if deleted == 0 {
		t.Fatalf("a gauge back at zero and idle should be deleted")
	}
}
