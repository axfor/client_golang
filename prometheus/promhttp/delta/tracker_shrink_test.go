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
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// The tracker holds every instance in per-shard slices. Removing one clears the
// pointer, so nothing is retained through it, but the array stays at the size
// the process once needed: after idle cleanup has dropped most of a per-key
// metric, those arrays are still sized for every key ever seen. Nothing reads
// wrong, the memory is simply never given back, so this looks at the capacity.
func TestTrackerSlicesFallBackAfterMassDeletion(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 20000 // enough that some shard is well past minShrinkSlice
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)
	before := prometheus.TrackerCapacity(f.trk)

	// All but one age out, then keep scraping: the batch high-water mark decays
	// an eighth a scrape, the same way the dto free lists fall back, so a burst
	// is given up over a few minutes rather than the moment it ends.
	for range 30 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	if got := f.exp.Tracked(); got != 1 {
		t.Fatalf("%d instances still tracked, want 1", got)
	}

	after := prometheus.TrackerCapacity(f.trk)
	if after >= before {
		t.Errorf("the tracker still holds room for %d entries, was %d before the instances were dropped", after, before)
	}
	if after > before/2 {
		t.Errorf("capacity only fell from %d to %d; most of the arrays are still held", before, after)
	}

	// And the survivor still works through it.
	f.c.WithLabelValues("k0").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=k0}`] != 2 {
		t.Fatalf("the surviving instance lost its state: %v", m)
	}
}

// A population that holds must not be reallocated every scrape.
func TestTrackerSlicesAreNotReallocatedWhileThePopulationHolds(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 20000
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)
	before := prometheus.TrackerCapacity(f.trk)

	for range 5 {
		for i := range n {
			f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
		}
		f.scrape(t, false)
	}
	if f.exp.Tracked() != n {
		t.Fatalf("tracked %d, want %d -- nothing should have aged out", f.exp.Tracked(), n)
	}
	if got := prometheus.TrackerCapacity(f.trk); got < before {
		t.Errorf("capacity fell from %d to %d while every instance was still being written", before, got)
	}
}
