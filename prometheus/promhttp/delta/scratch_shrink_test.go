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
)

// exposerCapacity is what the per-scrape scratch is holding room for. These are
// all sized by the first scrape after start-up -- every instance is new then,
// so every one is written -- and clearing them releases what they pointed at
// but keeps the arrays and buckets.
func exposerCapacity(e *TrackedExposer) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := cap(e.taken) + cap(e.fam.rows) + cap(e.fam.nums) + cap(e.fam.text)
	for _, s := range e.slots {
		total += cap(s.picks)
	}
	return total
}

func TestScrapeScratchFallsBackAfterMassDeletion(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 20000
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
		f.h.WithLabelValues("k" + strconv.Itoa(i)).Observe(1)
	}
	f.scrape(t, false)

	for range 30 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	if got := f.exp.Tracked(); got == 0 || got > 3 {
		t.Fatalf("%d instances tracked, want the handful still being written", got)
	}

	f.c.WithLabelValues("k0").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=k0}`] != 2 {
		t.Fatalf("the surviving instance lost its state: %v", m)
	}
}

// The scratch a scrape builds is given back with the scrape, including while
// the population holds and every instance is written each time: kept for the
// next one it would be live heap for the whole interval, and under the default
// GOGC the collector lets the heap grow to twice what is live.
func TestScrapeScratchIsReleasedAfterEveryScrape(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 20000
	for round := range 3 {
		for i := range n {
			f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
			f.h.WithLabelValues("k" + strconv.Itoa(i)).Observe(1)
		}
		m, _ := f.scrape(t, false)
		if len(m) < n {
			t.Fatalf("round %d reported %d series, want at least the %d counters", round, len(m), n)
		}
		if got := exposerCapacity(f.exp); got != 0 {
			t.Errorf("round %d: the scrape scratch still holds room for %d after the scrape", round, got)
		}
	}
}
