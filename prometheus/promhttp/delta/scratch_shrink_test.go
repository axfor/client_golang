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
	"reflect"
	"strconv"
	"testing"
)

// internPtr identifies the label table itself. A Go map does not expose its
// capacity, and clearing one leaves len at zero either way, so the only way to
// see whether the buckets were given back is whether the map was replaced.
func internPtr(e *TrackedExposer) uintptr {
	e.mu.Lock()
	defer e.mu.Unlock()
	return reflect.ValueOf(e.enc.intern).Pointer()
}

// exposerCapacity is what the per-scrape scratch is holding room for. These are
// all sized by the first scrape after start-up -- every instance is new then,
// so every one is written -- and clearing them releases what they pointed at
// but keeps the arrays and buckets.
func exposerCapacity(e *TrackedExposer) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := cap(e.taken) + cap(e.lent)
	for _, r := range e.rows {
		total += cap(r)
	}
	for _, l := range e.free {
		total += cap(l)
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
	before := exposerCapacity(f.exp)
	beforeIntern := internPtr(f.exp)

	for range 30 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	if got := f.exp.Tracked(); got == 0 || got > 3 {
		t.Fatalf("%d instances tracked, want the handful still being written", got)
	}

	after := exposerCapacity(f.exp)
	if after > before/4 {
		t.Errorf("the scrape scratch still holds room for %d, was %d -- the arrays were cleared but not given back", after, before)
	}
	if internPtr(f.exp) == beforeIntern {
		t.Error("the label table was cleared but not replaced, so it still holds buckets for every instance that is gone")
	}

	f.c.WithLabelValues("k0").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=k0}`] != 2 {
		t.Fatalf("the surviving instance lost its state: %v", m)
	}
}

func TestScrapeScratchIsNotReallocatedWhileThePopulationHolds(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 20000
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)
	before := exposerCapacity(f.exp)

	for range 5 {
		for i := range n {
			f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
		}
		f.scrape(t, false)
	}
	if got := exposerCapacity(f.exp); got < before/2 {
		t.Errorf("capacity fell from %d to %d while every instance was still being written", before, got)
	}
}

// The first version of all three of these passed a test that only asked
// whether the map object had been replaced. It had -- with one sized from the
// decayed mark, so the replacement was larger than what it replaced, and the
// mark was then set low enough that it never fired again. What has to be
// asserted is that the thing shrinks and keeps shrinking.

// A population that drains gradually has to be given back too. A mark that
// decays as fast as the drain is re-pinned every scrape and never gets two
// times above it, so only a collapse inside a single scrape would ever fire.
func TestStateMapIsRebuiltAfterAGradualDrain(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = minShrinkState + 4000
	live := make([]string, 0, n)
	for i := range n {
		k := "k" + strconv.Itoa(i)
		live = append(live, k)
		f.c.WithLabelValues(k).Inc()
	}
	f.scrape(t, false)
	before := mapPtr(f.exp)

	// Drain a tenth of the population per scrape -- slower than any decay a
	// mark could sensibly use.
	for len(live) > n/8 {
		live = live[:len(live)-len(live)/10]
		for _, k := range live {
			f.c.WithLabelValues(k).Inc()
		}
		f.scrape(t, false)
	}
	if mapPtr(f.exp) == before {
		t.Errorf("the state map drained from %d to %d without being rebuilt", n, f.exp.Tracked())
	}
}

// After a rebuild the mark has to sit at what is actually there. A Go map does
// not expose its capacity, so the size the replacement is made with can only be
// seen by measuring -- sizing it from the mark rather than from what is left
// rebuilds the table at the size it was meant to give back, which showed up as
// the label table not falling in a heap profile rather than as a failing test.
// What is assertable is the bookkeeping the sizing reads.
func TestRebuiltTableMarkFollowsWhatIsLeft(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = minShrink * 8
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)

	f.exp.mu.Lock()
	peak := f.exp.enc.internPeak
	f.exp.mu.Unlock()
	if peak < n {
		t.Fatalf("the mark is %d after interning at least %d", peak, n)
	}

	for range 30 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	f.exp.mu.Lock()
	peak, held := f.exp.enc.internPeak, len(f.exp.enc.intern)
	f.exp.mu.Unlock()
	if peak > n/2 {
		t.Errorf("the mark is still %d after the table drained to %d entries", peak, held)
	}
}
