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
