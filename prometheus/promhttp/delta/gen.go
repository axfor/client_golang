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
	"time"
)

// The generation label and the RebaseAfterGap bookkeeping.
//
// A counter or histogram that is deleted as idle and written to again starts
// from zero. Without anything to tell the two stretches apart, the collector
// only sees a counter whose value fell, and where it did not fall, because the
// instances that go idle are the low-traffic ones whose value is small, it sees
// no reset at all and undercounts. Options.GenLabel puts the instance's creation
// time on the series, so the rebuilt instance is a different one.
//
// RebaseAfterGap covers the other direction. An aggregator forgets an input that
// has had no samples for staleness_interval, by wall-clock time, while idle
// cleanup counts scrapes and does not advance while scraping is down. After a
// gap longer than that, the whole cumulative value reported on recovery would be
// counted as a new input, i.e. twice. Moving to a new generation and reporting
// only what was never delivered avoids it.

// rebaseState is the part of an exposer that tracks generations. Both exposers
// embed one.
type rebaseState struct {
	now           func() time.Time
	lastDelivered time.Time
	gen           int64 // generation of the most recent rebase, 0 if never rebased
	pending       bool  // the generation moved and nothing has been delivered since
}

func (rb *rebaseState) clock() time.Time {
	if rb.now != nil {
		return rb.now()
	}
	return time.Now()
}

// due reports whether this scrape has to move to a new generation, and moves it.
//
// It does not move twice in a row without a delivery in between: the scrape that
// moved may well have reached the aggregator, and moving again would make it
// count the new generation's value a second time.
func (rb *rebaseState) due(gap time.Duration) bool {
	if gap <= 0 || rb.pending {
		return false
	}
	now := rb.clock()
	if rb.lastDelivered.IsZero() {
		rb.lastDelivered = now
		return false
	}
	if now.Sub(rb.lastDelivered) <= gap {
		return false
	}
	rb.gen = now.Unix()
	rb.pending = true
	return true
}

// delivered records a delivered scrape, which releases the next rebase.
func (rb *rebaseState) delivered() {
	rb.lastDelivered = rb.clock()
	rb.pending = false
}

// genOf returns the generation a series reports under: its own creation time, or
// the generation of the last rebase when the series is older than it.
func (rb *rebaseState) genOf(en *entry) int64 {
	if rb.gen != 0 && int64(en.born) < rb.gen {
		return rb.gen
	}
	return int64(en.born)
}

// rebaseEntry brings an entry into the current generation the first time it is
// written after a rebase: what it had already delivered becomes its base, so the
// new series starts from the increment that never arrived.
func (rb *rebaseState) rebaseEntry(en *entry) {
	// Read the generation this entry is based on without allocating the side
	// struct to hold it: with no generation in play, which is every scrape of a
	// configuration that reports increments, both are zero and there is nothing
	// to do. Allocating first would put one on every instance -- the very cost
	// moving these fields out was meant to avoid.
	var basedOn int64
	if x := en.extra; x != nil {
		basedOn = x.basedOn
	}
	if basedOn == rb.gen {
		return
	}
	x := en.ext()
	x.basedOn = rb.gen
	x.baseValue = en.value
	x.baseSum = en.sum
	x.baseCount = en.count
	x.baseBuckets = append(x.baseBuckets[:0], en.buckets...)
}

func (en *entry) baseBucket(i int) uint64 {
	if x := en.extra; x != nil && i < len(x.baseBuckets) {
		return x.baseBuckets[i]
	}
	return 0
}
