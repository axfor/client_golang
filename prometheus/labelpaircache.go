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

package prometheus

import (
	"sync/atomic"

	dto "github.com/prometheus/client_model/go"
)

// One instance of a thing being measured usually carries the same labels in
// every metric that measures it: a request counter, a latency histogram and an
// in-flight gauge for one API key all read key="…", route="…". Each of those is
// a separate child, and each built and kept its own label pairs -- for a sidecar
// holding 30k keys across 36 metrics that was 870 MB, 39% of its heap, for about
// 30k distinct label sets.
//
// Children of one instance are created together, by the first write that touches
// it, so a small cache of what was built most recently catches nearly all of it.
// This is a cache, not a table of record: a miss builds the pairs the way it
// always did, and a slot is overwritten without ceremony, so nothing has to be
// swept when a child is deleted and the memory it holds is bounded by the slot
// count rather than by how many instances exist.
//
// Sharing the pairs means a caller that writes into what Write handed it changes
// what other metrics report. That was already true of a Desc's constant labels,
// which every metric of that Desc has always shared; the dto belongs to the
// collector, and nothing in this package writes to a label pair.
const (
	labelPairSlotBits = 12
	labelPairSlots    = 1 << labelPairSlotBits
)

type labelPairSlot struct {
	names  []string
	values []string
	pairs  []*dto.LabelPair
}

var labelPairCache [labelPairSlots]atomic.Pointer[labelPairSlot]

// cachedLabelPairs returns the shared pairs for these names and values, or nil
// when this slot holds something else.
//
// Only called for a Desc with no constant labels: with them the pairs depend on
// the Desc as well, and the names alone no longer identify what to build.
func cachedLabelPairs(names, values []string) []*dto.LabelPair {
	s := labelPairCache[labelPairHash(names, values)].Load()
	if s == nil || len(s.names) != len(names) || len(s.values) != len(values) {
		return nil
	}
	for i := range names {
		if s.names[i] != names[i] || s.values[i] != values[i] {
			return nil
		}
	}
	return s.pairs
}

// sharedLabelValues returns the copy of these values the cache already holds, or
// nil when it holds something else. A Vec keeps the values of every child it
// has, to match against on Delete and on a partial lookup, and those are the
// same values for every metric measuring one instance -- so they are the same
// slice, read but never written. Callers keep their own when this returns nil.
func sharedLabelValues(names, values []string) []string {
	s := labelPairCache[labelPairHash(names, values)].Load()
	if s == nil || len(s.names) != len(names) || len(s.values) != len(values) {
		return nil
	}
	for i := range names {
		if s.names[i] != names[i] || s.values[i] != values[i] {
			return nil
		}
	}
	return s.values
}

// putLabelPairs offers pairs to the cache. names and values are copied: they
// belong to the caller, which is free to reuse them.
func putLabelPairs(names, values []string, pairs []*dto.LabelPair) {
	s := &labelPairSlot{
		names:  append(make([]string, 0, len(names)), names...),
		values: append(make([]string, 0, len(values)), values...),
		pairs:  pairs,
	}
	labelPairCache[labelPairHash(names, values)].Store(s)
}

// labelPairHash is FNV-1a over the names and values, with a separator so that
// ("ab","c") and ("a","bc") do not land in one slot.
//
// The slot comes from the top bits, not h % slots: FNV's lowest bit is the
// parity of every input byte, so taking the bottom bits leaves whole classes of
// inputs that can never share a slot and others that collide in step. That is
// not a correctness problem -- the compare catches it either way -- but it is a
// hit rate one, and this cache exists for the hit rate.
func labelPairHash(names, values []string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	mix := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint32(s[i])
			h *= prime
		}
		h ^= 0xff
		h *= prime
	}
	for i := range names {
		mix(names[i])
	}
	for i := range values {
		mix(values[i])
	}
	return h >> (32 - labelPairSlotBits)
}
