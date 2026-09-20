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
	"strconv"
	"sync"
	"testing"
	"unsafe"

	dto "github.com/prometheus/client_model/go"
)

func pairsOf(t *testing.T, names, values []string) []*dto.LabelPair {
	t.Helper()
	desc := NewDesc("m", "h", names, nil)
	return MakeLabelPairs(desc, values)
}

// What the cache is for: the metrics measuring one instance all carry the same
// labels, and they end up pointing at one set of pairs rather than a copy each.
func TestLabelPairsAreSharedBetweenMetrics(t *testing.T) {
	names := []string{"key", "route"}
	values := []string{"k-shared", "/a"}

	first := pairsOf(t, names, values)
	second := pairsOf(t, names, values)
	if len(first) != 2 {
		t.Fatalf("got %d pairs, want 2", len(first))
	}
	if &first[0] != &second[0] {
		t.Errorf("two metrics with the same labels hold separate copies; they should share one")
	}
}

// The dangerous failure is not a miss, it is a hit that returns somebody else's
// labels. Every way two label sets can differ has to miss.
func TestLabelPairsNeverReturnTheWrongSet(t *testing.T) {
	base := struct{ names, values []string }{
		[]string{"key", "route"}, []string{"k1", "/a"},
	}
	pairsOf(t, base.names, base.values) // seed the cache

	for _, tc := range []struct {
		name   string
		names  []string
		values []string
	}{
		{"a different value", []string{"key", "route"}, []string{"k2", "/a"}},
		{"a different name", []string{"key", "path"}, []string{"k1", "/a"}},
		{"fewer labels", []string{"key"}, []string{"k1"}},
		{"more labels", []string{"key", "route", "zone"}, []string{"k1", "/a", "z"}},
		{"the same text split differently", []string{"ke", "yroute"}, []string{"k1", "/a"}},
		{"values swapped", []string{"key", "route"}, []string{"/a", "k1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pairsOf(t, tc.names, tc.values)
			if len(got) != len(tc.names) {
				t.Fatalf("got %d pairs, want %d", len(got), len(tc.names))
			}
			for i, lp := range got {
				var want string
				for j, n := range tc.names {
					if n == lp.GetName() {
						want = tc.values[j]
					}
					_ = j
				}
				if lp.GetValue() != want {
					t.Errorf("pair %d is %s=%q, want %q for that name", i, lp.GetName(), lp.GetValue(), want)
				}
			}
		})
	}
}

// A slot the cache is asked about while another goroutine is replacing it must
// return that slot's own labels or nothing, never a mix of two.
func TestLabelPairsUnderConcurrentReplacement(t *testing.T) {
	const writers, rounds = 8, 2000
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			names := []string{"key", "route"}
			for i := range rounds {
				v := strconv.Itoa(w*rounds + i)
				values := []string{"k" + v, "/r" + v}
				got := MakeLabelPairs(NewDesc("m", "h", names, nil), values)
				if len(got) != 2 {
					t.Errorf("got %d pairs, want 2", len(got))
					return
				}
				for _, lp := range got {
					want := values[0]
					if lp.GetName() == "route" {
						want = values[1]
					}
					if lp.GetValue() != want {
						t.Errorf("%s=%q, want %q", lp.GetName(), lp.GetValue(), want)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// Constant labels make the pairs depend on the Desc as well, so those are built
// every time rather than shared on the names alone.
func TestLabelPairsWithConstLabelsAreNotShared(t *testing.T) {
	names, values := []string{"key"}, []string{"k"}
	a := MakeLabelPairs(NewDesc("m", "h", names, Labels{"zone": "z1"}), values)
	b := MakeLabelPairs(NewDesc("m", "h", names, Labels{"zone": "z2"}), values)

	find := func(pairs []*dto.LabelPair, name string) string {
		for _, lp := range pairs {
			if lp.GetName() == name {
				return lp.GetValue()
			}
		}
		return ""
	}
	if find(a, "zone") != "z1" || find(b, "zone") != "z2" {
		t.Fatalf("constant labels were mixed up: %q and %q", find(a, "zone"), find(b, "zone"))
	}
	if unsafe.SliceData(a) == unsafe.SliceData(b) {
		t.Error("two Descs with different constant labels share one set of pairs")
	}
}

// The caller owns the slice it passes, and is free to write into it again. What
// the cache keeps has to be its own copy, or a later lookup matches against
// values that have since changed and hands back the wrong pairs.
func TestLabelPairsWhenTheCallerReusesItsSlice(t *testing.T) {
	desc := NewDesc("m", "h", []string{"key", "route"}, nil)
	values := []string{"k-reuse-1", "/a"}
	first := MakeLabelPairs(desc, values)

	values[0] = "k-reuse-2" // the caller reuses its buffer for the next child
	second := MakeLabelPairs(desc, values)

	if second[0].GetValue() == first[0].GetValue() {
		t.Fatalf("the second child got the first one's labels: both read %q", second[0].GetValue())
	}
	if got := second[0].GetValue(); got != "k-reuse-2" {
		t.Errorf("key=%q, want %q", got, "k-reuse-2")
	}
	if got := first[0].GetValue(); got != "k-reuse-1" {
		t.Errorf("the first child's labels were changed under it: key=%q, want %q", got, "k-reuse-1")
	}
}

// Two label sets whose text differs only in where the names are split must not
// land in one slot: they would evict each other and neither would ever be
// shared, which costs the memory this cache exists to save without failing any
// correctness check.
func TestLabelPairsSplitDifferentlyGetTheirOwnSlot(t *testing.T) {
	names, values := []string{"key", "route"}, []string{"kslot", "/a"}
	first := pairsOf(t, names, values)
	pairsOf(t, []string{"ke", "yroute"}, values) // same concatenated text
	again := pairsOf(t, names, values)

	if &first[0] != &again[0] {
		t.Error("the two label sets share a slot and evict each other; the separator in the hash is not doing its work")
	}
}

// findSlotCollision returns two values that land in the same slot with these
// names, so that a test can exercise what happens when the compare is the only
// thing standing between a lookup and somebody else's labels.
func findSlotCollision(t *testing.T, names []string, tail string) (string, string) {
	t.Helper()
	seen := map[uint32]string{}
	for i := 0; i < 1<<20; i++ {
		v := "c" + strconv.Itoa(i)
		h := labelPairHash(names, []string{v, tail})
		if prev, ok := seen[h]; ok {
			return prev, v
		}
		seen[h] = v
	}
	t.Fatal("no two values landed in the same slot")
	return "", ""
}

// Both of the cache's guards only ever matter on a slot two label sets share,
// and two values picked by hand almost never do. These use a pair that actually
// collides, so the compare is the only thing left.
func TestLabelPairsOnACollidingSlot(t *testing.T) {
	names := []string{"key", "route"}
	v1, v2 := findSlotCollision(t, names, "/a")
	desc := NewDesc("m", "h", names, nil)

	t.Run("the caller reusing its slice", func(t *testing.T) {
		// Both values share a slot, so mutating the caller's slice from one to
		// the other looks the slot up again rather than landing elsewhere. If
		// what the cache kept were the caller's slice, it would now read v2 and
		// match, handing back the pairs built for v1.
		values := []string{v1, "/a"}
		first := MakeLabelPairs(desc, values)
		values[0] = v2
		second := MakeLabelPairs(desc, values)

		if got := second[0].GetValue(); got != v2 {
			t.Errorf("key=%q, want %q -- the cache kept the caller's slice", got, v2)
		}
		if got := first[0].GetValue(); got != v1 {
			t.Errorf("the first set was changed under its holder: key=%q, want %q", got, v1)
		}
	})

	t.Run("a different number of labels", func(t *testing.T) {
		// A one-label set that shares a slot with the two-label set built from
		// the same value. Then the names and values the compare does look at all
		// match, and only the length tells the two apart: without that check the
		// one-label metric is handed two pairs -- or the compare runs off the end
		// of the shorter slice.
		two := []string{"key", "route"}
		one := []string{"key"}
		var v string
		for i := 0; i < 1<<22 && v == ""; i++ {
			c := "n" + strconv.Itoa(i)
			if labelPairHash(one, []string{c}) == labelPairHash(two, []string{c, "/a"}) {
				v = c
			}
		}
		if v == "" {
			t.Skip("no value put the one- and two-label sets in one slot")
		}

		MakeLabelPairs(NewDesc("m", "h", two, nil), []string{v, "/a"})
		short := MakeLabelPairs(NewDesc("m", "h", one, nil), []string{v})
		if len(short) != 1 {
			t.Fatalf("a one-label metric got %d pairs", len(short))
		}
		if got := short[0].GetValue(); got != v {
			t.Errorf("key=%q, want %q", got, v)
		}
	})
}
