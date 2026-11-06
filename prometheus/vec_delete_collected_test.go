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
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// collected returns the labels m carries when collected.
func collected(t *testing.T, m Metric) []*dto.LabelPair {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.Label
}

func counterValue(t *testing.T, m Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

// DeleteCollectedIf deletes the child with the labels a scrape collected, and
// only when the check passes, and never creates a child.
func TestDeleteCollectedIf(t *testing.T) {
	for _, collide := range []bool{false, true} {
		vec := NewCounterVec(CounterOpts{Name: "c_total", Help: "h", ConstLabels: Labels{"region": "r1"}}, []string{"key", "model"})
		if collide {
			vec.hashAdd = func(h uint64, s string) uint64 { return 1 }
			vec.hashAddByte = func(h uint64, b byte) uint64 { return 1 }
		}
		a := vec.WithLabelValues("a", "m")
		a.Add(3)
		vec.WithLabelValues("b", "m").Add(1)
		labels := collected(t, a) // carries the const label too
		if len(labels) != 3 {
			t.Fatalf("collected labels %v, want the const label among them", labels)
		}

		var seen float64
		if vec.DeleteCollectedIf(labels, func(m Metric) bool { seen = counterValue(t, m); return false }) {
			t.Fatalf("collide %v: deleted although the check said no", collide)
		}
		if seen != 3 {
			t.Fatalf("collide %v: the check was shown a metric holding %v, want the child holding 3", collide, seen)
		}
		if n := testutilChildren(vec); n != 2 {
			t.Fatalf("collide %v: %d children after a refused delete, want 2", collide, n)
		}
		if !vec.DeleteCollectedIf(labels, func(Metric) bool { return true }) {
			t.Fatalf("collide %v: not deleted although the check said yes", collide)
		}
		if n := testutilChildren(vec); n != 1 {
			t.Fatalf("collide %v: %d children after the delete, want 1", collide, n)
		}
		if vec.DeleteCollectedIf(labels, nil) {
			t.Fatalf("collide %v: deleted a child that is not there", collide)
		}
		if n := testutilChildren(vec); n != 1 {
			t.Fatalf("collide %v: %d children after deleting a missing one, want 1: it must not create one", collide, n)
		}
		if got := counterValue(t, vec.WithLabelValues("b", "m")); got != 1 {
			t.Fatalf("collide %v: the other child holds %v, want 1", collide, got)
		}
	}
}

// Labels a wrapping registerer adds are ignored like const labels; a variable
// label that is missing matches nothing.
func TestDeleteCollectedIfLabels(t *testing.T) {
	vec := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"key"})
	vec.WithLabelValues("a").Inc()
	pair := func(n, v string) *dto.LabelPair { return &dto.LabelPair{Name: &n, Value: &v} }

	if vec.DeleteCollectedIf([]*dto.LabelPair{pair("other", "a")}, nil) {
		t.Fatal("deleted with the variable label missing")
	}
	if !vec.DeleteCollectedIf([]*dto.LabelPair{pair("instance", "i1"), pair("key", "a")}, nil) {
		t.Fatal("not deleted with an extra label next to the variable one")
	}
}

// The values are taken as collected, already constrained: constraining them
// again would look for a different child.
func TestDeleteCollectedIfConstrained(t *testing.T) {
	vec := V2.NewCounterVec(CounterVecOpts{
		CounterOpts:    CounterOpts{Name: "c_total", Help: "h"},
		VariableLabels: ConstrainedLabels{{Name: "key", Constraint: func(s string) string { return "x" + s }}},
	})
	c := vec.WithLabelValues("a") // kept as "xa"
	c.Inc()
	if !vec.DeleteCollectedIf(collected(t, c), nil) {
		t.Fatal("the child was not found under the values it was collected with")
	}
}

// A curried vector deletes only within its curry.
func TestDeleteCollectedIfCurried(t *testing.T) {
	vec := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"key", "model"})
	in := vec.WithLabelValues("a", "m1")
	out := vec.WithLabelValues("a", "m2")
	in.Inc()
	out.Inc()
	curried := vec.MustCurryWith(Labels{"model": "m1"})
	if curried.DeleteCollectedIf(collected(t, out), nil) {
		t.Fatal("a curried vector deleted a child outside its curry")
	}
	if !curried.DeleteCollectedIf(collected(t, in), nil) {
		t.Fatal("a curried vector did not delete a child within its curry")
	}
	if n := testutilChildren(vec); n != 1 {
		t.Fatalf("%d children left, want 1", n)
	}
}

// A deleted child leaves change tracking, as with every other way of deleting.
func TestDeleteCollectedIfUntracks(t *testing.T) {
	trk := NewChangeTracker()
	SetChangeTracker(trk)
	defer SetChangeTracker(nil)
	vec := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"key"})
	c := vec.WithLabelValues("a")
	c.Inc()
	if !vec.DeleteCollectedIf(collected(t, c), nil) {
		t.Fatal("not deleted")
	}
	if TrackerSlot(c) != nil || !TrackedDeleted(c) {
		t.Fatal("the deleted child is still tracked as live")
	}
}

func testutilChildren(c Collector) int {
	ch := make(chan Metric, 64)
	c.Collect(ch)
	close(ch)
	return len(ch)
}
