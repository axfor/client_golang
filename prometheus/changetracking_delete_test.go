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
	"sync"
	"testing"
	"unsafe"

	dto "github.com/prometheus/client_model/go"
)

// Deleting an instance while requests write to it, and while the consumer takes
// and walks the tracker, has to be free of data races: run with -race. A write
// reads the instance's tracking state as the delete clears it.
func TestChangeTrackingDeleteRacesWrites(t *testing.T) {
	tr := NewChangeTracker()
	SetChangeTracker(tr)
	defer SetChangeTracker(nil)
	v := NewCounterVec(CounterOpts{Name: "c", Help: "c"}, []string{"k"})

	var mu sync.Mutex
	c := v.WithLabelValues("a")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				cc := c
				mu.Unlock()
				cc.Inc()
			}
		}()
	}
	for i := 0; i < 300; i++ {
		tr.Take(func(m Metric) { _ = TrackerSlot(m) })
		tr.RangeBucket(i%3, 3, func(m Metric) { _ = TrackedWritten(m) })
		if i%2 == 0 {
			DeleteTracked(v.WithLabelValues("a"))
		} else {
			DeleteTrackedIfUnwritten(v.WithLabelValues("a"))
		}
		mu.Lock()
		c = v.WithLabelValues("a")
		mu.Unlock()
	}
	close(stop)
	wg.Wait()
}

// DeleteTrackedIfUnwritten leaves an instance written since the tracker last
// handed it out, and removes one that was not.
func TestDeleteTrackedIfUnwritten(t *testing.T) {
	tr := NewChangeTracker()
	SetChangeTracker(tr)
	defer SetChangeTracker(nil)
	v := NewCounterVec(CounterOpts{Name: "c", Help: "c"}, []string{"k"})

	c := v.WithLabelValues("a")
	if !TrackedWritten(c) || DeleteTrackedIfUnwritten(c) {
		t.Fatal("a new instance, not taken yet, was deleted")
	}
	tr.Take(func(Metric) {})
	c.Inc()
	if !TrackedWritten(c) || DeleteTrackedIfUnwritten(c) {
		t.Fatal("an instance written after it was taken was deleted")
	}
	tr.Take(func(Metric) {})
	if TrackedWritten(c) || !DeleteTrackedIfUnwritten(c) {
		t.Fatal("an instance not written since it was taken was kept")
	}
	if TrackerSlot(c) != nil || TrackedWritten(c) || DeleteTrackedIfUnwritten(c) {
		t.Fatal("a deleted instance still looks tracked")
	}
	if v.WithLabelValues("a") == c {
		t.Fatal("the deleted instance is still in its Vec")
	}
	// A write to the deleted instance, by a caller that looked it up before the
	// deletion, is not lost: the instance is handed out, deleted, for the
	// consumer to settle. With a newer instance under its labels, it stays
	// deleted and ReviveTracked names that one, to move its writes into.
	c.Inc()
	n := 0
	tr.Take(func(m Metric) {
		if m == c {
			n++
		}
	})
	if n != 1 || !TrackedDeleted(c) {
		t.Fatalf("a deleted instance written since was handed out %d times, deleted %v; want once, deleted", n, TrackedDeleted(c))
	}
	if now, slot := ReviveTracked(c); now == c || now != v.WithLabelValues("a") || slot == nil {
		t.Fatal("with a newer instance under its labels, the deleted one was put back, or the newer one not named")
	}
}

// A deleted instance written since, with nothing in its Vec under its labels, is
// put back as it was: its tracking, its slot and its value.
func TestReviveTracked(t *testing.T) {
	tr := NewChangeTracker()
	SetChangeTracker(tr)
	defer SetChangeTracker(nil)
	v := NewCounterVec(CounterOpts{Name: "c", Help: "c"}, []string{"k", "l"})
	c := v.WithLabelValues("a", "b")
	c.Add(3)
	slot := TrackerSlot(c)
	marker := new(int)
	*slot = unsafe.Pointer(marker)
	tr.Take(func(Metric) {})
	if !DeleteTrackedIfUnwritten(c) {
		t.Fatal("not deleted")
	}
	c.Add(5)
	tr.Cut()
	var taken []Metric
	tr.Take(func(m Metric) { taken = append(taken, m) })
	if len(taken) != 1 || taken[0] != c {
		t.Fatalf("taken %v; want the deleted instance", taken)
	}
	now, s := ReviveTracked(c)
	if now != c || s == nil || *s != unsafe.Pointer(marker) {
		t.Fatal("not put back with its slot")
	}
	if TrackedDeleted(c) || TrackerSlot(c) == nil || v.WithLabelValues("a", "b") != c {
		t.Fatal("put back, but not tracked or not in its Vec")
	}
	if got, _ := CutValue(c); got != 8 {
		t.Fatalf("CutValue %v; want 8, the 3 and the 5 written after the deletion", got)
	}
	n := 0
	tr.RangeBucket(0, 1, func(m Metric) {
		if m == c {
			n++
		}
	})
	if n != 1 {
		t.Fatalf("the instance put back is walked %d times; want once", n)
	}
}

// What a deleted instance had not delivered goes into the newer one: a counter
// as of the cut, a classic histogram as observations.
func TestAddTrackedCutAndHistogram(t *testing.T) {
	tr := NewChangeTracker()
	SetChangeTracker(tr)
	defer SetChangeTracker(nil)
	cv := NewCounterVec(CounterOpts{Name: "c", Help: "c"}, []string{"k"})
	c := cv.WithLabelValues("a")
	c.Add(2)
	tr.Cut()
	tr.Take(func(Metric) {})
	if !AddTrackedCut(c, 1.5) || AddTrackedCut(c, -1) {
		t.Fatal("AddTrackedCut refused a counter, or took a negative value")
	}
	var m dto.Metric
	_ = c.Write(&m)
	if got, _ := CutValue(c); got != 3.5 || m.GetCounter().GetValue() != 3.5 || !TrackedWritten(c) {
		t.Fatalf("CutValue %v, value %v, written %v; want 3.5, 3.5, true", got, m.GetCounter().GetValue(), TrackedWritten(c))
	}

	hv := NewHistogramVec(HistogramOpts{Name: "h", Help: "h", Buckets: []float64{1, 10}}, []string{"k"})
	h := hv.WithLabelValues("a").(Histogram)
	h.Observe(20)
	if !AddTrackedHistogram(h.(Metric), []uint64{2, 3}, 4, 30.5) {
		t.Fatal("AddTrackedHistogram refused a classic histogram")
	}
	m.Reset()
	_ = h.(Metric).Write(&m)
	hh := m.GetHistogram()
	if hh.GetSampleCount() != 5 || hh.GetSampleSum() != 50.5 || hh.Bucket[0].GetCumulativeCount() != 2 || hh.Bucket[1].GetCumulativeCount() != 3 {
		t.Fatalf("histogram after the add: %v", hh)
	}
	if AddTrackedHistogram(c, []uint64{1}, 1, 1) {
		t.Fatal("AddTrackedHistogram took a counter")
	}
}

// Describes tells the Descs of a registry's collectors from those of others,
// and follows Unregister.
func TestRegistryDescribes(t *testing.T) {
	r := NewRegistry()
	in := NewCounterVec(CounterOpts{Name: "in_total", Help: "h"}, []string{"k"})
	out := NewCounterVec(CounterOpts{Name: "out_total", Help: "h"}, []string{"k"})
	r.MustRegister(in)
	if !r.Describes(in.WithLabelValues("a").Desc()) || r.Describes(out.WithLabelValues("a").Desc()) || r.Describes(nil) {
		t.Fatal("Describes does not tell the registered collector from the other")
	}
	r.Unregister(in)
	if r.Describes(in.WithLabelValues("a").Desc()) {
		t.Fatal("Describes still reports an unregistered collector")
	}
}
