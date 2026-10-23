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
	// A write to the deleted instance is not handed out.
	c.Inc()
	n := 0
	tr.Take(func(m Metric) {
		if m == c {
			n++
		}
	})
	if n != 0 {
		t.Fatal("a deleted instance was handed out")
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
