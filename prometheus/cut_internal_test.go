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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// cutWrite starts a write of 5 to a fresh tracked counter and holds it at step
// of the write, see countHook, until the returned release is called.
func cutWrite(t *testing.T, step int) (*ChangeTracker, Counter, func(), chan struct{}) {
	t.Helper()
	trk := NewChangeTracker()
	SetChangeTracker(trk)
	t.Cleanup(func() { SetChangeTracker(nil) })
	c := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"k"}).WithLabelValues("a")
	held, release := make(chan struct{}), make(chan struct{})
	once := false
	countHook = func(s int) {
		if s == step && !once {
			once = true
			close(held)
			<-release
		}
	}
	t.Cleanup(func() { countHook = nil })
	done := make(chan struct{})
	go func() { c.Add(5); close(done) }()
	<-held
	return trk, c, func() { close(release) }, done
}

// A write that read the cut and is not yet registered when the cut moves on
// goes to the new side: it is reported at the next cut, not left on the old side
// for the cut after, by when later writes would have been reported before it.
func TestCutWriteBeforeRegisteringGoesAfter(t *testing.T) {
	trk, c, release, done := cutWrite(t, 0)
	trk.Cut()
	trk.Take(func(Metric) {})
	if v, _ := CutValue(c); v != 0 {
		t.Fatalf("at the cut taken before the write: %v, want 0", v)
	}
	release()
	<-done
	trk.Cut()
	trk.Take(func(Metric) {})
	if v, _ := CutValue(c); v != 5 {
		t.Fatalf("at the next cut: %v, want 5", v)
	}
}

// A cut waits for a write registered on its side, until it has added its value
// and enlisted the instance, and reports it.
func TestCutWaitsForRegisteredWrite(t *testing.T) {
	for _, step := range []int{1, 2} {
		t.Run(fmt.Sprint("step=", step), func(t *testing.T) { cutWaitsAt(t, step) })
	}
}

func cutWaitsAt(t *testing.T, step int) {
	trk, c, release, done := cutWrite(t, step)
	cut := make(chan uint64)
	go func() { cut <- trk.Cut() }()
	select {
	case <-cut:
		t.Fatal("the cut went ahead of a write registered before it")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	<-done
	<-cut
	trk.Take(func(Metric) {})
	if v, _ := CutValue(c); v != 5 {
		t.Fatalf("at the cut: %v, want 5, the write it waited for", v)
	}
}

// A write never waits for a cut, nor for a scrape reading its values.
func TestCutNeverHoldsWrites(t *testing.T) {
	trk := NewChangeTracker()
	SetChangeTracker(trk)
	defer SetChangeTracker(nil)
	c := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"k"}).WithLabelValues("a")
	trk.Cut()
	trk.Take(func(Metric) {})
	// a consumer in the middle of reading, as it is for the whole of a scrape
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			c.Inc()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writes waited")
	}
	if v, _ := CutValue(c); v != 0 {
		t.Fatalf("at a cut taken before them: %v, want 0", v)
	}
	trk.Cut()
	trk.Take(func(Metric) {})
	if v, _ := CutValue(c); v != 1000 {
		t.Fatalf("at the next cut: %v, want 1000", v)
	}
}

// However fast counters are written, a cut waits only for the writes in
// progress when it moves on, so it always ends: its wait does not grow with the
// rate of writes. With more writers than CPUs one is often preempted in the
// middle of a write, and the cut then waits for it to be scheduled again, a time
// slice, tens of milliseconds. Nothing written is lost.
func TestCutUnderWriteFlood(t *testing.T) {
	for _, spread := range []int{1, 1000} {
		t.Run(fmt.Sprint("counters=", spread), func(t *testing.T) {
			trk := NewChangeTracker()
			SetChangeTracker(trk)
			defer SetChangeTracker(nil)
			vec := NewCounterVec(CounterOpts{Name: "c_total", Help: "h"}, []string{"k"})
			cs := make([]Counter, spread)
			for i := range cs {
				cs[i] = vec.WithLabelValues(fmt.Sprint(i))
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var written atomic.Int64
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					n := int64(0)
					for i := w; ; i++ {
						select {
						case <-stop:
							written.Add(n)
							return
						default:
						}
						cs[i%spread].Inc()
						n++
					}
				}(w)
			}
			var got float64
			take := func() { trk.Take(func(Metric) {}) }
			var worst, total time.Duration
			const cuts = 200
			for i := 0; i < cuts; i++ {
				t0 := time.Now()
				trk.Cut()
				d := time.Since(t0)
				worst, total = max(worst, d), total+d
				take()
			}
			close(stop)
			wg.Wait()
			trk.Cut()
			take()
			for _, c := range cs {
				v, _ := CutValue(c)
				got += v
			}
			if got != float64(written.Load()) {
				t.Fatalf("reported %v of %v writes", got, written.Load())
			}
			t.Logf("%d cuts under %d writes: mean %v, worst %v", cuts, written.Load(), total/cuts, worst)
			if worst > 10*time.Second {
				t.Fatalf("a cut took %v", worst)
			}
		})
	}
}
