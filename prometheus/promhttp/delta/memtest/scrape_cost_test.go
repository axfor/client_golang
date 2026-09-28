package memtest

import (
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"slices"
	"strconv"
	"testing"
	"time"
)

// What one scrape of the sidecar costs at production scale, with nothing else
// competing for the CPU: 1.58M instances, of which a rotating 1/N change
// between scrapes (N from MEMTEST_CHANGE_ONE_IN, default 4), so every instance
// changes every N scrapes and none goes idle.
//
// MEMTEST=1 go test -run TestScrapeCostAtScale -v
// MEMTEST_CPUPROFILE=<prefix> writes a CPU profile of each measured scrape, and
// of nothing else, to <prefix>.<n>; go tool pprof takes them all at once.
func TestScrapeCostAtScale(t *testing.T) {
	if os.Getenv("MEMTEST") == "" {
		t.Skip("set MEMTEST=1 to measure; takes a few minutes and 2 GiB")
	}
	every := 4
	if v, err := strconv.Atoi(os.Getenv("MEMTEST_CHANGE_ONE_IN")); err == nil && v > 0 {
		every = v
	}
	s := newSidecar(t)
	s.write(1, 0)
	s.scrape()
	round := 0
	for range 2 * every { // let the arrays sized by the first scrape settle
		s.write(every, round%every)
		s.scrape()
		round++
	}

	const measured = 10
	var took []time.Duration
	var allocs, wire int
	var allocBytes uint64
	var peakInuse uint64 // highest HeapInuse seen while a scrape ran, sampled every millisecond
	prefix := os.Getenv("MEMTEST_CPUPROFILE")
	for n := range measured {
		s.write(every, round%every)
		round++
		runtime.GC() // so a collection left over from the writes is not charged to the scrape

		var f *os.File
		if prefix != "" {
			var err error
			if f, err = os.Create(fmt.Sprintf("%s.%d", prefix, n)); err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal(err)
			}
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		stop := watchHeap()
		start := time.Now()
		wire = s.scrape()
		took = append(took, time.Since(start))
		peakInuse = max(peakInuse, stop())
		runtime.ReadMemStats(&after)
		allocBytes += after.TotalAlloc - before.TotalAlloc
		if f != nil {
			pprof.StopCPUProfile()
			f.Close()
		}
		allocs += int(after.Mallocs - before.Mallocs)
	}
	slices.Sort(took)
	var idle runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&idle)
	t.Logf("%d instances, 1/%d changing per scrape: median %v (min %v, max %v), %d allocs and %.1f MB on the wire per scrape",
		s.instances(), every, took[measured/2].Round(time.Millisecond), took[0].Round(time.Millisecond),
		took[measured-1].Round(time.Millisecond), allocs/measured, float64(wire)/1e6)
	t.Logf("allocated %.0f MiB per scrape; heap in use peaked at %.0f MiB during a scrape against %.0f MiB live between them",
		float64(allocBytes)/measured/(1<<20), float64(peakInuse)/(1<<20), float64(idle.HeapInuse)/(1<<20))
	runtime.KeepAlive(s)
}

// watchHeap samples the heap in use every millisecond until the returned stop
// is called, and stop returns the highest value seen. A scrape's transient
// allocation is what decides how far past the live heap a process goes during
// it, which the live heap after a collection does not show. runtime/metrics
// rather than ReadMemStats, which stops the world on every call.
func watchHeap() (stop func() uint64) {
	var high uint64
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		samples := []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
			{Name: "/memory/classes/heap/unused:bytes"},
		}
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			metrics.Read(samples)
			high = max(high, samples[0].Value.Uint64()+samples[1].Value.Uint64())
			select {
			case <-done:
				return
			case <-tick.C:
			}
		}
	}()
	return func() uint64 { close(done); <-finished; return high }
}
