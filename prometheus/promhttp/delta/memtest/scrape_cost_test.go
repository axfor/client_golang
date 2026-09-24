package memtest

import (
	"fmt"
	"os"
	"runtime"
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
		start := time.Now()
		wire = s.scrape()
		took = append(took, time.Since(start))
		runtime.ReadMemStats(&after)
		if f != nil {
			pprof.StopCPUProfile()
			f.Close()
		}
		allocs += int(after.Mallocs - before.Mallocs)
	}
	slices.Sort(took)
	t.Logf("%d instances, 1/%d changing per scrape: median %v (min %v, max %v), %d allocs and %.1f MB on the wire per scrape",
		s.instances(), every, took[measured/2].Round(time.Millisecond), took[0].Round(time.Millisecond),
		took[measured-1].Round(time.Millisecond), allocs/measured, float64(wire)/1e6)
	runtime.KeepAlive(s)
}
