// Package memtest measures what one metric instance costs a process that
// exposes through the change-tracking delta path. It is a package of its own
// because change tracking is switched on process-wide and has to be on before
// any metric exists.
package memtest

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
)

// What one instance of the sidecar's per-key metrics costs in live heap, see
// newSidecar for their shape.
func TestMemoryPerInstance(t *testing.T) {
	// About a minute and 2 GiB: a measurement to run on purpose, not a check for
	// every test run. MEMTEST=1 go test -run TestMemoryPerInstance -v
	if os.Getenv("MEMTEST") == "" {
		t.Skip("set MEMTEST=1 to measure; takes about a minute and 2 GiB")
	}
	s := newSidecar(t)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Every instance written and delivered once -- the first scrape after
	// start-up, which reports all of them -- then steady-state rounds in which
	// half of them change. The exposer sizes what it keeps between scrapes to a
	// decaying high-water mark, so a couple of rounds would still be measuring
	// the start-up peak; twenty let it settle.
	s.write(1, 0)
	s.scrape()
	for range 20 {
		s.write(2, 0)
		s.scrape()
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	n := s.instances()
	t.Logf("%d instances (%d keys x %d combinations x %d families): %.0f bytes and %.2f heap objects per instance",
		n, sidecarKeys, sidecarCombos, len(s.counters)+len(s.hists),
		float64(after.HeapAlloc-before.HeapAlloc)/float64(n), float64(after.HeapObjects-before.HeapObjects)/float64(n))
	// MEMTEST_HEAPPROFILE=<file> writes the heap here, while everything above is
	// still reachable; -memprofile is only written after the test returns, when
	// the exposer is already garbage.
	if path := os.Getenv("MEMTEST_HEAPPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	runtime.KeepAlive(s)
}
