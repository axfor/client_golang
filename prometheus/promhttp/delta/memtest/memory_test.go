// Package memtest measures what one metric instance costs a process that
// exposes through the change-tracking delta path. It is a package of its own
// because change tracking is switched on process-wide and has to be on before
// any metric exists.
package memtest

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp/delta"
)

// The shape of the model-router sidecar's per-key metrics: 26 counters and 7
// histograms over eight labels, four of them UUIDs, the latency histograms with
// 16 buckets and the others with 10.
func TestMemoryPerInstance(t *testing.T) {
	// About a minute and 2 GiB: a measurement to run on purpose, not a check for
	// every test run. MEMTEST=1 go test -run TestMemoryPerInstance -v
	if os.Getenv("MEMTEST") == "" {
		t.Skip("set MEMTEST=1 to measure; takes about a minute and 2 GiB")
	}
	trk := prometheus.EnableChangeTracking()
	reg := prometheus.NewRegistry()
	labels := []string{"apikey_id", "business_group", "business_group_id", "provider_id",
		"provider_model", "provider_model_id", "route_model", "route_model_id"}
	lat := []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000, 20000, 30000, 60000, 120000}
	def := []float64{16, 64, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}
	var counters []*prometheus.CounterVec
	for i := range 26 {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("acg_c%02d", i)}, labels)
		reg.MustRegister(v)
		counters = append(counters, v)
	}
	var hists []*prometheus.HistogramVec
	for i := range 7 {
		b := def
		if i < 3 {
			b = lat
		}
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: fmt.Sprintf("acg_h%d", i), Buckets: b}, labels)
		reg.MustRegister(v)
		hists = append(hists, v)
	}
	opts := delta.Increments()
	opts.TypeLabel = "_metric_type"
	exp := delta.NewTracked(trk, opts)

	uuid := func(kind string, i int) string {
		return fmt.Sprintf("%08x-%04x-5%03x-8%03x-%012x", i*2654435761%0xffffffff, len(kind), i%0xfff, (i/7)%0xfff, i)
	}
	const keys, combos = 12000, 4
	lvs := make([][]string, 0, keys*combos)
	for k := range keys {
		for c := range combos {
			lvs = append(lvs, []string{uuid("key", k), fmt.Sprintf("bg-%03d", k%50), uuid("bg", k%50),
				fmt.Sprintf("provider-%02d", c), fmt.Sprintf("provider-%02d-model-%d", c, c), uuid("model", c),
				fmt.Sprintf("model-%03d", k%20), uuid("route", k%20)})
		}
	}
	write := func(frac int) {
		for i, lv := range lvs {
			if i%frac != 0 {
				continue
			}
			for _, c := range counters {
				c.WithLabelValues(lv...).Add(3)
			}
			for _, h := range hists {
				h.WithLabelValues(lv...).Observe(float64(i % 3000))
			}
		}
	}
	scrape := func() {
		req := httptest.NewRequest(http.MethodGet, "/metrics/usage", nil)
		req.Header.Set("Accept-Encoding", "zstd")
		rec := httptest.NewRecorder()
		exp.Serve(rec, req)
		io.Copy(io.Discard, rec.Body)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Every instance written and delivered once -- the first scrape after
	// start-up, which reports all of them -- then steady-state rounds in which
	// half of them change. The exposer sizes what it keeps between scrapes to a
	// decaying high-water mark, so a couple of rounds would still be measuring
	// the start-up peak; twenty let it settle.
	write(1)
	scrape()
	for range 20 {
		write(2)
		scrape()
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	n := len(lvs) * (len(counters) + len(hists))
	t.Logf("%d instances (%d keys x %d combinations x %d families): %.0f bytes and %.2f heap objects per instance",
		n, keys, combos, len(counters)+len(hists),
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
	runtime.KeepAlive(lvs)
	runtime.KeepAlive(exp)
	runtime.KeepAlive(reg)
	runtime.KeepAlive(counters)
	runtime.KeepAlive(hists)
}
