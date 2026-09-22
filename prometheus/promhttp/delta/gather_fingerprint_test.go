package delta_test

import (
	"runtime"
	"testing"
	"time"
)

// Reference values for telling, from the Go runtime metrics a process already
// exposes, how many instances a registry holds and whether a slow request was a
// Gather. Neither needs pprof nor a code change in the process under test.
func TestACGGatherFingerprint(t *testing.T) {
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	t.Logf("空注册表: heap_objects=%d, heap_alloc=%d MiB", ms.HeapObjects, ms.HeapAlloc/1048576)

	reg := buildReal(30000)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	t.Logf("110 万实例: heap_objects=%.1fM, heap_alloc=%d MiB",
		float64(ms.HeapObjects)/1e6, ms.HeapAlloc/1048576)

	// One Gather, measured the way an outside observer can: the deltas in the
	// counters behind go_memstats_mallocs_total and go_memstats_alloc_bytes_total.
	runtime.ReadMemStats(&ms)
	m0, a0 := ms.Mallocs, ms.TotalAlloc
	t0 := time.Now()
	if _, err := reg.Gather(); err != nil {
		t.Fatal(err)
	}
	d := time.Since(t0)
	runtime.ReadMemStats(&ms)
	t.Logf("一次 Gather: 耗时 %v, mallocs_total +%.2fM, alloc_bytes_total +%d MiB",
		d.Round(time.Millisecond), float64(ms.Mallocs-m0)/1e6, (ms.TotalAlloc-a0)/1048576)

	// The same deltas for a request that does not gather, as the control.
	runtime.ReadMemStats(&ms)
	m0, a0 = ms.Mallocs, ms.TotalAlloc
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&ms)
	t.Logf("对照(不 Gather 的 10ms): mallocs_total +%d, alloc_bytes_total +%d 字节",
		ms.Mallocs-m0, ms.TotalAlloc-a0)
}
