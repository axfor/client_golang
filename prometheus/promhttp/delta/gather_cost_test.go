package delta_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The real ACG shape: 10 labels, UUID-length values, and enough histograms to
// reach the ~95 bucket series per key the cardinality analysis measured.
func TestACGGatherCostRealShape(t *testing.T) {
	const keys = 30000
	reg := prometheus.NewRegistry()

	labels := []string{
		"api_key", "api_key_name", "business_group", "business_group_id",
		"route_model", "route_model_id", "provider_model", "provider_model_id",
		"namespace", "job",
	}
	uuid := func(k, n int) string { return fmt.Sprintf("%08x-1234-4567-89ab-%012d", k, n) }

	var counters []*prometheus.CounterVec
	for i := 0; i < 28; i++ {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("acg_c%d_total", i)}, labels)
		reg.MustRegister(c)
		counters = append(counters, c)
	}
	var gauges []*prometheus.GaugeVec
	for i := 0; i < 3; i++ {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: fmt.Sprintf("acg_g%d", i)}, labels)
		reg.MustRegister(g)
		gauges = append(gauges, g)
	}
	buckets := []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000}
	var hists []*prometheus.HistogramVec
	for i := 0; i < 6; i++ {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: fmt.Sprintf("acg_h%d", i), Buckets: buckets}, labels)
		reg.MustRegister(h)
		hists = append(hists, h)
	}

	start := time.Now()
	for k := 0; k < keys; k++ {
		lv := []string{
			uuid(k, 1), "name-" + uuid(k, 2), uuid(k%50, 3), fmt.Sprintf("bg-%d", k%50),
			uuid(k%20, 4), fmt.Sprintf("rm-%d", k%20), uuid(k%10, 5), fmt.Sprintf("pm-%d", k%10),
			"acg-system", "model-router-metrics",
		}
		for _, c := range counters {
			c.WithLabelValues(lv...).Inc()
		}
		for _, g := range gauges {
			g.WithLabelValues(lv...).Set(1)
		}
		for _, h := range hists {
			h.WithLabelValues(lv...).Observe(123)
		}
	}
	inst := keys * (len(counters) + len(gauges) + len(hists))
	series := keys * (len(counters) + len(gauges) + len(hists)*(len(buckets)+3))
	t.Logf("建实例: %d 个实例 / 约 %d 条输出序列, 耗时 %v", inst, series, time.Since(start).Round(time.Millisecond))

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("静态持有: 存活堆 %d MiB", ms.HeapAlloc/1048576)

	for i := 1; i <= 3; i++ {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		before, gcBefore, pauseBefore := ms.Mallocs, ms.NumGC, ms.PauseTotalNs
		t0 := time.Now()
		mfs, err := reg.Gather()
		d := time.Since(t0)
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&ms)
		n := 0
		for _, mf := range mfs {
			n += len(mf.Metric)
		}
		t.Logf("第 %d 次 Gather(): %v, %d family / %d metric, %.2fM 次分配, 期间 GC %d 次 暂停 %v",
			i, d.Round(time.Millisecond), len(mfs), n, float64(ms.Mallocs-before)/1e6,
			ms.NumGC-gcBefore, time.Duration(ms.PauseTotalNs-pauseBefore).Round(time.Millisecond))
	}
}
