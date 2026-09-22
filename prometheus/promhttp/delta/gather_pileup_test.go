package delta_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func buildReal(keys int) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	labels := []string{
		"api_key", "api_key_name", "business_group", "business_group_id",
		"route_model", "route_model_id", "provider_model", "provider_model_id",
		"namespace", "job",
	}
	uuid := func(k, n int) string { return fmt.Sprintf("%08x-1234-4567-89ab-%012d", k, n) }
	var cs []*prometheus.CounterVec
	for i := 0; i < 28; i++ {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("acg_c%d_total", i)}, labels)
		reg.MustRegister(c)
		cs = append(cs, c)
	}
	var gs []*prometheus.GaugeVec
	for i := 0; i < 3; i++ {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: fmt.Sprintf("acg_g%d", i)}, labels)
		reg.MustRegister(g)
		gs = append(gs, g)
	}
	bk := []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000}
	var hs []*prometheus.HistogramVec
	for i := 0; i < 6; i++ {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: fmt.Sprintf("acg_h%d", i), Buckets: bk}, labels)
		reg.MustRegister(h)
		hs = append(hs, h)
	}
	for k := 0; k < keys; k++ {
		lv := []string{
			uuid(k, 1), "name-" + uuid(k, 2), uuid(k%50, 3), fmt.Sprintf("bg-%d", k%50),
			uuid(k%20, 4), fmt.Sprintf("rm-%d", k%20), uuid(k%10, 5), fmt.Sprintf("pm-%d", k%10),
			"acg-system", "model-router-metrics",
		}
		for _, c := range cs {
			c.WithLabelValues(lv...).Inc()
		}
		for _, g := range gs {
			g.WithLabelValues(lv...).Set(1)
		}
		for _, h := range hs {
			h.WithLabelValues(lv...).Observe(123)
		}
	}
	return reg
}

// 抓取间隔小于 Gather 耗时会让 Gather 叠在一起跑。每个 Gather 都要 churn 几百 MiB,
// 叠起来把 GC 压垮,于是每个都变得更慢 —— 越堆越慢,自我放大。
func TestACGGatherPileup(t *testing.T) {
	reg := buildReal(30000)
	for _, n := range []int{1, 2, 3, 4} {
		var wg sync.WaitGroup
		durs := make([]time.Duration, n)
		t0 := time.Now()
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				s := time.Now()
				if _, err := reg.Gather(); err != nil {
					t.Error(err)
				}
				durs[i] = time.Since(s)
			}(i)
		}
		wg.Wait()
		var mx time.Duration
		for _, d := range durs {
			if d > mx {
				mx = d
			}
		}
		t.Logf("%d 个 Gather 并发: 墙钟 %v, 单个最慢 %v", n, time.Since(t0).Round(time.Millisecond), mx.Round(time.Millisecond))
	}
}
