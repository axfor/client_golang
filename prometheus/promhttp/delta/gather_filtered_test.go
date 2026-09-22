package delta_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

// A registry holding both the per-key metrics and a handful of framework ones,
// which is the shape that makes filtering after Gather so expensive: the
// framework endpoint wants the handful and pays for the millions.
func buildMixed(keys int) *prometheus.Registry {
	reg := buildReal(keys)
	for i := 0; i < 8; i++ {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: fmt.Sprintf("model_router_f%d", i)})
		reg.MustRegister(g)
		g.Set(float64(i))
	}
	return reg
}

func framework(name string) bool { return strings.HasPrefix(name, "model_router_") }

// A collector reporting more than one name, only some of them kept. Collector
// level filtering cannot express this: the collector has to be walked, and the
// metrics it yields filtered one by one.
type twoNamed struct{ kept, dropped *prometheus.Desc }

func newTwoNamed() *twoNamed {
	return &twoNamed{
		kept:    prometheus.NewDesc("model_router_paired", "kept half", nil, nil),
		dropped: prometheus.NewDesc("acg_paired_total", "dropped half", nil, nil),
	}
}

func (c *twoNamed) Describe(ch chan<- *prometheus.Desc) { ch <- c.kept; ch <- c.dropped }
func (c *twoNamed) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.kept, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, 2)
}

func TestGatherFilteredMatchesFilteringAfterGather(t *testing.T) {
	reg := buildMixed(200)

	got, err := reg.GatherFiltered(framework)
	if err != nil {
		t.Fatal(err)
	}
	all, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var want []*dto.MetricFamily
	for _, mf := range all {
		if framework(mf.GetName()) {
			want = append(want, mf)
		}
	}

	if len(got) != len(want) {
		t.Fatalf("families: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].GetName() != want[i].GetName() {
			t.Fatalf("family %d: got %q, want %q", i, got[i].GetName(), want[i].GetName())
		}
		if len(got[i].Metric) != len(want[i].Metric) {
			t.Fatalf("%s: got %d metrics, want %d", got[i].GetName(), len(got[i].Metric), len(want[i].Metric))
		}
		if got[i].Metric[0].GetGauge().GetValue() != want[i].Metric[0].GetGauge().GetValue() {
			t.Fatalf("%s: value differs", got[i].GetName())
		}
	}

	// The filter has to be able to reach the per-key families too, otherwise
	// this is only testing that it drops everything.
	perKey, err := reg.GatherFiltered(func(n string) bool { return n == "acg_c0_total" })
	if err != nil {
		t.Fatal(err)
	}
	if len(perKey) != 1 || perKey[0].GetName() != "acg_c0_total" || len(perKey[0].Metric) != 200 {
		t.Fatalf("per-key filter: got %d families", len(perKey))
	}

	// A collector reporting both a kept and a dropped name must yield only the
	// kept one. Skipping whole collectors cannot do this on its own.
	mixed := prometheus.NewRegistry()
	mixed.MustRegister(newTwoNamed())
	paired, err := mixed.GatherFiltered(framework)
	if err != nil {
		t.Fatal(err)
	}
	if len(paired) != 1 {
		t.Fatalf("paired collector: got %d families, want 1", len(paired))
	}
	if n := paired[0].GetName(); n != "model_router_paired" {
		t.Fatalf("paired collector: got %q, want model_router_paired", n)
	}

	// Filtering everything out must not be mistaken for an empty registry.
	none, err := reg.GatherFiltered(func(string) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Fatalf("empty filter: got %d families", len(none))
	}
}

func TestGatherFilteredCost(t *testing.T) {
	reg := buildMixed(30000)

	for i := 1; i <= 2; i++ {
		t0 := time.Now()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, mf := range mfs {
			if framework(mf.GetName()) {
				n += len(mf.Metric)
			}
		}
		t.Logf("Gather() 全量再过滤: %v  → 留下 %d 条", time.Since(t0).Round(time.Millisecond), n)
	}
	for i := 1; i <= 2; i++ {
		t0 := time.Now()
		mfs, err := reg.GatherFiltered(framework)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, mf := range mfs {
			n += len(mf.Metric)
		}
		t.Logf("GatherFiltered():         %v  → 留下 %d 条", time.Since(t0).Round(time.Microsecond), n)
	}
}
