package memtest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// What one request costs the sidecar on its own path: an update to each of the
// 33 per-key metrics for a label combination that already exists, the steady
// state of a key in use. Change tracking is on, as it is in the sidecar.
func BenchmarkRequestPath(b *testing.B) {
	s := newSidecar(b)
	names := []string{"apikey_id", "business_group", "business_group_id", "provider_id",
		"provider_model", "provider_model_id", "route_model", "route_model_id"}
	const combos = 1000
	maps := make([]prometheus.Labels, combos)
	for i := range combos {
		maps[i] = prometheus.Labels{}
		for j, n := range names {
			maps[i][n] = s.lvs[i][j]
		}
	}
	s.write(len(s.lvs)/combos, 0) // create the children first

	b.Run("WithLabelValues", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			lv := s.lvs[i%combos]
			for _, c := range s.counters {
				c.WithLabelValues(lv...).Add(3)
			}
			for _, h := range s.hists {
				h.WithLabelValues(lv...).Observe(float64(i % 3000))
			}
		}
	})
	b.Run("With", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			l := maps[i%combos]
			for _, c := range s.counters {
				c.With(l).Add(3)
			}
			for _, h := range s.hists {
				h.With(l).Observe(float64(i % 3000))
			}
		}
	})
}
