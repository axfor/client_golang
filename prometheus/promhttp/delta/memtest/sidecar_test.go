package memtest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp/delta"
)

// sidecar is the shape of the model-router sidecar's per-key metrics: 26
// counters and 7 histograms over eight labels, four of them UUIDs, the latency
// histograms with 16 buckets and the others with 10, exposed through the
// tracked delta path with the preset the sidecar uses.
type sidecar struct {
	exp      *delta.TrackedExposer
	reg      *prometheus.Registry
	counters []*prometheus.CounterVec
	hists    []*prometheus.HistogramVec
	lvs      [][]string
}

const sidecarKeys, sidecarCombos = 12000, 4

func newSidecar(t testing.TB) *sidecar {
	t.Helper()
	trk := prometheus.EnableChangeTracking()
	s := &sidecar{reg: prometheus.NewRegistry()}
	labels := []string{"apikey_id", "business_group", "business_group_id", "provider_id",
		"provider_model", "provider_model_id", "route_model", "route_model_id"}
	lat := []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000, 20000, 30000, 60000, 120000}
	def := []float64{16, 64, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}
	for i := range 26 {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("acg_c%02d", i)}, labels)
		s.reg.MustRegister(v)
		s.counters = append(s.counters, v)
	}
	for i := range 7 {
		b := def
		if i < 3 {
			b = lat
		}
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: fmt.Sprintf("acg_h%d", i), Buckets: b}, labels)
		s.reg.MustRegister(v)
		s.hists = append(s.hists, v)
	}
	opts := delta.Increments()
	opts.TypeLabel = "_metric_type"
	s.exp = delta.NewTracked(trk, opts)

	uuid := func(kind string, i int) string {
		return fmt.Sprintf("%08x-%04x-5%03x-8%03x-%012x", i*2654435761%0xffffffff, len(kind), i%0xfff, (i/7)%0xfff, i)
	}
	s.lvs = make([][]string, 0, sidecarKeys*sidecarCombos)
	for k := range sidecarKeys {
		for c := range sidecarCombos {
			s.lvs = append(s.lvs, []string{uuid("key", k), fmt.Sprintf("bg-%03d", k%50), uuid("bg", k%50),
				fmt.Sprintf("provider-%02d", c), fmt.Sprintf("provider-%02d-model-%d", c, c), uuid("model", c),
				fmt.Sprintf("model-%03d", k%20), uuid("route", k%20)})
		}
	}
	return s
}

// instances is how many metric instances the sidecar holds once every label
// combination has been written.
func (s *sidecar) instances() int { return len(s.lvs) * (len(s.counters) + len(s.hists)) }

// write writes every label combination i with i%every == phase, across every
// metric.
func (s *sidecar) write(every, phase int) {
	for i, lv := range s.lvs {
		if i%every != phase {
			continue
		}
		for _, c := range s.counters {
			c.WithLabelValues(lv...).Add(3)
		}
		for _, h := range s.hists {
			h.WithLabelValues(lv...).Observe(float64(i % 3000))
		}
	}
}

// scrape runs one scrape the way vmagent asks for it and returns its size on
// the wire. The response is counted and dropped, as a socket would take it:
// httptest.ResponseRecorder keeps the whole body in a growing buffer, and the
// copying that costs is a tenth of a scrape that production never pays.
func (s *sidecar) scrape() int {
	req := httptest.NewRequest(http.MethodGet, "/metrics/usage", nil)
	req.Header.Set("Accept-Encoding", "zstd")
	w := &countingWriter{h: http.Header{}}
	s.exp.Serve(w, req)
	return w.n
}

type countingWriter struct {
	h http.Header
	n int
}

func (w *countingWriter) Header() http.Header { return w.h }
func (w *countingWriter) WriteHeader(int)     {}
func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}
