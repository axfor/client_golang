// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp/delta"
)

// value scrapes h at target and returns the value of name{key="a"}, or NaN-free
// zero with ok=false when the series is absent.
func value(t *testing.T, h http.Handler, target, name string) (float64, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d", target, rec.Code)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := parser.TextToMetricFamilies(strings.NewReader(rec.Body.String()))
	if err != nil {
		t.Fatalf("%s: cannot parse response: %s\n%s", target, err, rec.Body.String())
	}
	mf, ok := mfs[name]
	if !ok {
		return 0, false
	}
	return mf.Metric[0].GetCounter().GetValue(), true
}

// With the switch off, ?delta=1 is an ordinary scrape: nothing is subtracted and
// nothing is left out.
func TestDeltaEndpointOffByDefault(t *testing.T) {
	if delta.Enabled() {
		t.Fatal("delta exposition should be off by default")
	}
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "delta_off_total", Help: "h"}, []string{"key"})
	reg.MustRegister(c)
	c.WithLabelValues("a").Add(3)
	h := HandlerFor(reg, HandlerOpts{})

	if v, ok := value(t, h, "/metrics?delta=1", "delta_off_total"); !ok || v != 3 {
		t.Fatalf("first scrape should report the cumulative 3, got %v (present %v)", v, ok)
	}
	c.WithLabelValues("a").Add(2)
	if v, ok := value(t, h, "/metrics?delta=1", "delta_off_total"); !ok || v != 5 {
		t.Fatalf("second scrape should report the cumulative 5, got %v (present %v)", v, ok)
	}
}

// One call to Enable is the whole setup: the handlers promhttp already hands out
// answer ?delta=1 with increments, while plain /metrics keeps reporting
// cumulative values and does not consume them.
func TestDeltaEndpointEnabled(t *testing.T) {
	delta.Enable(delta.Options{ReportIncrements: true, DisableHeartbeat: true})
	t.Cleanup(func() {
		delta.Disable()
		prometheus.SetChangeTracker(nil)
	})

	// Created after Enable, so change tracking sees them.
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "delta_on_total", Help: "h"}, []string{"key"})
	reg.MustRegister(c)
	h := HandlerFor(reg, HandlerOpts{})

	c.WithLabelValues("a").Add(3)
	if v, ok := value(t, h, "/metrics?delta=1", "delta_on_total"); !ok || v != 3 {
		t.Fatalf("first scrape should report 3, got %v (present %v)", v, ok)
	}
	c.WithLabelValues("a").Add(2)
	if v, ok := value(t, h, "/metrics?delta=1", "delta_on_total"); !ok || v != 2 {
		t.Fatalf("second scrape should report the increment 2, got %v (present %v)", v, ok)
	}
	if _, ok := value(t, h, "/metrics?delta=1", "delta_on_total"); ok {
		t.Fatal("a scrape with no writes in between should leave the series out")
	}

	// Plain /metrics is untouched: still cumulative, and it does not eat the
	// increments a delta scraper is waiting for.
	if v, ok := value(t, h, "/metrics", "delta_on_total"); !ok || v != 5 {
		t.Fatalf("/metrics should report the cumulative 5, got %v (present %v)", v, ok)
	}
	c.WithLabelValues("a").Add(4)
	if v, ok := value(t, h, "/metrics?delta=1", "delta_on_total"); !ok || v != 4 {
		t.Fatalf("the plain scrape must not consume increments, got %v (present %v)", v, ok)
	}
}
