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

package delta

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// scopeFixture is the setup app-side.md describes: one tracker for the whole
// process, framework metrics in one registry and usage metrics in another.
type scopeFixture struct {
	exp       *TrackedExposer
	framework *prometheus.Registry
	fw        *prometheus.CounterVec
	usage     *prometheus.CounterVec
}

func newScopeFixture(t *testing.T, scoped bool) *scopeFixture {
	trk := prometheus.NewChangeTracker()
	prometheus.SetChangeTracker(trk)
	t.Cleanup(func() { prometheus.SetChangeTracker(nil) })
	f := &scopeFixture{framework: prometheus.NewRegistry()}
	usageReg := prometheus.NewRegistry()
	f.fw = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "framework_requests_total", Help: "h"}, []string{"route"})
	f.framework.MustRegister(f.fw)
	f.usage = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "acg_requests_total", Help: "h"}, []string{"key"})
	usageReg.MustRegister(f.usage)
	opts := Increments()
	if scoped {
		opts.Scope = usageReg
	}
	f.exp = NewTracked(trk, opts)
	return f
}

func (f *scopeFixture) scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics/usage", nil))
	return rec.Body.String()
}

func (f *scopeFixture) frameworkValue(t *testing.T, route string) string {
	t.Helper()
	mfs, err := f.framework.Gather()
	if err != nil {
		t.Fatalf("cannot gather: %s", err)
	}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			for _, l := range m.Label {
				if l.GetName() == "route" && l.GetValue() == route {
					return strconv.FormatFloat(m.GetCounter().GetValue(), 'f', -1, 64)
				}
			}
		}
	}
	return ""
}

// With Scope set, the metrics of other registries are neither reported nor
// deleted as idle: a framework counter quiet for longer than IdleScrapes stays
// in its Vec, and a child some code keeps goes on counting.
func TestTrackedScopeLeavesOtherRegistriesAlone(t *testing.T) {
	f := newScopeFixture(t, true)
	kept := f.fw.WithLabelValues("/chat") // held, as package-level vars often are
	kept.Add(10)
	f.usage.WithLabelValues("quiet").Inc()
	for i := 0; i < 40; i++ {
		f.usage.WithLabelValues("busy").Inc()
		if body := f.scrape(t); strings.Contains(body, "framework_requests_total") {
			t.Fatalf("the usage endpoint carries a metric of another registry:\n%s", body)
		}
	}
	kept.Add(5)
	if got := f.frameworkValue(t, "/chat"); got != "15" {
		t.Fatalf("the framework counter reads %q after a quiet spell, want 15", got)
	}
	// Usage metrics are still cleaned up when idle: the quiet key is gone,
	// the busy one is still there.
	if n := f.exp.Tracked(); n != 1 {
		t.Fatalf("tracking %d instances, want only the busy usage key", n)
	}
}

// Without Scope everything the tracker sees is in, as before: the framework
// counter is reported and, once idle, deleted.
func TestTrackedWithoutScopeTakesEverything(t *testing.T) {
	f := newScopeFixture(t, false)
	f.fw.WithLabelValues("/chat").Add(10)
	if body := f.scrape(t); !strings.Contains(body, "framework_requests_total") {
		t.Fatalf("without Scope the framework counter is not reported:\n%s", body)
	}
	for i := 0; i < 40; i++ {
		f.usage.WithLabelValues("busy").Inc()
		f.scrape(t)
	}
	if got := f.frameworkValue(t, "/chat"); got != "" {
		t.Fatalf("without Scope the idle framework counter was kept, reading %s", got)
	}
}

// A usage Vec registered after it was first written loses nothing: what was
// written before is reported with the first write after registration.
func TestTrackedScopeRegisteredLater(t *testing.T) {
	trk := prometheus.NewChangeTracker()
	prometheus.SetChangeTracker(trk)
	defer prometheus.SetChangeTracker(nil)
	usageReg := prometheus.NewRegistry()
	opts := Increments()
	opts.Scope = usageReg
	exp := NewTracked(trk, opts)
	late := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "acg_late_total", Help: "h"}, []string{"key"})
	late.WithLabelValues("a").Add(4)
	rec := httptest.NewRecorder()
	exp.Serve(rec, httptest.NewRequest("GET", "/", nil))
	if strings.Contains(rec.Body.String(), "acg_late_total") {
		t.Fatalf("reported before it was registered with the scope:\n%s", rec.Body.String())
	}
	usageReg.MustRegister(late)
	late.WithLabelValues("a").Add(1)
	rec = httptest.NewRecorder()
	exp.Serve(rec, httptest.NewRequest("GET", "/", nil))
	if got := parseCounters(t, rec.Body.String())[`acg_late{key="a"}`]; got != 5 {
		t.Fatalf("reported %v after registration, want 5:\n%s", got, rec.Body.String())
	}
}
