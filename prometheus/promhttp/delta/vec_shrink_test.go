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
	"reflect"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// The Vec holds its children in a map, and a Go map never returns its buckets:
// after idle cleanup has dropped most of a per-key metric's children, the array
// it needed at its widest is still there. Nothing reads wrong -- the children
// are gone and the counts are right -- so this looks at the map object itself.
//
// It lives here rather than in package prometheus because that package's tests
// do not build on every Go release (a generated runtime-metrics fixture), and
// this exercises the same thing through the public API anyway.
func TestVecChildMapIsRebuiltAfterIdleCleanup(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})

	const n = 1<<12 + 1000 // above what prometheus considers worth a copy
	for i := range n {
		f.c.WithLabelValues("k" + strconv.Itoa(i)).Inc()
	}
	f.scrape(t, false)
	before := vecMapPtr(t, f.c)

	for range 5 {
		f.c.WithLabelValues("k0").Inc()
		f.scrape(t, false)
	}
	if got := children(f.c); got != 1 {
		t.Fatalf("%d children left in the Vec, want 1", got)
	}
	if vecMapPtr(t, f.c) == before {
		t.Error("the Vec emptied its child map but kept the buckets it needed for every child that is gone")
	}

	// The survivor still has to work, and from the value it actually holds.
	f.c.WithLabelValues("k0").Add(2)
	if m, _ := f.scrape(t, false); m[`c_total{key=k0}`] != 2 {
		t.Fatalf("the surviving child lost its state across the rebuild: %v", m)
	}
}

// vecMapPtr reaches the unexported child map of a CounterVec, so a test can
// tell a rebuilt map from an emptied one.
func vecMapPtr(t *testing.T, c *prometheus.CounterVec) uintptr {
	t.Helper()
	v := reflect.ValueOf(c).Elem().FieldByName("MetricVec").Elem()
	mm := v.FieldByName("metricMap").Elem().FieldByName("metrics")
	return mm.Pointer()
}
