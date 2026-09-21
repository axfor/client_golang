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

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// scrapeBuckets serves one scrape and returns, per key, the cumulative count of
// each bucket. The shared fixture's parser keeps only a histogram's sum and
// count, and the buckets are the whole point here.
func scrapeBuckets(t *testing.T, e *TrackedExposer) map[string][]uint64 {
	t.Helper()
	rec := httptest.NewRecorder()
	e.Serve(rec, httptest.NewRequest("GET", "/metrics/delta", nil))

	parser := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := parser.TextToMetricFamilies(strings.NewReader(rec.Body.String()))
	if err != nil {
		t.Fatalf("cannot parse response: %s\n%s", err, rec.Body.String())
	}
	out := map[string][]uint64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			if m.Histogram == nil {
				continue
			}
			var key string
			for _, l := range m.Label {
				if l.GetName() == "key" {
					key = l.GetValue()
				}
			}
			counts := make([]uint64, 0, len(m.Histogram.Bucket))
			for _, b := range m.Histogram.Bucket {
				counts = append(counts, b.GetCumulativeCount())
			}
			out[key] = counts
		}
	}
	return out
}

// A histogram's buckets are cloned into arrays shared by the whole histogram
// rather than one object per bucket. Those dtos are then lent out and written
// into again on later scrapes, so what has to hold is that two instances of a
// family never read each other's bucket counts -- a bug that would put one
// key's latency distribution under another key's labels, with the sum and
// count still right so nothing looks wrong.
//
// The fixture's bounds are 5, 10, 25, 50, 100, 250, plus the +Inf bucket.
func TestHistogramBucketsDoNotBleedBetweenInstances(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, HeartbeatScrapes: 1})

	f.h.WithLabelValues("a").Observe(1)   // in every bucket from le=5 up
	f.h.WithLabelValues("b").Observe(30)  // from le=50 up
	f.h.WithLabelValues("c").Observe(500) // in none of them
	got := scrapeBuckets(t, f.exp)

	for key, want := range map[string][]uint64{
		"a": {1, 1, 1, 1, 1, 1, 1},
		"b": {0, 0, 0, 1, 1, 1, 1},
		"c": {0, 0, 0, 0, 0, 0, 1},
	} {
		if !equalCounts(got[key], want) {
			t.Errorf("key %s buckets = %v, want %v -- a bucket array was shared across instances", key, got[key], want)
		}
	}

	// Again, so the dtos come back from the free list and are written into in
	// place. A shared array would show here even if the first scrape looked fine.
	f.h.WithLabelValues("c").Observe(1)
	got2 := scrapeBuckets(t, f.exp)
	if want := []uint64{1, 1, 1, 1, 1, 1, 1}; !equalCounts(got2["c"], want) {
		t.Errorf("after c observed 1 its buckets = %v, want %v", got2["c"], want)
	}
	if _, ok := got2["a"]; ok {
		t.Errorf("key a was not written this scrape but still appears with buckets %v", got2["a"])
	}
}

// The same with enough instances and scrapes that the free list rotates and an
// array handed to one instance is later handed to another.
func TestHistogramBucketsSurviveTheFreeListRotating(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, HeartbeatScrapes: 1})

	const n = 200
	want := map[int][]uint64{
		0: {1, 1, 1, 1, 1, 1, 1}, // observed 1
		1: {0, 0, 0, 1, 1, 1, 1}, // observed 30
		2: {0, 0, 0, 0, 0, 0, 1}, // observed 500, only the +Inf bucket
	}
	values := map[int]float64{0: 1, 1: 30, 2: 500}

	for round := 1; round <= 4; round++ {
		for i := range n {
			f.h.WithLabelValues("k" + strconv.Itoa(i)).Observe(values[i%3])
		}
		got := scrapeBuckets(t, f.exp)
		for i := range n {
			k := "k" + strconv.Itoa(i)
			if !equalCounts(got[k], want[i%3]) {
				t.Fatalf("round %d, %s: buckets = %v, want %v -- a bucket array was reused across instances",
					round, k, got[k], want[i%3])
			}
		}
	}
}

func equalCounts(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
