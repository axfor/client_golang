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

package prometheus

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// Writing repeatedly into the same dto.Metric has to give the same answers as
// writing into a fresh one every time. The in-place path is only taken on the
// second and later writes, so a fresh dto per write never exercises it.
func TestHistogramWriteIntoReusedMetric(t *testing.T) {
	reused := NewHistogram(HistogramOpts{Name: "h", Help: "h", Buckets: []float64{5, 10, 25}})
	fresh := NewHistogram(HistogramOpts{Name: "h", Help: "h", Buckets: []float64{5, 10, 25}})

	var out dto.Metric
	for round, vs := range [][]float64{{1, 7}, {30}, {}, {6, 6, 100}} {
		for _, v := range vs {
			reused.Observe(v)
			fresh.Observe(v)
		}
		if err := reused.Write(&out); err != nil {
			t.Fatal(err)
		}
		var want dto.Metric
		if err := fresh.Write(&want); err != nil {
			t.Fatal(err)
		}
		if out.Histogram.GetSampleCount() != want.Histogram.GetSampleCount() ||
			out.Histogram.GetSampleSum() != want.Histogram.GetSampleSum() {
			t.Fatalf("round %d: reused dto gives count=%d sum=%v, fresh gives count=%d sum=%v",
				round, out.Histogram.GetSampleCount(), out.Histogram.GetSampleSum(),
				want.Histogram.GetSampleCount(), want.Histogram.GetSampleSum())
		}
		for i := range want.Histogram.Bucket {
			if out.Histogram.Bucket[i].GetCumulativeCount() != want.Histogram.Bucket[i].GetCumulativeCount() ||
				out.Histogram.Bucket[i].GetUpperBound() != want.Histogram.Bucket[i].GetUpperBound() {
				t.Fatalf("round %d bucket %d: reused le=%v count=%d, fresh le=%v count=%d", round, i,
					out.Histogram.Bucket[i].GetUpperBound(), out.Histogram.Bucket[i].GetCumulativeCount(),
					want.Histogram.Bucket[i].GetUpperBound(), want.Histogram.Bucket[i].GetCumulativeCount())
			}
		}
	}
}
