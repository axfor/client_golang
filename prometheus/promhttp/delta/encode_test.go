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
	"bufio"
	"bytes"
	"math"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
)

func lp(name, value string) *dto.LabelPair {
	return &dto.LabelPair{Name: proto.String(name), Value: proto.String(value)}
}

func bucket(le float64, count uint64) *dto.Bucket {
	return &dto.Bucket{UpperBound: proto.Float64(le), CumulativeCount: proto.Uint64(count)}
}

func family(name, help string, typ dto.MetricType, ms ...*dto.Metric) *dto.MetricFamily {
	mf := &dto.MetricFamily{Name: proto.String(name), Type: typ.Enum(), Metric: ms}
	if help != "" {
		mf.Help = proto.String(help)
	}
	return mf
}

// Whatever this encoder writes has to be byte for byte what expfmt writes, both
// where it renders the samples itself and where it hands them back to expfmt.
func TestEncodeMatchesExpfmt(t *testing.T) {
	odd := `a"b\c` + "\n" + `d`
	for _, mf := range []*dto.MetricFamily{
		family("c_total", "a counter", dto.MetricType_COUNTER,
			&dto.Metric{Label: []*dto.LabelPair{lp("key", "a")}, Counter: &dto.Counter{Value: proto.Float64(3)}},
			&dto.Metric{Label: []*dto.LabelPair{lp("key", odd), lp("zz", "")}, Counter: &dto.Counter{Value: proto.Float64(0)}},
			&dto.Metric{Counter: &dto.Counter{Value: proto.Float64(-1)}},
		),
		family("g", "", dto.MetricType_GAUGE,
			&dto.Metric{Label: []*dto.LabelPair{lp("k", "v")}, Gauge: &dto.Gauge{Value: proto.Float64(math.NaN())}},
			&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(math.Inf(+1))}},
			&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(math.Inf(-1))}},
			&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(1)}},
			&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(1e-7)}},
			&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(123456789012345.678)}},
		),
		family("u", "untyped\\ and \nnewline", dto.MetricType_UNTYPED,
			&dto.Metric{Label: []*dto.LabelPair{lp("k", "v")}, Untyped: &dto.Untyped{Value: proto.Float64(7)}},
		),
		// a histogram without an explicit +Inf bucket, which expfmt adds
		family("h", "a histogram", dto.MetricType_HISTOGRAM,
			&dto.Metric{
				Label: []*dto.LabelPair{lp("key", "a")},
				Histogram: &dto.Histogram{
					SampleCount: proto.Uint64(4), SampleSum: proto.Float64(17.5),
					Bucket: []*dto.Bucket{bucket(5, 1), bucket(10, 3)},
				},
			},
		),
		// and one that carries it
		family("h2", "", dto.MetricType_HISTOGRAM,
			&dto.Metric{
				Histogram: &dto.Histogram{
					SampleCount: proto.Uint64(2), SampleSum: proto.Float64(0),
					Bucket: []*dto.Bucket{bucket(1, 1), bucket(math.Inf(+1), 2)},
				},
			},
		),
		// falls back to expfmt: a summary
		family("s", "a summary", dto.MetricType_SUMMARY,
			&dto.Metric{Summary: &dto.Summary{
				SampleCount: proto.Uint64(2), SampleSum: proto.Float64(3),
				Quantile: []*dto.Quantile{{Quantile: proto.Float64(0.5), Value: proto.Float64(1.5)}},
			}},
		),
		// falls back to expfmt: a sample with its own timestamp
		family("ts_total", "", dto.MetricType_COUNTER,
			&dto.Metric{Counter: &dto.Counter{Value: proto.Float64(1)}, TimestampMs: proto.Int64(1234)},
		),
		// falls back to expfmt: a name that is not a valid legacy name
		family("weird.name", "dotted", dto.MetricType_COUNTER,
			&dto.Metric{Counter: &dto.Counter{Value: proto.Float64(1)}},
		),
		// falls back to expfmt: a label name that is not a valid legacy name
		family("lbl_total", "", dto.MetricType_COUNTER,
			&dto.Metric{Label: []*dto.LabelPair{lp("a.b", "v")}, Counter: &dto.Counter{Value: proto.Float64(1)}},
		),
	} {
		t.Run(mf.GetName(), func(t *testing.T) {
			rows := make([]*entry, len(mf.Metric))
			for i := range rows {
				rows[i] = &entry{}
			}
			var got bytes.Buffer
			w := bufio.NewWriter(&got)
			enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
			if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{rows}, func(*entry) int64 { return 0 }, newEncState(), "", ""); err != nil {
				t.Fatal(err)
			}
			w.Flush()

			var want bytes.Buffer
			ref := expfmt.NewEncoder(&want, expfmt.NewFormat(expfmt.TypeTextPlain))
			if err := ref.Encode(mf); err != nil {
				t.Fatal(err)
			}
			if got.String() != want.String() {
				t.Fatalf("output differs\n--- this encoder ---\n%s\n--- expfmt ---\n%s", got.String(), want.String())
			}
		})
	}
}

// The cached prefixes have to be rebuilt when the generation label changes,
// otherwise a rebase would keep reporting the old generation.
func TestEncodeRebuildsOnGenerationChange(t *testing.T) {
	mf := family("c_total", "", dto.MetricType_COUNTER,
		&dto.Metric{Label: []*dto.LabelPair{lp("gen", "1000")}, Counter: &dto.Counter{Value: proto.Float64(1)}},
	)
	en := &entry{}
	shapes := newEncState()
	gen := int64(1000)

	render := func() string {
		var out bytes.Buffer
		w := bufio.NewWriter(&out)
		enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
		if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{{en}}, func(*entry) int64 { return gen }, shapes, "gen", ""); err != nil {
			t.Fatal(err)
		}
		w.Flush()
		return out.String()
	}
	first := render()
	mf.Metric[0].Label[0].Value = proto.String("2000")
	gen = 2000
	second := render()
	if first == second {
		t.Fatalf("the prefix should have been rebuilt for the new generation:\n%s", second)
	}
	if !bytes.Contains([]byte(second), []byte(`gen="2000"`)) {
		t.Fatalf("the new generation should be in the output:\n%s", second)
	}
}

// A family this encoder cannot take goes to expfmt, which renders from the dto,
// so the generation has to be put there before it does. Whether the encoder
// takes a family is only known once every metric of it is in, so a metric that
// forces the fallback -- here one carrying its own timestamp -- must not leave
// the others in the family without their generation.
func TestFallbackKeepsGeneration(t *testing.T) {
	mf := family("c_total", "", dto.MetricType_COUNTER,
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "a")}, Counter: &dto.Counter{Value: proto.Float64(1)}},
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "b")}, Counter: &dto.Counter{Value: proto.Float64(2)}, TimestampMs: proto.Int64(1234)},
	)
	rows := []*entry{{born: 1000}, {born: 2000}}

	var out bytes.Buffer
	w := bufio.NewWriter(&out)
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{rows},
		func(en *entry) int64 { return int64(en.born) }, newEncState(), "gen", ""); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	// The generation goes in its sorted place among the metric's own labels, so
	// a consumer that sorts what it receives has nothing to move.
	for _, want := range []string{`gen="1000",key="a"`, `gen="2000",key="b"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("the fallback dropped %s:\n%s", want, out.String())
		}
	}
}

// The family shape carries the le fragments of one bucket layout. A family whose
// metrics disagree on the layout has to go to expfmt rather than be written
// against the wrong bounds.
func TestMismatchedBucketLayoutFallsBack(t *testing.T) {
	mf := family("h", "", dto.MetricType_HISTOGRAM,
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "a")}, Histogram: &dto.Histogram{
			SampleCount: proto.Uint64(2), SampleSum: proto.Float64(3),
			Bucket: []*dto.Bucket{bucket(5, 1), bucket(10, 2)},
		}},
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "b")}, Histogram: &dto.Histogram{
			SampleCount: proto.Uint64(3), SampleSum: proto.Float64(9),
			Bucket: []*dto.Bucket{bucket(1, 1), bucket(2, 2), bucket(4, 3)},
		}},
	)
	rows := []*entry{{}, {}}

	var got bytes.Buffer
	w := bufio.NewWriter(&got)
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{rows},
		func(*entry) int64 { return 0 }, newEncState(), "", ""); err != nil {
		t.Fatal(err)
	}
	w.Flush()

	var want bytes.Buffer
	ref := expfmt.NewEncoder(&want, expfmt.NewFormat(expfmt.TypeTextPlain))
	if err := ref.Encode(mf); err != nil {
		t.Fatal(err)
	}
	if got.String() != want.String() {
		t.Fatalf("output differs\n--- this encoder ---\n%s\n--- expfmt ---\n%s", got.String(), want.String())
	}
}

// Same number of buckets, different bounds: the le of one would be written
// against the other's values, which looks well formed and is wrong.
func TestSameBucketCountDifferentBoundsFallsBack(t *testing.T) {
	mf := family("h3", "", dto.MetricType_HISTOGRAM,
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "a")}, Histogram: &dto.Histogram{
			SampleCount: proto.Uint64(2), SampleSum: proto.Float64(3),
			Bucket: []*dto.Bucket{bucket(5, 1), bucket(10, 2)},
		}},
		&dto.Metric{Label: []*dto.LabelPair{lp("key", "b")}, Histogram: &dto.Histogram{
			SampleCount: proto.Uint64(2), SampleSum: proto.Float64(3),
			Bucket: []*dto.Bucket{bucket(1, 1), bucket(2, 2)},
		}},
	)
	rows := []*entry{{}, {}}

	var got bytes.Buffer
	w := bufio.NewWriter(&got)
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{rows},
		func(*entry) int64 { return 0 }, newEncState(), "", ""); err != nil {
		t.Fatal(err)
	}
	w.Flush()

	var want bytes.Buffer
	ref := expfmt.NewEncoder(&want, expfmt.NewFormat(expfmt.TypeTextPlain))
	if err := ref.Encode(mf); err != nil {
		t.Fatal(err)
	}
	if got.String() != want.String() {
		t.Fatalf("output differs\n--- this encoder ---\n%s\n--- expfmt ---\n%s", got.String(), want.String())
	}
}

// The generation label goes where it sorts among the metric's own labels, which
// Write hands out sorted. VictoriaMetrics sorts every label set it ingests, so a
// label appended past the end is one it has to move for every series of every
// scrape. Both the cached encoder and the expfmt fallback place it the same way.
func TestGenerationLabelIsSorted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels []*dto.LabelPair
		want   string
	}{
		{"before every label", []*dto.LabelPair{lp("key", "a"), lp("zone", "b")}, `{gen="7",key="a",zone="b"}`},
		{"between two labels", []*dto.LabelPair{lp("app", "a"), lp("key", "b")}, `{app="a",gen="7",key="b"}`},
		{"after every label", []*dto.LabelPair{lp("app", "a"), lp("be", "b")}, `{app="a",be="b",gen="7"}`},
		{"no labels of its own", nil, `{gen="7"}`},
		// A metric already carrying a label of that name has it replaced, not
		// duplicated: two labels of one name is not a valid exposition.
		{"replacing a label of the same name", []*dto.LabelPair{lp("app", "a"), lp("gen", "stale"), lp("key", "b")}, `{app="a",gen="7",key="b"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []string{"cached", "fallback"} {
				m := &dto.Metric{Label: tc.labels, Counter: &dto.Counter{Value: proto.Float64(1)}}
				if path == "fallback" {
					// A sample carrying its own timestamp is not one this encoder writes.
					m.TimestampMs = proto.Int64(1234)
				}
				mf := family("c_total", "", dto.MetricType_COUNTER, m)
				var out bytes.Buffer
				w := bufio.NewWriter(&out)
				enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
				if err := encodeFamilies(w, enc, []*dto.MetricFamily{mf}, [][]*entry{{{born: 7}}},
					func(en *entry) int64 { return int64(en.born) }, newEncState(), "gen", ""); err != nil {
					t.Fatal(err)
				}
				w.Flush()
				if !strings.Contains(out.String(), tc.want) {
					t.Errorf("%s path: want %s in\n%s", path, tc.want, out.String())
				}
			}
		})
	}
}
