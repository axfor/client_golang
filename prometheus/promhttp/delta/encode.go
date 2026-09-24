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
	"math"
	"strconv"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

// Writing the text exposition format with the per-series part cached.
//
// expfmt renders the metric name and every label pair of every sample,
// validating the names and escaping the values each time: a histogram's labels
// once per bucket, plus _sum and _count. Here a series' labels are rendered once
// per scrape and every line of it is composed from that plus pieces the family
// shares -- the metric name with each suffix, and the le of each bucket, which
// the family holds once.
//
// The rendered labels are not kept between scrapes. They were, on each entry and
// shared between the families of one instance, and that was over 50 bytes of
// every instance held for its whole life to spare rendering again the part of
// them that changes in a scrape.
//
// Anything this encoder does not handle falls back to expfmt, so the output is
// the same either way: summaries, native histograms, samples carrying their own
// timestamp, and names that are not valid legacy names, which the exposition
// format escapes according to a negotiated scheme.

// encState is what the cached encoder keeps between scrapes.
type encState struct {
	shapes map[string]*familyShape

	num   []byte        // scratch for formatting one value
	extra [2]extraLabel // the generation and the type, reused per family
}

func newEncState() *encState {
	return &encState{shapes: map[string]*familyShape{}}
}

// familyShape is what every series of a family writes the same way: the metric
// name with each suffix, and the le fragment of each bucket. It is built once
// per family per bucket layout rather than once per series.
type familyShape struct {
	typ     dto.MetricType
	name    []byte // name, for counters, gauges and untyped
	bucket  []byte // name_bucket
	sum     []byte // name_sum
	count   []byte // name_count
	les     [][]byte
	bounds  []float64 // the upper bounds les was built from, for checking a metric fits
	infLast bool      // the layout already ends with +Inf
}

func shapeOf(mf *dto.MetricFamily, m *dto.Metric, shapes map[string]*familyShape) *familyShape {
	name := mf.GetName()
	sh := shapes[name]
	buckets := bucketsOf(m)
	if sh != nil && (sh.typ != dto.MetricType_HISTOGRAM || len(sh.les) == len(buckets)+boolToInt(!sh.infLast)) {
		return sh
	}
	sh = &familyShape{typ: mf.GetType(), name: []byte(name)}
	if sh.typ == dto.MetricType_HISTOGRAM {
		sh.bucket = []byte(name + "_bucket")
		sh.sum = []byte(name + "_sum")
		sh.count = []byte(name + "_count")
		for _, b := range buckets {
			sh.les = append(sh.les, leFragment(b.GetUpperBound()))
			sh.bounds = append(sh.bounds, b.GetUpperBound())
			sh.infLast = math.IsInf(b.GetUpperBound(), +1)
		}
		if !sh.infLast {
			sh.les = append(sh.les, leFragment(math.Inf(+1)))
		}
	}
	shapes[name] = sh
	return sh
}

// fits reports whether a metric has the bucket layout this shape was built for.
//
// The bounds are compared, not just how many there are: two histograms with the
// same number of buckets but different bounds would otherwise be written against
// each other's le, which looks well formed and is wrong.
func (sh *familyShape) fits(m *dto.Metric) bool {
	if sh.typ != dto.MetricType_HISTOGRAM {
		return true
	}
	buckets := bucketsOf(m)
	want := len(sh.les)
	if !sh.infLast {
		want-- // the +Inf fragment is synthesised, not one of the metric's buckets
	}
	if len(buckets) != want {
		return false
	}
	// Comparing the bounds themselves rather than rendering them again: this runs
	// for every histogram of every scrape and must not allocate.
	for i, b := range buckets {
		if sh.bounds[i] != b.GetUpperBound() {
			return false
		}
	}
	return true
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// leFragment renders `,le="x"} ` once for a bucket bound.
func leFragment(v float64) []byte {
	b := append([]byte(","), model.BucketLabel...)
	b = append(b, '=', '"')
	b = appendFloat(b, v)
	return append(b, '"', '}', ' ')
}

// stampFallback puts the generation label on a family expfmt is about to render.
// Gauges never carry it, and neither does a family with no entries behind it.
func stampFallback(mf *dto.MetricFamily, rows []*entry, gen func(*entry) int64, genLabel, typeLabel string) {
	if (genLabel == "" && typeLabel == "") || len(rows) != len(mf.Metric) {
		return
	}
	genKinds := mf.GetType() == dto.MetricType_COUNTER || mf.GetType() == dto.MetricType_HISTOGRAM
	if !genKinds && typeLabel == "" {
		return
	}
	for j, m := range mf.Metric {
		en := rows[j]
		if en == nil {
			continue
		}
		if typeLabel != "" {
			applyLabel(m, typeLabel, typeName(mf.GetType()))
		}
		if genKinds && genLabel != "" {
			applyGen(m, genLabel, gen(en))
		}
	}
}

// applyGen puts the generation on a metric, replacing a label of that name if
// the metric carries one. Write hands out the metric's own label slice, so a
// value written into it in place would change the metric itself; the slice is
// rebuilt instead.
func applyGen(m *dto.Metric, name string, gen int64) {
	applyLabel(m, name, strconv.FormatInt(gen, 10))
}

// applyLabel puts one label on a metric in its sorted place, replacing a label
// of that name if the metric carries one. Write hands out the metric's own
// label slice, so a value written into it in place would change the metric
// itself; the slice is rebuilt instead.
func applyLabel(m *dto.Metric, name, value string) {
	n, v := name, value
	at, replace := genIndex(m.Label, name)
	m.Label = withLabel(m.Label, &dto.LabelPair{Name: &n, Value: &v}, at, replace)
}

// withLabel returns labels with lp at index at, dropping what is there when the
// metric already carries a label of that name. The label slice handed out by
// Write belongs to the metric itself, so it is copied rather than written into.
func withLabel(labels []*dto.LabelPair, lp *dto.LabelPair, at int, replace bool) []*dto.LabelPair {
	out := make([]*dto.LabelPair, 0, len(labels)+1)
	out = append(out, labels[:at]...)
	out = append(out, lp)
	if replace {
		at++
	}
	return append(out, labels[at:]...)
}

func bucketsOf(m *dto.Metric) []*dto.Bucket {
	if m.Histogram == nil {
		return nil
	}
	return m.Histogram.Bucket
}

// buildLabels renders this series' labels once, without the closing brace, so a
// line can be composed as name + labels + ("} " or `,le="x"} `).
// A series with no labels renders as nothing, and the closing brace is left off
// the line as well.
//
// The generation label is appended here rather than put on the dto: this is the
// only place it is needed for a family written from the cache, and keeping it
// out of the dto saves a label slice per series.
func buildLabels(dst []byte, m *dto.Metric, extra []extraLabel) ([]byte, bool) {
	for _, lp := range m.Label {
		if !model.LegacyValidation.IsValidLabelName(lp.GetName()) {
			return nil, false
		}
	}
	if len(m.Label) == 0 && len(extra) == 0 {
		return dst, true
	}
	sep := byte('{')
	next := 0 // the extra label to place before the metric's own labels from here
	write := func(name, value string) {
		dst = append(dst, sep)
		dst = append(dst, name...)
		dst = append(dst, '=', '"')
		dst = appendEscaped(dst, value, true)
		dst = append(dst, '"')
		sep = ','
	}
	for _, lp := range m.Label {
		name := lp.GetName()
		replaced := false
		for next < len(extra) && extra[next].name <= name {
			write(extra[next].name, extra[next].value)
			if extra[next].name == name {
				replaced = true // the metric's own label of that name gives way
			}
			next++
		}
		if !replaced {
			write(name, lp.GetValue())
		}
	}
	for ; next < len(extra); next++ {
		write(extra[next].name, extra[next].value)
	}
	return dst, true
}

// sortExtra orders the extra labels by name. There are at most two.
func sortExtra(e []extraLabel) {
	for i := 1; i < len(e); i++ {
		for j := i; j > 0 && e[j].name < e[j-1].name; j-- {
			e[j], e[j-1] = e[j-1], e[j]
		}
	}
}

// extraLabel is a label this package puts on a series that the metric does not
// carry itself. They are placed in sorted order among the metric's own labels,
// which Write hands out sorted, so a consumer that sorts what it receives has
// nothing to move.
type extraLabel struct{ name, value string }

// genIndex says where the generation label belongs among a metric's own labels,
// which Write hands out sorted by name, and whether one of that name is already
// there and is to be replaced rather than inserted. Consumers that sort what
// they receive -- VictoriaMetrics does -- then have nothing to move, and a
// metric that happens to carry a label named like GenLabel does not go out
// carrying it twice. An index of -1 means the generation is not wanted.
func genIndex(labels []*dto.LabelPair, genLabel string) (int, bool) {
	if genLabel == "" {
		return -1, false
	}
	for i, lp := range labels {
		switch name := lp.GetName(); {
		case genLabel == name:
			return i, true
		case genLabel < name:
			return i, false
		}
	}
	return len(labels), false
}

func appendGen(dst []byte, sep byte, genLabel string, gen int64) []byte {
	if genLabel == "" {
		return dst
	}
	dst = append(dst, sep)
	dst = append(dst, genLabel...)
	dst = append(dst, '=', '"')
	dst = strconv.AppendInt(dst, gen, 10)
	return append(dst, '"')
}

var closeBrace = []byte("} ")

func writeFamilyHeader(w *bufio.Writer, mf *dto.MetricFamily) {
	name := mf.GetName()
	if mf.Help != nil {
		w.WriteString("# HELP ")
		w.WriteString(name)
		w.WriteByte(' ')
		w.Write(appendEscaped(nil, mf.GetHelp(), false))
		w.WriteByte('\n')
	}
	w.WriteString("# TYPE ")
	w.WriteString(name)
	switch mf.GetType() {
	case dto.MetricType_COUNTER:
		w.WriteString(" counter\n")
	case dto.MetricType_GAUGE:
		w.WriteString(" gauge\n")
	case dto.MetricType_UNTYPED:
		w.WriteString(" untyped\n")
	default:
		w.WriteString(" histogram\n")
	}
}

// appendEscaped mirrors expfmt's escaper and quotedEscaper.
func appendEscaped(dst []byte, s string, quote bool) []byte {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '"':
			if quote {
				dst = append(dst, '\\', '"')
			} else {
				dst = append(dst, '"')
			}
		default:
			dst = append(dst, c)
		}
	}
	return dst
}

// appendFloat mirrors expfmt's writeFloat.
func appendFloat(dst []byte, f float64) []byte {
	switch {
	case f == 1:
		return append(dst, '1')
	case f == 0:
		return append(dst, '0')
	case f == -1:
		return append(dst, '-', '1')
	case math.IsNaN(f):
		return append(dst, "NaN"...)
	case math.IsInf(f, +1):
		return append(dst, "+Inf"...)
	case math.IsInf(f, -1):
		return append(dst, "-Inf"...)
	default:
		return strconv.AppendFloat(dst, f, 'g', -1, 64)
	}
}
