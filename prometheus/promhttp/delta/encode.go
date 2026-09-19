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
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Writing the text exposition format with the per-series part cached.
//
// expfmt renders the metric name and every label pair of every sample on every
// scrape, validating the names and escaping the values each time, which for a
// process whose series are stable is the largest single cost of a scrape. None
// of it changes between scrapes, so each series keeps its rendered labels and a
// scrape composes a line from those plus pieces the family shares.
//
// A series keeps one label string, not one prefix per line: a histogram writes a
// line per bucket plus _sum and _count, and holding a full prefix for each cost
// more memory than the whole rest of the exposer. What varies between those
// lines is the metric name and the le, which every series of a family has in
// common and which the family therefore holds once.
//
// Anything this encoder does not handle falls back to expfmt, so the output is
// the same either way: summaries, native histograms, samples carrying their own
// timestamp, and names that are not valid legacy names, which the exposition
// format escapes according to a negotiated scheme.

// encodeFamilies writes the families of one scrape. rows[i] holds the entries
// behind buf[i].Metric, in the same order, and carries the cached labels.
func encodeFamilies(w *bufio.Writer, enc expfmt.Encoder, buf []*dto.MetricFamily, rows [][]*entry, gen func(*entry) int64, shapes map[string]*familyShape, genLabel string) error {
	for i, mf := range buf {
		if len(mf.Metric) == 0 {
			continue
		}
		sh := cacheLabels(mf, rows[i], gen, shapes, genLabel)
		if sh == nil {
			// expfmt renders from the dto, so the generation has to go on it here.
			// It cannot be decided earlier: whether this encoder takes a family is
			// only known once every metric of it is in.
			stampFallback(mf, rows[i], gen, genLabel)
			if err := enc.Encode(mf); err != nil {
				return err
			}
			continue
		}
		writeFamilyHeader(w, mf)
		for j, m := range mf.Metric {
			if err := writeCached(w, sh, m, rows[i][j]); err != nil {
				return err
			}
		}
	}
	return nil
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

// cacheLabels makes sure every entry of a family has its rendered labels, and
// returns the shape the family writes with, or nil when expfmt has to take over.
func cacheLabels(mf *dto.MetricFamily, rows []*entry, gen func(*entry) int64, shapes map[string]*familyShape, genLabel string) *familyShape {
	if len(rows) != len(mf.Metric) || !handled(mf) {
		return nil
	}
	if !model.LegacyValidation.IsValidMetricName(mf.GetName()) {
		return nil
	}
	// A gauge never carries the generation: it is a current value, not something
	// that accumulates across a rebuild.
	switch mf.GetType() {
	case dto.MetricType_COUNTER, dto.MetricType_HISTOGRAM:
	default:
		genLabel = ""
	}
	var sh *familyShape
	for j, m := range mf.Metric {
		en := rows[j]
		if en == nil || m.TimestampMs != nil {
			return nil
		}
		if sh == nil {
			sh = shapeOf(mf, m, shapes)
		} else if !sh.fits(m) {
			// The shape carries the le fragments of the family's bucket layout.
			// A metric with a different layout would be written against the wrong
			// bounds, so the family goes to expfmt instead.
			return nil
		}
		g := gen(en)
		if en.rendered != nil && en.renderedGen == g {
			continue
		}
		rendered, ok := buildLabels(en.rendered[:0], m, genLabel, g)
		if !ok {
			return nil
		}
		en.rendered, en.renderedGen = rendered, g
	}
	return sh
}

// stampFallback puts the generation label on a family expfmt is about to render.
// Gauges never carry it, and neither does a family with no entries behind it.
func stampFallback(mf *dto.MetricFamily, rows []*entry, gen func(*entry) int64, genLabel string) {
	if genLabel == "" || len(rows) != len(mf.Metric) {
		return
	}
	switch mf.GetType() {
	case dto.MetricType_COUNTER, dto.MetricType_HISTOGRAM:
	default:
		return
	}
	for j, m := range mf.Metric {
		if en := rows[j]; en != nil {
			applyGen(m, genLabel, gen(en))
		}
	}
}

// applyGen adds or updates one label on a metric. The label slice handed out by
// Write belongs to the metric itself, so it is copied rather than appended to.
func applyGen(m *dto.Metric, name string, gen int64) {
	value := strconv.FormatInt(gen, 10)
	for _, lp := range m.Label {
		if lp.GetName() == name {
			lp.Value = &value
			return
		}
	}
	labels := make([]*dto.LabelPair, len(m.Label), len(m.Label)+1)
	copy(labels, m.Label)
	n := name
	m.Label = append(labels, &dto.LabelPair{Name: &n, Value: &value})
}

func handled(mf *dto.MetricFamily) bool {
	switch mf.GetType() {
	case dto.MetricType_COUNTER, dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
		return true
	case dto.MetricType_HISTOGRAM:
		for _, m := range mf.Metric {
			if m.Histogram == nil || isNative(m.Histogram) {
				return false
			}
		}
		return true
	default:
		return false
	}
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
func buildLabels(dst []byte, m *dto.Metric, genLabel string, gen int64) ([]byte, bool) {
	for _, lp := range m.Label {
		if !model.LegacyValidation.IsValidLabelName(lp.GetName()) {
			return nil, false
		}
	}
	if len(m.Label) == 0 && genLabel == "" {
		return dst, true
	}
	sep := byte('{')
	for _, lp := range m.Label {
		dst = append(dst, sep)
		dst = append(dst, lp.GetName()...)
		dst = append(dst, '=', '"')
		dst = appendEscaped(dst, lp.GetValue(), true)
		dst = append(dst, '"')
		sep = ','
	}
	if genLabel != "" {
		dst = append(dst, sep)
		dst = append(dst, genLabel...)
		dst = append(dst, '=', '"')
		dst = strconv.AppendInt(dst, gen, 10)
		dst = append(dst, '"')
	}
	return dst, true
}

// writeCached writes one metric from its cached labels and its family's shape.
//
// A series with no labels needs the braces opened by the le fragment instead of
// by the labels, which is the only place the two cases differ.
func writeCached(w *bufio.Writer, sh *familyShape, m *dto.Metric, en *entry) error {
	plain := closeBrace
	if len(en.rendered) == 0 {
		plain = plain[1:] // no labels, so no braces to close
	}
	line := func(name, tail []byte, v float64) error {
		if _, err := w.Write(name); err != nil {
			return err
		}
		if _, err := w.Write(en.rendered); err != nil {
			return err
		}
		if len(en.rendered) == 0 && len(tail) > 0 && tail[0] == ',' {
			if err := w.WriteByte('{'); err != nil {
				return err
			}
			tail = tail[1:]
		}
		if _, err := w.Write(tail); err != nil {
			return err
		}
		en.num = appendFloat(en.num[:0], v) // kept, or the buffer is reallocated every value
		if _, err := w.Write(en.num); err != nil {
			return err
		}
		return w.WriteByte('\n')
	}
	switch sh.typ {
	case dto.MetricType_COUNTER:
		return line(sh.name, plain, m.GetCounter().GetValue())
	case dto.MetricType_GAUGE:
		return line(sh.name, plain, m.GetGauge().GetValue())
	case dto.MetricType_UNTYPED:
		return line(sh.name, plain, m.GetUntyped().GetValue())
	}
	h := m.GetHistogram()
	count := h.GetSampleCountFloat()
	if count == 0 {
		count = float64(h.GetSampleCount())
	}
	for i, b := range h.Bucket {
		v := b.GetCumulativeCountFloat()
		if v == 0 {
			v = float64(b.GetCumulativeCount())
		}
		if err := line(sh.bucket, sh.les[i], v); err != nil {
			return err
		}
	}
	if !sh.infLast {
		if err := line(sh.bucket, sh.les[len(sh.les)-1], count); err != nil {
			return err
		}
	}
	if err := line(sh.sum, plain, h.GetSampleSum()); err != nil {
		return err
	}
	return line(sh.count, plain, count)
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
