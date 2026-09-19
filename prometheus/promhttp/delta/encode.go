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
// of it changes between scrapes, so each series keeps the bytes to the left of
// its value and a scrape only appends numbers.
//
// Anything this encoder does not handle falls back to expfmt, so the output is
// the same either way: summaries, native histograms, samples carrying their own
// timestamp, and names that are not valid legacy names, which the exposition
// format escapes according to a negotiated scheme.

// encodeFamilies writes the families of one scrape. rows[i] holds the entries
// behind buf[i].Metric, in the same order, and carries the cached prefixes.
func encodeFamilies(w *bufio.Writer, enc expfmt.Encoder, buf []*dto.MetricFamily, rows [][]*entry, gen func(*entry) int64) error {
	for i, mf := range buf {
		if len(mf.Metric) == 0 {
			continue
		}
		if !cacheLines(mf, rows[i], gen) {
			if err := enc.Encode(mf); err != nil {
				return err
			}
			continue
		}
		writeFamilyHeader(w, mf)
		for j, m := range mf.Metric {
			if err := writeCached(w, mf, m, rows[i][j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// cacheLines makes sure every entry of a family has its prefixes, and reports
// whether the whole family can be written from them.
func cacheLines(mf *dto.MetricFamily, rows []*entry, gen func(*entry) int64) bool {
	if len(rows) != len(mf.Metric) || !handled(mf) {
		return false
	}
	name := mf.GetName()
	if !model.LegacyValidation.IsValidMetricName(name) {
		return false
	}
	for j, m := range mf.Metric {
		en := rows[j]
		if en == nil || m.TimestampMs != nil {
			return false
		}
		g := gen(en)
		if en.lines != nil && en.linesGen == g && en.linesBuckets == len(bucketsOf(m)) {
			continue
		}
		lines, ok := buildLines(en.lines[:0], name, mf.GetType(), m)
		if !ok {
			return false
		}
		en.lines, en.linesGen, en.linesBuckets = lines, g, len(bucketsOf(m))
	}
	return true
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

// buildLines renders the bytes to the left of the value for every sample line
// this metric produces, in the order they are written.
func buildLines(dst [][]byte, name string, typ dto.MetricType, m *dto.Metric) ([][]byte, bool) {
	for _, lp := range m.Label {
		if !model.LegacyValidation.IsValidLabelName(lp.GetName()) {
			return nil, false
		}
	}
	line := func(suffix, extraName string, extraValue float64) {
		var b []byte
		b = appendNameAndLabels(b, name+suffix, m.Label, extraName, extraValue)
		dst = append(dst, append(b, ' '))
	}
	if typ != dto.MetricType_HISTOGRAM {
		line("", "", 0)
		return dst, true
	}
	infSeen := false
	for _, b := range m.Histogram.Bucket {
		line("_bucket", model.BucketLabel, b.GetUpperBound())
		if math.IsInf(b.GetUpperBound(), +1) {
			infSeen = true
		}
	}
	if !infSeen {
		line("_bucket", model.BucketLabel, math.Inf(+1))
	}
	line("_sum", "", 0)
	line("_count", "", 0)
	return dst, true
}

// writeCached writes one metric from its cached prefixes.
func writeCached(w *bufio.Writer, mf *dto.MetricFamily, m *dto.Metric, en *entry) error {
	value := func(i int, v float64) error {
		if _, err := w.Write(en.lines[i]); err != nil {
			return err
		}
		if _, err := w.Write(appendFloat(en.num[:0], v)); err != nil {
			return err
		}
		return w.WriteByte('\n')
	}
	switch mf.GetType() {
	case dto.MetricType_COUNTER:
		return value(0, m.GetCounter().GetValue())
	case dto.MetricType_GAUGE:
		return value(0, m.GetGauge().GetValue())
	case dto.MetricType_UNTYPED:
		return value(0, m.GetUntyped().GetValue())
	}
	h := m.GetHistogram()
	i := 0
	infSeen := false
	for _, b := range h.Bucket {
		v := b.GetCumulativeCountFloat()
		if v == 0 {
			v = float64(b.GetCumulativeCount())
		}
		if err := value(i, v); err != nil {
			return err
		}
		i++
		if math.IsInf(b.GetUpperBound(), +1) {
			infSeen = true
		}
	}
	count := h.GetSampleCountFloat()
	if count == 0 {
		count = float64(h.GetSampleCount())
	}
	if !infSeen {
		if err := value(i, count); err != nil {
			return err
		}
		i++
	}
	if err := value(i, h.GetSampleSum()); err != nil {
		return err
	}
	return value(i+1, count)
}

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

// appendNameAndLabels mirrors expfmt's writeNameAndLabelPairs for names that are
// valid legacy names, which is all this encoder accepts.
func appendNameAndLabels(dst []byte, name string, labels []*dto.LabelPair, extraName string, extraValue float64) []byte {
	dst = append(dst, name...)
	if len(labels) == 0 && extraName == "" {
		return dst
	}
	sep := byte('{')
	for _, lp := range labels {
		dst = append(dst, sep)
		dst = append(dst, lp.GetName()...)
		dst = append(dst, '=', '"')
		dst = appendEscaped(dst, lp.GetValue(), true)
		dst = append(dst, '"')
		sep = ','
	}
	if extraName != "" {
		dst = append(dst, sep)
		dst = append(dst, extraName...)
		dst = append(dst, '=', '"')
		dst = appendFloat(dst, extraValue)
		dst = append(dst, '"')
	}
	return append(dst, '}')
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
