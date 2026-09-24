// Copyright 2025 The Prometheus Authors
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
	"unsafe"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/protobuf/proto"

	"github.com/prometheus/client_golang/prometheus"
)

// A scrape goes in two passes. The first takes the instances due -- changed,
// heartbeat, every gauge -- and only groups them by family. The second writes
// one family at a time: each instance into the one scratch dto, decided, and its
// values copied out before the scratch is reused for the next.
//
// It used to write every instance into a dto of its own and keep them all until
// the whole scrape was collected, because a family's instances are not taken
// together and the output has to be. Those dtos were then kept on free lists for
// the next scrape to reuse, sized to a decaying high-water mark of how many a
// scrape reported: at 1.6M instances with half changing per scrape that was a
// quarter of the process, 500 MiB, most of it histogram dtos of seventeen
// separately allocated buckets each. What a family needs to be encoded is now
// held only while that family is being encoded.

// pickReason says why an instance is in this scrape.
type pickReason uint8

const (
	pickChanged   pickReason = iota // written since the last scrape
	pickHeartbeat                   // unchanged, its heartbeat bucket is due
	pickGauge                       // a gauge, which goes out every scrape
)

// pick is one instance taken for this scrape and not yet written.
type pick struct {
	m      prometheus.Metric
	en     *entry
	fresh  bool
	reason pickReason
}

// familySlot is what the first pass collected for one family.
type familySlot struct {
	desc  *prometheus.Desc
	picks []pick
	peak  int // recent high-water mark of len(picks), so the array can fall back
}

// frow is one instance of the family being written. Its values are
// nums[off:off+n]: for a histogram, the upper bound and the count of every
// bucket followed by the sum and the count; otherwise the one value.
type frow struct {
	en     *entry
	labels []*dto.LabelPair // the metric's own, which Write hands out and never changes
	full   *dto.Metric      // a full copy, for what the values cannot express
	ts     int64
	hasTs  bool
	off, n int32
}

// pick records m for this scrape, creating its entry if it is new.
func (e *TrackedExposer) pick(m prometheus.Metric, reason pickReason) {
	en, slot := entryOf(m)
	if slot == nil {
		return // deleted from its Vec since it was taken
	}
	desc := m.Desc()
	fresh := en == nil
	if fresh {
		en = &entry{changed: e.round, born: e.rb.clock().Unix()}
		*slot = unsafe.Pointer(en)
		e.live++
	}
	en.lastRound = e.round

	name := desc.Name()
	idx, ok := e.byName[name]
	if !ok {
		idx = e.nslots
		if idx == len(e.slots) {
			e.slots = append(e.slots, familySlot{})
		}
		e.slots[idx].desc = desc
		e.byName[name] = idx
		e.nslots++
	}
	s := &e.slots[idx]
	s.picks = append(s.picks, pick{m: m, en: en, fresh: fresh, reason: reason})
	// Undelivered, every instance taken is put back on the dirty list, whether or
	// not its write goes through: the tracker has already cleared its flag.
	e.taken = append(e.taken, m)
}

// familyRows is one family while it is being written: its header, how it will
// be encoded, and the values kept for each of its instances.
type familyRows struct {
	mf     *dto.MetricFamily
	sh     *familyShape
	cached bool // still able to take the cached path
	extra  []extraLabel
	genAt  int
	rows   []frow
	nums   []float64

	// The largest family written this scrape, so Serve can let the arrays fall
	// back to what a scrape needs rather than to what one family once did.
	rowsHigh, numsHigh int
}

// start begins a family with header mf.
func (fr *familyRows) start(mf *dto.MetricFamily, es *encState, genLabel, typeLabel string) {
	fr.mf, fr.sh = mf, nil
	fr.cached = model.LegacyValidation.IsValidMetricName(mf.GetName()) && handledType(mf.GetType())
	fr.extra, fr.genAt = familyExtra(es, mf.GetType(), genLabel, typeLabel)
	fr.rows, fr.nums = fr.rows[:0], fr.nums[:0]
}

// add keeps what the family's encoding needs of m, an instance whose entry is
// en and whose generation is gen: its values, and on the cached path its label
// string. m can be reused as soon as add returns.
func (fr *familyRows) add(m *dto.Metric, en *entry, gen int64, es *encState) {
	row := frow{en: en, labels: m.Label, off: int32(len(fr.nums))}
	if en == nil {
		fr.cached = false
	}
	if m.TimestampMs != nil {
		row.ts, row.hasTs = *m.TimestampMs, true
		fr.cached = false
	}
	switch fr.mf.GetType() {
	case dto.MetricType_COUNTER:
		fr.nums = append(fr.nums, m.GetCounter().GetValue())
	case dto.MetricType_GAUGE:
		fr.nums = append(fr.nums, m.GetGauge().GetValue())
	case dto.MetricType_UNTYPED:
		fr.nums = append(fr.nums, m.GetUntyped().GetValue())
	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		if h == nil || isNative(h) {
			fr.cached = false
			row.full = proto.Clone(m).(*dto.Metric)
			break
		}
		for _, b := range h.Bucket {
			v := b.GetCumulativeCountFloat()
			if v == 0 {
				v = float64(b.GetCumulativeCount())
			}
			fr.nums = append(fr.nums, b.GetUpperBound(), v)
		}
		count := h.GetSampleCountFloat()
		if count == 0 {
			count = float64(h.GetSampleCount())
		}
		fr.nums = append(fr.nums, h.GetSampleSum(), count)
	default:
		row.full = proto.Clone(m).(*dto.Metric)
	}
	row.n = int32(len(fr.nums)) - row.off

	if fr.cached {
		if fr.sh == nil {
			fr.sh = shapeOf(fr.mf, m, es.shapes)
		} else if !fr.sh.fits(m) {
			// The shape carries the le fragments of the family's bucket layout.
			// A metric with a different layout would be written against the
			// wrong bounds, so the family goes to expfmt instead.
			fr.cached = false
		}
	}
	if fr.cached && (en.rendered == "" || en.renderedGen != gen) {
		if fr.genAt >= 0 {
			fr.extra[fr.genAt].value = strconv.FormatInt(gen, 10)
		}
		rendered, ok := buildLabels(es.buf[:0], m, fr.extra)
		if ok {
			es.buf = rendered // keep the grown scratch
			en.rendered, en.renderedGen = es.share(rendered), gen
		} else {
			fr.cached = false
		}
	}
	fr.rows = append(fr.rows, row)
}

// encode writes the family out: on the cached path from the values kept, and
// otherwise through expfmt, from dtos built back out of them -- the path for
// timestamps, native histograms, summaries, and bucket layouts that differ
// within one family.
func (fr *familyRows) encode(w *bufio.Writer, enc expfmt.Encoder, es *encState, gen func(*entry) int64, genLabel, typeLabel string) error {
	if len(fr.rows) == 0 {
		return nil
	}
	if fr.cached {
		writeFamilyHeader(w, fr.mf)
		for i := range fr.rows {
			if err := writeRow(w, fr.sh, &fr.rows[i], fr.nums, es); err != nil {
				return err
			}
		}
		return nil
	}
	fr.mf.Metric = make([]*dto.Metric, 0, len(fr.rows))
	ens := make([]*entry, 0, len(fr.rows))
	for i := range fr.rows {
		fr.mf.Metric = append(fr.mf.Metric, rebuild(fr.mf.GetType(), &fr.rows[i], fr.nums))
		ens = append(ens, fr.rows[i].en)
	}
	stampFallback(fr.mf, ens, gen, genLabel, typeLabel)
	err := enc.Encode(fr.mf)
	fr.mf.Metric = nil
	return err
}

// reset drops what the family held, keeping the arrays for the next one. The
// rows reference entries and label slices, so they are cleared rather than only
// truncated: a family written once and not again would keep them alive.
func (fr *familyRows) reset() {
	fr.rowsHigh, fr.numsHigh = max(fr.rowsHigh, len(fr.rows)), max(fr.numsHigh, len(fr.nums))
	clear(fr.rows)
	fr.rows, fr.nums, fr.mf, fr.sh = fr.rows[:0], fr.nums[:0], nil, nil
}

// emitFamily writes, decides and encodes the instances collected for one family.
func (e *TrackedExposer) emitFamily(s *familySlot, w *bufio.Writer, enc expfmt.Encoder, st *ScrapeStats, pending *[]pendingCommit) error {
	fr := &e.fam
	defer fr.reset()
	name := s.desc.Name()
	m := e.scratch[name]
	if m == nil {
		m = &dto.Metric{}
		e.scratch[name] = m
	}
	for _, p := range s.picks {
		m.TimestampMs = nil // the one field Write sets only when it has one
		if err := p.m.Write(m); err != nil {
			continue
		}
		switch p.reason {
		case pickHeartbeat:
			st.Heartbeat++
		case pickGauge:
			st.Gauges++
		}
		typ := metricType(m)
		if typ == nil || !e.opts.Only.carries(*typ) {
			continue // not this endpoint's kind; nothing taken, nothing lost
		}
		if fr.mf == nil {
			name, help := name, s.desc.Help()
			fr.start(&dto.MetricFamily{Name: &name, Help: &help, Type: typ}, e.enc, e.opts.GenLabel, e.opts.TypeLabel)
			st.Families++
		}
		en := p.en
		en.kind = fr.mf.GetType()
		if p.fresh && en.kind == dto.MetricType_GAUGE {
			// Gauges are reported in full every scrape, which the bucketed walk
			// over the tracker cannot do, so they get a list of their own. Entries
			// dropped as idle are pruned from it lazily in Serve.
			e.gauges = append(e.gauges, p.m)
		}
		// Capture the labels before decide stamps the generation on them: they
		// are handed to Options.Delete, which looks the child up in its Vec, and
		// the Vec has no generation label. Only needed when there is a callback;
		// without one idle cleanup goes through prometheus.DeleteTracked.
		if en.del == nil && e.opts.Delete != nil {
			en.del = &deleteKey{family: name, labels: labelsOf(m.Label)}
		}
		emit, _, pc := e.decide(fr.mf, m, en)
		if pc != nil {
			*pending = append(*pending, *pc)
		}
		if !emit && p.reason == pickChanged {
			continue // unchanged, e.g. added and subtracted again within one scrape
		}
		fr.add(m, en, e.rb.genOf(en), e.enc)
	}
	st.Samples += len(fr.rows)
	return fr.encode(w, enc, e.enc, e.rb.genOf, e.opts.GenLabel, e.opts.TypeLabel)
}

// handledType reports whether a family of this type can take the cached path;
// a histogram family can still be sent to expfmt by one of its instances.
func handledType(t dto.MetricType) bool {
	switch t {
	case dto.MetricType_COUNTER, dto.MetricType_GAUGE, dto.MetricType_UNTYPED, dto.MetricType_HISTOGRAM:
		return true
	}
	return false
}

// familyExtra returns the labels the cached encoder adds to every instance of a
// family of type t, sorted, and where the generation is among them (-1: none).
func familyExtra(es *encState, t dto.MetricType, genLabel, typeLabel string) ([]extraLabel, int) {
	// A gauge never carries the generation: it is a current value, not something
	// that accumulates across a rebuild.
	switch t {
	case dto.MetricType_COUNTER, dto.MetricType_HISTOGRAM:
	default:
		genLabel = ""
	}
	// The type is the family's, so the label that carries it is built once here
	// rather than per series.
	extra := es.extra[:0]
	if typeLabel != "" {
		extra = append(extra, extraLabel{typeLabel, typeName(t)})
	}
	if genLabel != "" {
		extra = append(extra, extraLabel{genLabel, ""})
	}
	sortExtra(extra)
	genAt := -1
	for i := range extra {
		if genLabel != "" && extra[i].name == genLabel {
			genAt = i
		}
	}
	return extra, genAt
}

// writeRow writes one instance on the cached path, from the values kept for it.
func writeRow(w *bufio.Writer, sh *familyShape, r *frow, nums []float64, es *encState) error {
	en := r.en
	plain := closeBrace
	if len(en.rendered) == 0 {
		plain = plain[1:] // no labels, so no braces to close
	}
	line := func(name, tail []byte, v float64) error {
		if _, err := w.Write(name); err != nil {
			return err
		}
		if _, err := w.WriteString(en.rendered); err != nil {
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
		es.num = appendFloat(es.num[:0], v) // kept, or the buffer is reallocated every value
		if _, err := w.Write(es.num); err != nil {
			return err
		}
		return w.WriteByte('\n')
	}
	vals := nums[r.off : r.off+r.n]
	if sh.typ != dto.MetricType_HISTOGRAM {
		return line(sh.name, plain, vals[0])
	}
	buckets := (len(vals) - 2) / 2
	count := vals[len(vals)-1]
	for i := range buckets {
		if err := line(sh.bucket, sh.les[i], vals[2*i+1]); err != nil {
			return err
		}
	}
	if !sh.infLast {
		if err := line(sh.bucket, sh.les[len(sh.les)-1], count); err != nil {
			return err
		}
	}
	if err := line(sh.sum, plain, vals[len(vals)-2]); err != nil {
		return err
	}
	return line(sh.count, plain, count)
}

// rebuild makes a dto for expfmt out of what was kept for one instance.
func rebuild(t dto.MetricType, r *frow, nums []float64) *dto.Metric {
	if r.full != nil {
		return r.full
	}
	m := &dto.Metric{Label: r.labels}
	if r.hasTs {
		ts := r.ts
		m.TimestampMs = &ts
	}
	vals := nums[r.off : r.off+r.n]
	switch t {
	case dto.MetricType_COUNTER:
		m.Counter = &dto.Counter{Value: proto.Float64(vals[0])}
	case dto.MetricType_GAUGE:
		m.Gauge = &dto.Gauge{Value: proto.Float64(vals[0])}
	case dto.MetricType_UNTYPED:
		m.Untyped = &dto.Untyped{Value: proto.Float64(vals[0])}
	case dto.MetricType_HISTOGRAM:
		buckets := (len(vals) - 2) / 2
		h := &dto.Histogram{SampleSum: proto.Float64(vals[len(vals)-2])}
		count := vals[len(vals)-1]
		if count == math.Trunc(count) && count >= 0 && count < 1<<63 {
			h.SampleCount = proto.Uint64(uint64(count))
		} else {
			h.SampleCountFloat = proto.Float64(count)
		}
		h.Bucket = make([]*dto.Bucket, buckets)
		for i := range buckets {
			b := &dto.Bucket{UpperBound: proto.Float64(vals[2*i])}
			c := vals[2*i+1]
			if c == math.Trunc(c) && c >= 0 && c < 1<<63 {
				b.CumulativeCount = proto.Uint64(uint64(c))
			} else {
				b.CumulativeCountFloat = proto.Float64(c)
			}
			h.Bucket[i] = b
		}
		m.Histogram = h
	}
	return m
}
