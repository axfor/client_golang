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
	"net/http"
	"sync"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/prometheus/client_golang/prometheus"
)

// TrackedExposer does what Exposer does without going through Gather: a scrape
// takes only the instances written since the previous one and never builds a dto
// for the rest.
//
// The difference grows with the share of series that sit still. Measured on
// 3000 keys x 33 metrics, about 400k series with 2% changing per scrape: the
// Gather path costs around 240 ms and 1.33M allocations per scrape, this one
// around 2 ms and a few dozen allocations.
//
// Usage, when wiring it by hand rather than through Enable:
//
//	prometheus.EnableChangeTracking()          // before creating any metric
//	reg := prometheus.NewRegistry()
//	... register and use CounterVec / GaugeVec / HistogramVec ...
//	exp := delta.NewTracked(nil, delta.Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 30})
//	mux.Handle("/metrics/delta", exp.Handler())
//
// Constraints:
//   - Only instances created through a Vec are tracked; NewCounter and friends
//     are not. The dirty list has a single consumer.
//   - Idle cleanup removes the child from its Vec. Options.Delete can do that,
//     and with no Delete set prometheus.DeleteTracked does it directly.
type TrackedExposer struct {
	opts    Options
	tracker *prometheus.ChangeTracker
	rb      rebaseState

	mu      sync.Mutex
	round   uint64
	state   map[prometheus.Metric]*entry
	scratch dto.Metric
	buf     []*dto.MetricFamily
	taken   []prometheus.Metric
	byName  map[string]*dto.MetricFamily
}

// NewTracked returns an Exposer backed by change tracking. A nil t means
// prometheus.CurrentChangeTracker(). Metrics have to have been created after
// prometheus.SetChangeTracker(t), or they are not tracked.
func NewTracked(t *prometheus.ChangeTracker, opts Options) *TrackedExposer {
	opts = check(opts)
	if t == nil {
		t = prometheus.CurrentChangeTracker()
	}
	return &TrackedExposer{
		opts:    opts,
		tracker: t,
		state:   map[prometheus.Metric]*entry{},
		byName:  map[string]*dto.MetricFamily{},
	}
}

// Handler returns an http.Handler.
func (e *TrackedExposer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { e.Serve(w, r) })
}

// Serve writes one scrape.
func (e *TrackedExposer) Serve(w http.ResponseWriter, r *http.Request) ScrapeStats {
	e.mu.Lock()
	defer e.mu.Unlock()

	st := ScrapeStats{}
	e.round++
	st.Round = e.round
	st.Rebased = e.rb.due(e.opts.RebaseAfterGap)

	e.buf = e.buf[:0]
	clear(e.byName)
	var pending []pendingCommit
	var idle []*entry
	taken := e.taken[:0] // taken this scrape; put back on the dirty list if undelivered

	// everything written since the previous scrape
	e.tracker.Take(func(m prometheus.Metric) {
		pc, ok := e.take(m, false)
		if !ok {
			return
		}
		taken = append(taken, m)
		if pc != nil {
			pending = append(pending, *pc)
		}
	})

	// the bucket due this scrape: top up unchanged series, list idle ones to drop
	if !e.opts.DisableHeartbeat || e.opts.IdleScrapes > 0 {
		bucket := int(e.round % uint64(e.opts.HeartbeatScrapes))
		e.tracker.RangeBucket(bucket, e.opts.HeartbeatScrapes, func(m prometheus.Metric) {
			en := e.state[m]
			if en == nil {
				return // a new instance not taken yet; leave it for the next scrape
			}
			if e.opts.IdleScrapes > 0 && e.round-en.changed > uint64(e.opts.IdleScrapes) && deletable(en) {
				idle = append(idle, en)
				en.metric = m
				return
			}
			if !e.opts.DisableHeartbeat && en.lastRound != e.round {
				if pc, ok := e.take(m, true); ok {
					st.Heartbeat++
					taken = append(taken, m)
					if pc != nil {
						pending = append(pending, *pc)
					}
				}
			}
		})
	}

	for _, mf := range e.buf {
		st.Samples += len(mf.Metric)
	}
	st.Families = len(e.buf)

	rs := newResponse(w, r)
	enc := expfmt.NewEncoder(rs.w, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range e.buf {
		if err := enc.Encode(mf); err != nil {
			st.Err = err
			break
		}
	}
	st.Err, st.Delivered = rs.close(st.Err, r)

	if st.Delivered {
		e.commit(pending)
		e.rb.delivered()
	} else {
		// undelivered: put them back so the next scrape reports them again,
		// together with whatever accrues in the meantime
		for _, m := range taken {
			prometheus.MarkChanged(m)
		}
	}
	clear(taken)
	e.taken = taken[:0]
	st.Deleted = e.drop(idle)
	return st
}

// take writes one instance into a dto, works out the value to report, and puts
// it in this scrape's family.
func (e *TrackedExposer) take(m prometheus.Metric, heartbeat bool) (*pendingCommit, bool) {
	e.scratch.Reset()
	if err := m.Write(&e.scratch); err != nil {
		return nil, false
	}
	desc := m.Desc()
	en := e.state[m]
	if en == nil {
		en = &entry{family: desc.Name(), changed: e.round, born: e.rb.clock().Unix()}
		e.state[m] = en
	}
	en.lastRound = e.round

	out := en.out
	if out == nil {
		out = cloneMetric(&e.scratch) // built once per instance, reused every scrape
		en.out = out
	} else {
		refillMetric(out, &e.scratch)
	}
	mf := e.family(desc, out)
	if mf == nil {
		return nil, false
	}
	en.kind = mf.GetType()
	// Capture the labels before decide stamps the generation on them: they are
	// handed to Options.Delete, which looks the child up in its Vec, and the Vec
	// has no generation label. Only needed when there is a callback; without one
	// idle cleanup goes through prometheus.DeleteTracked.
	if en.labels == nil && e.opts.Delete != nil {
		en.labels = labelsOf(out.Label)
	}
	emit, _, pc := e.decide(mf, out, en)
	if !emit && !heartbeat {
		// unchanged, e.g. added and subtracted again within one scrape: skip it,
		// without affecting the next scrape
		mf.Metric = mf.Metric[:len(mf.Metric)-1]
		return pc, true
	}
	return pc, true
}

// family finds or creates the metric family this instance belongs to and adds it.
func (e *TrackedExposer) family(desc *prometheus.Desc, m *dto.Metric) *dto.MetricFamily {
	name := desc.Name()
	mf := e.byName[name]
	if mf == nil {
		typ := metricType(m)
		if typ == nil {
			return nil
		}
		help := desc.Help()
		mf = &dto.MetricFamily{Name: &name, Help: &help, Type: typ}
		e.byName[name] = mf
		e.buf = append(e.buf, mf)
	}
	mf.Metric = append(mf.Metric, m)
	return mf
}

// decide reuses Exposer's change-only and delta logic.
func (e *TrackedExposer) decide(mf *dto.MetricFamily, m *dto.Metric, en *entry) (bool, bool, *pendingCommit) {
	shim := &Exposer{opts: e.opts, round: e.round, rb: e.rb}
	return shim.decide(mf, m, en)
}

func (e *TrackedExposer) commit(pending []pendingCommit) {
	for _, pc := range pending {
		pc.en.value = pc.value
		pc.en.sum = pc.sum
		pc.en.count = pc.count
		if pc.buckets != nil {
			pc.en.spare = pc.en.buckets
			pc.en.buckets = pc.buckets
		}
	}
}

// drop removes idle instances from their Vec, through Options.Delete when one is
// set and through prometheus.DeleteTracked otherwise. Either way the instance
// leaves the tracking table with it.
func (e *TrackedExposer) drop(idle []*entry) int {
	for _, en := range idle {
		if e.opts.Delete != nil {
			e.opts.Delete(en.family, en.labels)
		} else {
			prometheus.DeleteTracked(en.metric)
		}
		delete(e.state, en.metric)
		en.metric = nil
	}
	return len(idle)
}

func metricType(m *dto.Metric) *dto.MetricType {
	var t dto.MetricType
	switch {
	case m.Counter != nil:
		t = dto.MetricType_COUNTER
	case m.Gauge != nil:
		t = dto.MetricType_GAUGE
	case m.Histogram != nil:
		t = dto.MetricType_HISTOGRAM
	case m.Summary != nil:
		t = dto.MetricType_SUMMARY
	case m.Untyped != nil:
		t = dto.MetricType_UNTYPED
	default:
		return nil
	}
	return &t
}

// refillMetric copies the values from scratch into an existing dto instead of
// allocating one per scrape. Labels never change, so they are left alone.
func refillMetric(dst, src *dto.Metric) {
	switch {
	case src.Counter != nil && dst.Counter != nil:
		*dst.Counter.Value = src.Counter.GetValue()
	case src.Gauge != nil && dst.Gauge != nil:
		*dst.Gauge.Value = src.Gauge.GetValue()
	case src.Histogram != nil && dst.Histogram != nil:
		*dst.Histogram.SampleSum = src.Histogram.GetSampleSum()
		*dst.Histogram.SampleCount = src.Histogram.GetSampleCount()
		for i, b := range src.Histogram.Bucket {
			if i < len(dst.Histogram.Bucket) {
				*dst.Histogram.Bucket[i].CumulativeCount = b.GetCumulativeCount()
			}
		}
	case src.Untyped != nil && dst.Untyped != nil:
		*dst.Untyped.Value = src.Untyped.GetValue()
	case src.Summary != nil:
		dst.Summary = src.Summary
	}
}

// cloneMetric copies what is in scratch: scratch is reused for every instance,
// while the output is only encoded once the whole scrape has been collected.
func cloneMetric(src *dto.Metric) *dto.Metric {
	out := &dto.Metric{Label: src.Label}
	switch {
	case src.Counter != nil:
		v := src.Counter.GetValue()
		out.Counter = &dto.Counter{Value: &v, Exemplar: src.Counter.Exemplar, CreatedTimestamp: src.Counter.CreatedTimestamp}
	case src.Gauge != nil:
		v := src.Gauge.GetValue()
		out.Gauge = &dto.Gauge{Value: &v}
	case src.Histogram != nil:
		h := src.Histogram
		sum, count := h.GetSampleSum(), h.GetSampleCount()
		nh := &dto.Histogram{SampleSum: &sum, SampleCount: &count, CreatedTimestamp: h.CreatedTimestamp}
		nh.Bucket = make([]*dto.Bucket, len(h.Bucket))
		for i, b := range h.Bucket {
			c, ub := b.GetCumulativeCount(), b.GetUpperBound()
			nh.Bucket[i] = &dto.Bucket{CumulativeCount: &c, UpperBound: &ub, Exemplar: b.Exemplar}
		}
		out.Histogram = nh
	case src.Summary != nil:
		out.Summary = src.Summary
	case src.Untyped != nil:
		v := src.Untyped.GetValue()
		out.Untyped = &dto.Untyped{Value: &v}
	}
	return out
}
