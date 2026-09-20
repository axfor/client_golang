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
	rows    [][]*entry // rows[i] are the entries behind buf[i].Metric, in order
	taken   []prometheus.Metric
	gauges  []prometheus.Metric // every gauge seen, walked in full every scrape

	// A dto is only needed for the instances written in one scrape, which at a
	// realistic active share is a small part of what is registered. They are
	// lent out per scrape instead of kept per instance, from a free list per
	// family so that the one handed out already has the family's bucket layout
	// and Write updates it in place.
	free   map[string][]*dto.Metric
	lent   []loan
	peak   map[string]int // recent high-water mark of borrows, so the free list can fall back
	byName map[string]int // family name to its index in buf
	enc    *encState
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
		free:    map[string][]*dto.Metric{},
		peak:    map[string]int{},
		byName:  map[string]int{},
		enc:     newEncState(),
	}
}

// Tracked returns how many instances this exposer is holding state for. It is
// the number idle cleanup shrinks, so a caller watching memory can tell an
// instance that was dropped from one that is merely quiet -- reading it off a
// Gather instead walks every instance in the registry, which is the cost the
// dirty list exists to avoid.
func (e *TrackedExposer) Tracked() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.state)
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

	for i := range e.rows {
		e.rows[i] = e.rows[i][:0]
	}
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
				en.lastRound = e.round // handled this scrape: do not also top it up below
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

	// Gauges are current values: one left out is a gap for the consumer, not a
	// saving, so every gauge goes out every scrape regardless of DisableHeartbeat.
	// This is what the Gather path does too, see Exposer.decide.
	for i := 0; e.opts.Only.carries(dto.MetricType_GAUGE) && i < len(e.gauges); {
		m := e.gauges[i]
		en := e.state[m]
		if en == nil { // dropped as idle; a new child registers itself again
			e.gauges[i] = e.gauges[len(e.gauges)-1]
			e.gauges[len(e.gauges)-1] = nil
			e.gauges = e.gauges[:len(e.gauges)-1]
			continue
		}
		if en.lastRound != e.round {
			if pc, ok := e.take(m, true); ok {
				st.Gauges++
				taken = append(taken, m)
				if pc != nil {
					pending = append(pending, *pc)
				}
			}
		}
		i++
	}

	for _, mf := range e.buf {
		st.Samples += len(mf.Metric)
	}
	st.Families = len(e.buf)

	rs := newResponse(w, r)
	enc := expfmt.NewEncoder(rs.w, expfmt.NewFormat(expfmt.TypeTextPlain))
	st.Err = encodeFamilies(rs.w, enc, e.buf, e.rows, e.rb.genOf, e.enc, e.opts.GenLabel, e.opts.TypeLabel)
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
	e.returnAll()

	clear(taken)
	e.taken = taken[:0]
	st.Deleted = e.drop(idle)
	return st
}

// take writes one instance into a dto, works out the value to report, and puts
// it in this scrape's family.
func (e *TrackedExposer) take(m prometheus.Metric, heartbeat bool) (*pendingCommit, bool) {
	desc := m.Desc()
	en := e.state[m]
	fresh := en == nil
	if fresh {
		en = &entry{family: desc.Name(), changed: e.round, born: e.rb.clock().Unix()}
		e.state[m] = en
	}
	en.lastRound = e.round

	// Write into a dto borrowed for this scrape rather than a shared scratch: one
	// a previous Write already filled in is updated in place, so a steady-state
	// scrape allocates nothing here. An empty free list means there is nothing to
	// update, so it goes through the scratch and keeps the copy.
	name := desc.Name()
	out := e.borrow(name)
	if out == nil {
		e.scratch.Reset()
		if err := m.Write(&e.scratch); err != nil {
			return nil, false
		}
		out = cloneMetric(&e.scratch)
		e.lent = append(e.lent, loan{name, out})
	} else if err := m.Write(out); err != nil {
		return nil, false
	}
	if typ := metricType(out); typ == nil || !e.opts.Only.carries(*typ) {
		return nil, true // not this endpoint's kind; nothing taken, nothing lost
	}
	mf, idx := e.family(desc, out, en)
	if mf == nil {
		return nil, false
	}
	en.kind = mf.GetType()
	if fresh && en.kind == dto.MetricType_GAUGE {
		// Gauges are reported in full every scrape, which the bucketed walk over
		// the tracker cannot do, so they get a list of their own. Entries dropped
		// as idle are pruned from it lazily in Serve.
		e.gauges = append(e.gauges, m)
	}
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
		e.rows[idx] = e.rows[idx][:len(e.rows[idx])-1]
		return pc, true
	}
	return pc, true
}

// family finds or creates the metric family this instance belongs to, adds it,
// and returns the family together with its index, which the caller needs to
// keep the parallel entry rows in step.
func (e *TrackedExposer) family(desc *prometheus.Desc, m *dto.Metric, en *entry) (*dto.MetricFamily, int) {
	name := desc.Name()
	idx, ok := e.byName[name]
	if !ok {
		typ := metricType(m)
		if typ == nil {
			return nil, 0
		}
		help := desc.Help()
		idx = len(e.buf)
		e.buf = append(e.buf, &dto.MetricFamily{Name: &name, Help: &help, Type: typ})
		if idx == len(e.rows) {
			e.rows = append(e.rows, nil)
		}
		e.rows[idx] = e.rows[idx][:0]
		e.byName[name] = idx
	}
	mf := e.buf[idx]
	mf.Metric = append(mf.Metric, m)
	e.rows[idx] = append(e.rows[idx], en)
	return mf, idx
}

// loan is a dto handed out for one scrape, and the family to put it back under.
type loan struct {
	family string
	m      *dto.Metric
}

// borrow takes a dto off the family's free list, or returns nil when it is
// empty. What it hands out is recorded so that returnAll can take it back once
// the response has been written.
func (e *TrackedExposer) borrow(family string) *dto.Metric {
	l := e.free[family]
	if len(l) == 0 {
		return nil
	}
	out := l[len(l)-1]
	e.free[family] = l[:len(l)-1]
	e.lent = append(e.lent, loan{family, out})
	return out
}

// returnAll puts this scrape's dtos back, and lets the free lists fall back
// towards what recent scrapes actually used. Without that they would stay at
// their high-water mark, which is the first scrape after start-up: every
// instance is new then, so every one is written, and the lists would keep a dto
// per instance for the rest of the process -- the very thing lending them
// avoids. The mark decays by an eighth a scrape, so a burst is given up over a
// few minutes while a steady load never has to reallocate.
func (e *TrackedExposer) returnAll() {
	used := map[string]int{}
	for i, l := range e.lent {
		e.free[l.family] = append(e.free[l.family], l.m)
		used[l.family]++
		e.lent[i].m = nil
	}
	e.lent = e.lent[:0]
	for family, l := range e.free {
		mark := e.peak[family] - e.peak[family]/8
		if used[family] > mark {
			mark = used[family]
		}
		e.peak[family] = mark
		if keep := mark + mark/4; len(l) > keep {
			clear(l[keep:])
			e.free[family] = l[:keep]
		}
	}
}

// decide reuses Exposer's change-only and delta logic.
func (e *TrackedExposer) decide(mf *dto.MetricFamily, m *dto.Metric, en *entry) (bool, bool, *pendingCommit) {
	shim := &Exposer{opts: e.opts, round: e.round, rb: e.rb, cached: true}
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
