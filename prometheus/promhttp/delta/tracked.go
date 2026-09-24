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
	"unsafe"

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

	mu    sync.Mutex
	round uint64
	// Each instance's entry is kept in the instance's own tracker slot, see
	// entryOf, rather than in a map from instance to entry: at two million
	// instances such a map is about 40 bytes an instance on top of the entries.
	live int // entries created and not dropped as idle, for Tracked
	// scratch is the dto each family's instances are written into, one after
	// another, see tracked_emit.go. One per family rather than one for all, so
	// that Write finds the family's shape already there and updates it in place
	// instead of allocating a counter, or a histogram and all its buckets, for
	// every instance of every scrape.
	scratch map[string]*dto.Metric
	taken   []prometheus.Metric
	gauges  []prometheus.Metric // every gauge seen, walked in full every scrape

	// The instances taken this scrape, by family, in the order the families
	// were first seen; slots past nslots are left over from earlier scrapes.
	slots  []familySlot
	nslots int
	byName map[string]int // family name to its index in slots
	fam    familyRows     // the family being written
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
		scratch: map[string]*dto.Metric{},
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
	return e.live
}

// entryOf returns m's entry, nil when it has none yet, and the slot it is kept
// in, nil when m is no longer tracked.
func entryOf(m prometheus.Metric) (*entry, *unsafe.Pointer) {
	slot := prometheus.TrackerSlot(m)
	if slot == nil {
		return nil, nil
	}
	return (*entry)(*slot), slot
}

// idleEntry is an instance idle cleanup is about to drop.
type idleEntry struct {
	m    prometheus.Metric
	slot *unsafe.Pointer
	en   *entry
}

// minShrink is the capacity below which giving an array back is not worth the
// copy.
const minShrink = 1 << 6

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

	clear(e.byName)
	e.nslots = 0
	e.taken = e.taken[:0]
	var idle []idleEntry

	// First pass: take what is due, grouped by family, without writing any of it.
	//
	// everything written since the previous scrape
	e.tracker.Take(func(m prometheus.Metric) { e.pick(m, pickChanged) })

	// the bucket due this scrape: top up unchanged series, list idle ones to drop
	if !e.opts.DisableHeartbeat || e.opts.IdleScrapes > 0 {
		bucket := int(e.round % uint64(e.opts.HeartbeatScrapes))
		e.tracker.RangeBucket(bucket, e.opts.HeartbeatScrapes, func(m prometheus.Metric) {
			en, slot := entryOf(m)
			if en == nil {
				return // a new instance not taken yet; leave it for the next scrape
			}
			if e.opts.IdleScrapes > 0 && e.round-en.changed > uint64(e.opts.IdleScrapes) && deletable(en) {
				idle = append(idle, idleEntry{m: m, slot: slot, en: en})
				en.lastRound = e.round // handled this scrape: do not also top it up below
				return
			}
			if !e.opts.DisableHeartbeat && en.lastRound != e.round {
				e.pick(m, pickHeartbeat)
			}
		})
	}

	// Gauges are current values: one left out is a gap for the consumer, not a
	// saving, so every gauge goes out every scrape regardless of DisableHeartbeat.
	// This is what the Gather path does too, see Exposer.decide.
	for i := 0; e.opts.Only.carries(dto.MetricType_GAUGE) && i < len(e.gauges); {
		m := e.gauges[i]
		en, _ := entryOf(m)
		if en == nil { // dropped as idle or deleted; a new child registers itself again
			e.gauges[i] = e.gauges[len(e.gauges)-1]
			e.gauges[len(e.gauges)-1] = nil
			e.gauges = e.gauges[:len(e.gauges)-1]
			continue
		}
		if en.lastRound != e.round {
			e.pick(m, pickGauge)
		}
		i++
	}

	// Second pass: write, decide and encode one family at a time.
	rs := newResponse(w, r)
	enc := expfmt.NewEncoder(rs.w, expfmt.NewFormat(expfmt.TypeTextPlain))
	var pending []pendingCommit
	for i := 0; i < e.nslots && st.Err == nil; i++ {
		st.Err = e.emitFamily(&e.slots[i], rs.w, enc, &st, &pending)
	}
	e.enc.resetIntern()
	st.Err, st.Delivered = rs.close(st.Err, r)

	if st.Delivered {
		e.commit(pending)
		e.rb.delivered()
	} else {
		// undelivered: put them back so the next scrape reports them again,
		// together with whatever accrues in the meantime
		for _, m := range e.taken {
			prometheus.MarkChanged(m)
		}
	}

	// The scrape's scratch goes with the scrape. It used to be kept for the next
	// one, sized to a decaying high-water mark of recent scrapes, to save
	// reallocating it once a minute; but kept, it is live heap for the whole
	// minute in between, and under the default GOGC the collector lets the heap
	// grow to twice what is live. At 2.1M instances with a quarter changing per
	// scrape these arrays were over 100 MiB of each sidecar.
	for i := range e.slots {
		e.slots[i].picks = nil
		if i >= e.nslots {
			e.slots[i].desc = nil
		}
	}
	e.fam.rows, e.fam.nums = nil, nil
	e.taken = nil
	st.Deleted = e.drop(idle)
	return st
}

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
			pc.en.buckets = pc.buckets
		}
	}
}

// drop removes idle instances from their Vec, through Options.Delete when one is
// set and through prometheus.DeleteTracked otherwise, and lets go of their
// entries.
func (e *TrackedExposer) drop(idle []idleEntry) int {
	for _, x := range idle {
		if e.opts.Delete != nil {
			family, labels := x.m.Desc().Name(), prometheus.Labels(nil)
			if d := x.en.del; d != nil {
				family, labels = d.family, d.labels
			}
			e.opts.Delete(family, labels)
		} else {
			prometheus.DeleteTracked(x.m)
		}
		// Cleared whichever way it went: a Delete that left the child in place
		// must see it start over as new, as it did when the entry left a map.
		*x.slot = nil
		e.live--
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
