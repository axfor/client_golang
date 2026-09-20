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

// Package delta provides change-only and delta exposition, for processes with
// very many series of which most stay unchanged for long stretches. Per-API-key
// usage is the motivating case: 30k keys x 37 metrics is 4.4M lines per scrape,
// while only the keys that saw traffic in the last minute actually changed.
//
//	change-only  A series not written since the last delivered scrape is left out.
//	             Unwritten series are topped up on a rotating heartbeat, or never
//	             when DisableHeartbeat is set.
//	delta        Counters and histograms report what accrued since the last
//	             delivered scrape. A scrape that does not arrive is rolled into
//	             the next one. On the collector side, vmagent's sum_samples_total
//	             adds the values back up per series: it does not have to remember
//	             a previous value per instance, so its memory does not grow with
//	             the number of instances and a restart does not have to drop the
//	             first sample as a baseline.
//
// Usage:
//
//	exp := delta.New(reg, delta.Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 30})
//	mux.Handle("/metrics/delta", exp.Handler())
//
// Constraints:
//   - A single scraper only. Delivery state is global, so two scrapers would eat
//     each other's increments.
//   - Summary quantiles are not additive and are passed through unchanged; so are
//     native histograms (see Options.ReportIncrements).
//   - In delta mode the counter values on the wire are no longer cumulative. The
//     collector has to add them up per series.
package delta

import (
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Logger is the subset of promhttp.Logger this package needs. It is declared
// here rather than imported so that promhttp can depend on this package and
// serve delta scrapes from its own handlers.
type Logger interface {
	Println(v ...any)
}

// Kind selects what an exposition carries, so that two endpoints over the same
// metrics can be aggregated differently without the aggregator having to be told
// which metric is which.
//
// A consumer adds up what a counter or a histogram reports and reads a gauge as
// it stands, which are different operations, and the exposition format carries
// no type for the aggregator to switch on -- it has to be told, by name, and the
// list has to be kept up to date as metrics are added. Serving the two kinds at
// two endpoints moves that knowledge back to where it is known: a metric added
// later lands on the right endpoint by its own type, and the aggregator picks
// the arithmetic by which endpoint it scraped.
type Kind int

const (
	// Everything is the default: one endpoint with all of it.
	Everything Kind = iota
	// Accumulating carries counters, histograms, summaries and untyped values --
	// what a consumer adds up.
	Accumulating
	// Current carries gauges -- what a consumer reads as it stands.
	Current
)

// typeName is what TypeLabel carries for this type.
func typeName(t dto.MetricType) string {
	switch t {
	case dto.MetricType_COUNTER:
		return "counter"
	case dto.MetricType_GAUGE:
		return "gauge"
	case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
		return "histogram"
	case dto.MetricType_SUMMARY:
		return "summary"
	}
	return "untyped"
}

// carries reports whether this exposition takes a metric of this type.
func (k Kind) carries(t dto.MetricType) bool {
	switch k {
	case Accumulating:
		return t != dto.MetricType_GAUGE
	case Current:
		return t == dto.MetricType_GAUGE
	}
	return true
}

// Increments returns the options this package recommends for feeding an
// aggregator: report what accrued since the last delivered scrape, say nothing
// at all about an instance nothing wrote, and let an instance that has gone
// quiet for half an hour go.
//
//	exp := delta.NewTracked(trk, delta.Increments())
//
// There is one right combination for that job and assembling it by hand is how
// it goes wrong, so it is one call. Change a field on the result where a
// deployment really differs -- a scrape interval that is not 60s makes
// IdleScrapes mean a different span of time.
func Increments() Options {
	return Options{
		ReportIncrements: true,
		DisableHeartbeat: true,
		IdleScrapes:      30,
	}
}

// WithOnly returns these options limited to one kind of metric, for the second
// of a pair of endpoints:
//
//	counters := delta.NewTracked(trk, delta.Increments().WithOnly(delta.Accumulating))
//	gauges   := delta.New(reg, delta.Options{DisableHeartbeat: true}.WithOnly(delta.Current))
func (o Options) WithOnly(k Kind) Options {
	o.Only = k
	return o
}

// Options controls the exposition. The zero value is change-only, a three-scrape
// heartbeat, and no idle deletion.
type Options struct {
	// ReportIncrements makes counters and histograms report what accrued since
	// the last delivered scrape. A value read out is only committed once the
	// response has been delivered; otherwise it is rolled into the next scrape.
	// Gauges and summaries are not differenced and always carry their current
	// value.
	ReportIncrements bool

	// DisableHeartbeat drops unchanged series entirely. Otherwise each one is
	// topped up every HeartbeatScrapes scrapes, so that the collector and the
	// query side can still find a previous value ahead of a window.
	DisableHeartbeat bool

	// HeartbeatScrapes is the heartbeat rotation length, 3 by default, i.e. three
	// minutes at a 60s scrape interval.
	HeartbeatScrapes int

	// IdleScrapes, when greater than zero, drops a series from the state table
	// after that many scrapes without a write. Together with Delete it also
	// removes the child from its Vec, so process memory follows the active set.
	// A series that reappears starts from zero again, which is exactly right in
	// delta mode.
	//
	// Note that deletion is not coordinated with writes: if a request is holding
	// the old child while Vec.Delete removes it, that write is lost. This is
	// client_golang's own semantics and not specific to this package. A generous
	// threshold (30 at a 60s interval, i.e. half an hour without traffic) makes
	// it very unlikely; leave the option off to rule it out entirely and keep
	// every instance resident.
	IdleScrapes int

	// Delete is called when a series is dropped as idle, to remove the matching
	// child from its *prometheus.CounterVec and friends:
	//
	//	Delete: func(name string, labels prometheus.Labels) {
	//		if v, ok := vecs[name]; ok { v.Delete(labels) }
	//	}
	//
	// On the change-tracking path this can be left nil: the instance knows the
	// Vec that created it, and prometheus.DeleteTracked removes it directly.
	Delete func(name string, labels prometheus.Labels)

	// GenLabel, when set, adds this label to counter and histogram output, with
	// the series' creation time in Unix seconds as its value, so that an instance
	// deleted as idle and written to again is a different series.
	//
	// Without it a rebuilt counter relies on the collector spotting a counter
	// reset, which it cannot do when the new value is not below the old one. The
	// instances that go idle are exactly the low-traffic ones, whose old value is
	// small, so it undercounts.
	GenLabel string

	// RebaseAfterGap, when greater than zero, makes the first scrape after a gap
	// that long move to a new generation and report only what was never
	// delivered: every counter and histogram reports its value minus the one it
	// last delivered. Requires GenLabel, and cannot be combined with
	// ReportIncrements, which needs no generation change.
	//
	// It exists because an aggregator forgets an input with no samples within
	// staleness_interval by wall-clock time, while idle cleanup counts scrapes
	// and does not advance while scraping is down. Without it, a gap longer than
	// staleness_interval makes the whole cumulative value reported after recovery
	// count as a new input, i.e. twice. Keep it below staleness_interval minus
	// one scrape interval.
	RebaseAfterGap time.Duration

	// Only limits what this exposition carries, so that the kinds needing
	// different aggregation can be scraped separately. The default carries
	// everything. See Kind.
	Only Kind
	// TypeLabel, when set, adds this label to every sample, carrying the
	// metric's type -- "counter", "gauge", "histogram", "summary" or "untyped".
	//
	// It solves the same problem as Only, for a consumer that would rather not
	// add a second scrape. An aggregator adds up what a counter reports and
	// reads a gauge as it stands, and the exposition format gives it no type to
	// switch on, so it is told by name -- and a gauge added later matches the
	// same pattern as a counter and is silently accumulated. With this label the
	// aggregation is selected by what the metric is:
	//
	//	- match: '{_metric_type!="gauge"}'
	//	  drop_input_labels: [_metric_type]
	//	  outputs: [sum_samples_total]
	//	- match: '{_metric_type="gauge"}'
	//	  drop_input_labels: [_metric_type]
	//	  outputs: [sum_samples]
	//
	// Two rules, written once, that no metric added later changes.
	//
	// The label is dropped before the aggregator groups, so it does not reach
	// what is stored. It does travel on the wire: a label repeated on every
	// sample is 4.6% of an uncompressed exposition and 0.2% of a zstd-compressed
	// one, measured on 6384 samples of a real one.
	//
	// Do not use a name beginning with two underscores: relabeling drops those
	// before a scrape is handed on, so the label would be gone before the
	// aggregator saw it, and nothing would fail.
	TypeLabel string
	// Gatherer selects the slower path that reads through a Gatherer instead of
	// change tracking. Enable uses it when change tracking cannot be turned on
	// early enough, for instance when metrics already exist.
	Gatherer prometheus.Gatherer

	// ErrorLog receives gathering errors. HTTPErrorOnError answers them with
	// 500 instead of serving what could be gathered.
	ErrorLog         Logger
	HTTPErrorOnError bool
}

// ScrapeStats describes the outcome of one scrape, for tests and monitoring.
type ScrapeStats struct {
	Round     uint64 // scrape number, starting at 1
	Families  int    // metric families written
	Samples   int    // series written
	Heartbeat int    // of those, how many were heartbeat top-ups
	Gauges    int    // of those, how many were gauges reported unchanged
	Deleted   int    // idle series dropped this scrape
	Rebased   bool   // the gap since the last delivery exceeded RebaseAfterGap, so the generation moved
	Delivered bool   // response fully written and the request not cancelled
	Err       error
}

// Exposer holds the last delivered values and the idle counters.
type Exposer struct {
	g      prometheus.Gatherer
	opts   Options
	rb     rebaseState
	cached bool // the caller writes through the cached encoder, not expfmt

	mu     sync.Mutex
	round  uint64
	state  map[string]*entry
	keyBuf []byte // reused series key; m[string(buf)] looks up without allocating
}

type entry struct {
	family    string
	labels    prometheus.Labels
	kind      dto.MetricType
	value     float64           // counter / gauge: last delivered value
	sum       float64           // histogram: last delivered _sum
	count     uint64            // histogram: last delivered _count
	metric    prometheus.Metric // the instance, on the change-tracking path
	buckets   []uint64          // histogram: last delivered cumulative count per le
	spare     []uint64          // buffer rotated with buckets
	changed   uint64            // last scrape whose value differed from the delivered one
	lastRound uint64            // last scrape this was seen in

	born int64 // creation time in Unix seconds, the value of GenLabel

	// The rendered labels of this series, without the closing brace, plus the
	// generation they were rendered under. Shared with every other entry of the
	// same instance, see encState.share.
	rendered    string
	renderedGen int64
	// What only some configurations need, allocated when one of them first
	// does. Stamping the generation on the dto is the Gather path only -- the
	// cached encoder writes it into the label string it keeps -- and the bases
	// only exist under RebaseAfterGap, which ReportIncrements rules out. Holding
	// them inline cost 96 of the entry's 264 bytes on every instance of every
	// configuration, whether or not it had any use for them.
	extra *entryExtra
}

type entryExtra struct {
	genPair    *dto.LabelPair   // the generation label, updated in place
	genLabels  []*dto.LabelPair // the labels with the generation appended, built once
	genStamped int64            // the generation genPair currently carries

	basedOn     int64 // the generation the bases below belong to
	baseValue   float64
	baseSum     float64
	baseCount   uint64
	baseBuckets []uint64
}

func (en *entry) ext() *entryExtra {
	if en.extra == nil {
		en.extra = &entryExtra{}
	}
	return en.extra
}

// New returns an Exposer reading from g.
func New(g prometheus.Gatherer, opts Options) *Exposer {
	opts = check(opts)
	return &Exposer{g: g, opts: opts, state: map[string]*entry{}}
}

// check fills in the defaults and rejects combinations that would silently
// miscount.
func check(opts Options) Options {
	if opts.HeartbeatScrapes <= 0 {
		opts.HeartbeatScrapes = 3
	}
	if opts.RebaseAfterGap > 0 && opts.GenLabel == "" {
		panic("delta: RebaseAfterGap requires GenLabel")
	}
	if opts.ReportIncrements && opts.GenLabel != "" {
		panic("delta: ReportIncrements and GenLabel are mutually exclusive; an increment is already right across a restart or a deletion, so the generation buys nothing and has to be aggregated away again")
	}
	if opts.ReportIncrements && opts.RebaseAfterGap > 0 {
		panic("delta: ReportIncrements and RebaseAfterGap are mutually exclusive; reporting increments needs no generation change")
	}
	// A gauge is never differenced, so an idle one the aggregator has forgotten
	// cannot be counted twice -- the check below is about accumulated values and
	// does not apply to an exposition that carries none.
	if opts.Only != Current && opts.DisableHeartbeat && !opts.ReportIncrements && (opts.IdleScrapes <= 0 || opts.GenLabel == "") {
		panic("delta: DisableHeartbeat without ReportIncrements requires IdleScrapes > 0 and GenLabel, or an idle instance the aggregator has forgotten is counted twice")
	}
	return opts
}

// Handler returns an http.Handler.
func (e *Exposer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { e.Serve(w, r) })
}

// Serve writes one scrape and returns its stats.
func (e *Exposer) Serve(w http.ResponseWriter, r *http.Request) ScrapeStats {
	e.mu.Lock()
	defer e.mu.Unlock()

	st := ScrapeStats{}
	mfs, err := e.g.Gather()
	if err != nil {
		st.Err = err
		if e.opts.ErrorLog != nil {
			e.opts.ErrorLog.Println("error gathering metrics:", err)
		}
		if e.opts.HTTPErrorOnError {
			http.Error(w, "An error has occurred while serving metrics:\n\n"+err.Error(), http.StatusInternalServerError)
			return st
		}
	}

	e.round++
	st.Round = e.round
	st.Rebased = e.rb.due(e.opts.RebaseAfterGap)
	out, pending, heartbeat := e.plan(mfs)
	st.Families, st.Heartbeat = len(out), heartbeat
	for _, mf := range out {
		st.Samples += len(mf.Metric)
	}

	rs := newResponse(w, r)
	enc := expfmt.NewEncoder(rs.w, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range out {
		if err := enc.Encode(mf); err != nil {
			st.Err = err
			break
		}
	}
	st.Err, st.Delivered = rs.close(st.Err, r)

	if st.Delivered {
		e.commit(pending)
		e.rb.delivered()
	}
	st.Deleted = e.sweep()
	return st
}

// plan picks the series to write and the values to write for them, and records
// what to commit once the response is delivered.
func (e *Exposer) plan(mfs []*dto.MetricFamily) (out []*dto.MetricFamily, pending []pendingCommit, heartbeat int) {
	for _, mf := range mfs {
		if !e.opts.Only.carries(mf.GetType()) {
			continue
		}
		kept := mf.Metric[:0]
		for _, m := range mf.Metric {
			e.keyBuf = appendSeriesKey(e.keyBuf[:0], mf.GetName(), m.Label)
			en := e.state[string(e.keyBuf)]
			if en == nil {
				en = &entry{family: mf.GetName(), kind: mf.GetType(), changed: e.round, born: e.rb.clock().Unix()}
				if e.opts.Delete != nil {
					en.labels = labelsOf(m.Label)
				}
				e.state[string(e.keyBuf)] = en
			}
			en.lastRound = e.round

			emit, hb, pc := e.decide(mf, m, en)
			if hb {
				heartbeat++
			}
			if pc != nil {
				pending = append(pending, *pc)
			}
			if emit {
				kept = append(kept, m)
			}
		}
		if len(kept) > 0 {
			mf.Metric = kept
			out = append(out, mf)
		}
	}
	return out, pending, heartbeat
}

type pendingCommit struct {
	en      *entry
	value   float64
	sum     float64
	count   uint64
	buckets []uint64
}

// decide works out whether a series is written this scrape and with what value.
// It returns: whether to write it, whether that is a heartbeat top-up, and what
// to commit once delivered (nil when there is nothing to commit).
func (e *Exposer) decide(mf *dto.MetricFamily, m *dto.Metric, en *entry) (bool, bool, *pendingCommit) {
	switch mf.GetType() {
	case dto.MetricType_COUNTER:
		cur := m.GetCounter().GetValue()
		changed := cur != en.value
		if changed {
			en.changed = e.round
		}
		if !changed && !e.heartbeatTurn(en) {
			return false, false, nil
		}
		e.rb.rebaseEntry(en)
		if e.opts.ReportIncrements {
			*m.Counter.Value = cur - en.value // in place: Gather returns a fresh copy
		} else if e.rb.gen != 0 {
			*m.Counter.Value = cur - en.ext().baseValue
		}
		e.stamp(mf, m, en)
		return true, !changed, &pendingCommit{en: en, value: cur}

	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		if isNative(h) {
			// A native histogram is not differenced and cannot be compared on a
			// single value: pass it through.
			return true, false, nil
		}
		sum, count := h.GetSampleSum(), h.GetSampleCount()
		buckets := en.spare[:0]
		if cap(buckets) < len(h.Bucket) {
			buckets = make([]uint64, 0, len(h.Bucket))
		}
		for _, b := range h.Bucket {
			buckets = append(buckets, b.GetCumulativeCount())
		}
		changed := count != en.count || sum != en.sum
		if changed {
			en.changed = e.round
		}
		if !changed && !e.heartbeatTurn(en) {
			return false, false, nil
		}
		e.rb.rebaseEntry(en)
		if e.opts.ReportIncrements {
			*h.SampleSum = sum - en.sum
			*h.SampleCount = count - en.count
			for i, b := range h.Bucket {
				var prev uint64
				if i < len(en.buckets) {
					prev = en.buckets[i]
				}
				*b.CumulativeCount = b.GetCumulativeCount() - prev
			}
		} else if e.rb.gen != 0 {
			*h.SampleSum = sum - en.ext().baseSum
			*h.SampleCount = count - en.ext().baseCount
			for i, b := range h.Bucket {
				*b.CumulativeCount = b.GetCumulativeCount() - en.baseBucket(i)
			}
		}
		e.stamp(mf, m, en)
		return true, !changed, &pendingCommit{en: en, sum: sum, count: count, buckets: buckets}

	case dto.MetricType_GAUGE:
		// A gauge is a current value: never differenced, always written. Only the
		// idle bookkeeping looks at whether it changed.
		cur := m.GetGauge().GetValue()
		if cur != en.value {
			en.changed = e.round
		}
		return true, false, &pendingCommit{en: en, value: cur}

	default:
		// summary / untyped: quantiles are not additive, pass them through.
		return true, false, nil
	}
}

// stamp puts the generation label on a counter or histogram. Gauges never carry
// it: they are current values, not something that accumulates across a rebuild.
func (e *Exposer) stamp(mf *dto.MetricFamily, m *dto.Metric, en *entry) {
	if e.opts.GenLabel == "" {
		return
	}
	if e.cached {
		// The cached encoder puts the generation in the label string it keeps, so
		// it is not carried on the dto, which would cost a label slice per series.
		// Whether that encoder actually takes the family is only known once the
		// family is complete, so a fallback stamps there instead; see stampFallback.
		return
	}
	gen := e.rb.genOf(en)
	x := en.ext()
	if x.genPair == nil {
		// Built once. The label slice handed out by Write belongs to the metric
		// itself, so it is copied rather than appended to.
		name, value := e.opts.GenLabel, strconv.FormatInt(gen, 10)
		x.genPair = &dto.LabelPair{Name: &name, Value: &value}
		at, replace := genIndex(m.Label, name)
		x.genLabels = withLabel(m.Label, x.genPair, at, replace)
		x.genStamped = gen
	} else if x.genStamped != gen {
		value := strconv.FormatInt(gen, 10)
		x.genPair.Value = &value
		x.genStamped = gen
	}
	// Write resets the labels to the metric's own every scrape, so put the
	// stamped set back.
	m.Label = x.genLabels
}

// deletable reports whether an idle series may be dropped.
//
// A gauge is only droppable at zero. An Inc/Dec gauge such as a count of
// in-flight requests sits at a non-zero value for as long as the request runs,
// without being written again; deleting it there would leave the Dec that ends
// the request to land on an instance rebuilt from zero and send the gauge
// negative.
func deletable(en *entry) bool {
	return en.kind != dto.MetricType_GAUGE || en.value == 0
}

// heartbeatTurn reports whether an unchanged series is due for a top-up.
func (e *Exposer) heartbeatTurn(en *entry) bool {
	if e.opts.DisableHeartbeat {
		return false
	}
	return int(e.round%uint64(e.opts.HeartbeatScrapes)) == int(hash(en.family, en.labels)%uint64(e.opts.HeartbeatScrapes))
}

// commit records the values read this scrape as the last delivered ones.
func (e *Exposer) commit(pending []pendingCommit) {
	for _, pc := range pending {
		pc.en.value = pc.value
		pc.en.sum = pc.sum
		pc.en.count = pc.count
		if pc.buckets != nil {
			pc.en.spare = pc.en.buckets // rotate the two buffers instead of reallocating
			pc.en.buckets = pc.buckets
		}
	}
}

// sweep drops series with no write for IdleScrapes scrapes in a row.
func (e *Exposer) sweep() int {
	idle := uint64(e.opts.IdleScrapes)
	if idle == 0 {
		return 0
	}
	n := 0
	for k, en := range e.state {
		if en.lastRound == e.round && e.round-en.changed > idle && deletable(en) {
			delete(e.state, k)
			if e.opts.Delete != nil {
				e.opts.Delete(en.family, en.labels)
			}
			n++
		}
	}
	return n
}

// appendSeriesKey appends the series identity to dst. Labels from Gather are
// already sorted by name, so appending them in order is enough.
func appendSeriesKey(dst []byte, name string, labels []*dto.LabelPair) []byte {
	dst = append(dst, name...)
	for _, l := range labels {
		dst = append(dst, 0)
		dst = append(dst, l.GetName()...)
		dst = append(dst, '=')
		dst = append(dst, l.GetValue()...)
	}
	return dst
}

func labelsOf(pairs []*dto.LabelPair) prometheus.Labels {
	if len(pairs) == 0 {
		return nil
	}
	l := make(prometheus.Labels, len(pairs))
	for _, p := range pairs {
		l[p.GetName()] = p.GetValue()
	}
	return l
}

func hash(name string, labels prometheus.Labels) uint64 {
	h := uint64(14695981039346656037)
	add := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= 1099511628211
		}
	}
	add(name)
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		add(k)
		add(labels[k])
	}
	return h
}

func isNative(h *dto.Histogram) bool {
	return h.GetSchema() != 0 || len(h.PositiveSpan) > 0 || len(h.NegativeSpan) > 0
}
