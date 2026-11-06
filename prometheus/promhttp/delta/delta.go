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
//	             the next one, and so is one the scraper says it did not process,
//	             see ack.go. On the collector side, vmagent's sum_samples_total
//	             adds the values back up per series: it does not have to remember
//	             a previous value per instance, so its memory does not grow with
//	             the number of instances and a restart does not have to drop the
//	             first sample as a baseline.
//
// Usage:
//
//	opts := delta.Increments()
//	opts.Vecs = []delta.Vec{tokens, cost} // what idle cleanup deletes from
//	exp := delta.New(reg, opts)
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

	// IdleScrapes, when greater than zero, deletes a series after that many
	// scrapes without a write: its child leaves its Vec and its state leaves
	// the exposer, so process memory follows the active set. A series that
	// reappears starts from zero again, which is exactly right in delta mode.
	// A series whose child is not deleted keeps its state: dropping the state
	// alone would make the next scrape report the child's whole value again.
	//
	// On the change-tracking path an instance is deleted only if it has not
	// been written since the scrape took it, checked under the lock of its Vec,
	// so a request that looks the key up while the scrape is under way keeps its
	// write; a caller that looked it up before and writes to it after has that
	// write settled, see prometheus.ReviveTracked.
	//
	// On the Gather path a child is deleted through Vecs, only if it still
	// holds what the scrape gathered, checked under the lock of its Vec; through
	// Delete, which cannot check, so a write between Gather and the deletion is
	// lost with the child; and with neither, not at all. A caller that looked a
	// child up before the deletion and writes to it after loses that write --
	// never keep children of a Vec this deletes from. The Gather path also lets
	// go of the state of a series it has not gathered for IdleScrapes scrapes:
	// its child is gone, deleted or its registry replaced, and one that comes
	// back is new and reported from zero.
	//
	// A generous threshold (30 at a 60s interval, i.e. half an hour without
	// traffic) makes what is left very unlikely; leave the option off to rule
	// it out and keep every instance resident.
	IdleScrapes int

	// Delete is called when a series is dropped as idle, to remove the matching
	// child from its *prometheus.CounterVec and friends:
	//
	//	Delete: func(name string, labels prometheus.Labels) {
	//		if v, ok := vecs[name]; ok { v.Delete(labels) }
	//	}
	//
	// On the change-tracking path this can be left nil: the instance knows the
	// Vec that created it, and prometheus.DeleteTracked removes it directly. A
	// child the callback leaves in its Vec keeps its state there.
	//
	// On the Gather path set Vecs instead, which checks what Delete cannot:
	// that nothing was written since the scrape, and that the child really went.
	// A child Delete leaves in place -- of a family it does not know, a plain
	// Counter, or a Vec with const labels, which the labels it is given include
	// and Vec.Delete rejects -- has its state dropped all the same there, and
	// the next scrape reports its whole value again.
	Delete func(name string, labels prometheus.Labels)

	// Vecs are what idle cleanup deletes children from on the Gather path, each
	// matched to the family it gathers as by its name:
	//
	//	Vecs: []delta.Vec{tokens, cost, latency},
	//
	// A child is deleted only if it still holds what the scrape gathered,
	// checked under the lock of its Vec, so a write between Gather and the
	// deletion keeps it. An idle series of a family with no Vec here -- one
	// left out, a plain Counter, a family a wrapping Registerer renamed -- is
	// kept, state and all, and the family is logged once to ErrorLog. It cannot
	// be combined with Delete. The change-tracking path deletes through the
	// instance and ignores it. See also Exposer.Replace.
	Vecs []Vec

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
	//	- match: '{_metric_type!="gauge",_metric_type!=""}'
	//	  drop_input_labels: [_metric_type]
	//	  outputs: [sum_samples_total]
	//	- match: '{_metric_type="gauge"}'
	//	  drop_input_labels: [_metric_type]
	//	  outputs: [sum_samples]
	//
	// Two rules, written once, that no metric added later changes.
	//
	// The _metric_type!="" on the first rule is not redundant: a selector reads
	// an absent label as empty, so without it the rule also takes every sample
	// that carries no type at all -- the plain endpoints on the same aggregator,
	// and everything else it scrapes -- and adds each cumulative value it sees
	// on top of the last. Every sample from here carries a non-empty type, so
	// that one condition is exactly "came from a typed endpoint".
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
	// Scope limits the change-tracking path to the metrics registered with it.
	// Change tracking sees every metric of the process, so without it the
	// exposition carries, and idle cleanup deletes from their Vecs, the metrics
	// of every other registry too: a framework counter quiet for IdleScrapes
	// scrapes would vanish from its own endpoint, and a child some code keeps
	// would lose every write after that. Set it to the registry the usage
	// metrics are registered with. The Gather path reads its Gatherer only and
	// ignores it.
	Scope *prometheus.Registry
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
	Forgotten int    // series not gathered for IdleScrapes scrapes whose state was let go (Gather path)
	Rebased   bool   // the gap since the last delivery exceeded RebaseAfterGap, so the generation moved
	Delivered bool   // response fully written and the request not cancelled
	// Aborted: the response broke off midway, so the connection was aborted
	// rather than the response ended, see abort. Nothing of it is committed.
	Aborted bool
	// Settled is what became of the previous round, held for the scraper to
	// acknowledge, when this request came in; see ack.go.
	Settled Settlement
	Err     error
}

// Settlement is what became of a round held for acknowledgement.
type Settlement int

const (
	SettledNone       Settlement = iota // no round was held
	SettledCommitted                    // acknowledged, or the request did not say: committed
	SettledRolledBack                   // the scraper did not process it: reported again
)

// Vec is a Vec idle cleanup can delete children from on the Gather path, see
// Options.Vecs. *prometheus.CounterVec, GaugeVec, HistogramVec, SummaryVec and
// MetricVec all are one.
type Vec interface {
	prometheus.Collector
	DeleteCollectedIf(labels []*dto.LabelPair, ok func(prometheus.Metric) bool) bool
}

// Exposer holds the last delivered values and the idle counters.
type Exposer struct {
	g      prometheus.Gatherer
	opts   Options
	rb     rebaseState
	cached bool // the caller writes through the cached encoder, not expfmt

	mu     sync.Mutex
	round  uint64
	ack    ackState
	held   []pendingCommit // the round awaiting acknowledgement, see ack.go
	state  map[string]*entry
	keyBuf []byte          // reused series key; m[string(buf)] looks up without allocating
	vecs   map[string]Vec  // Options.Vecs by the name of the family each gathers as
	kept   map[string]bool // families logged for having idle series nothing can delete
}

type entry struct {
	kind dto.MetricType
	// Creation time in Unix seconds, the value of GenLabel. Seconds fit in 32
	// bits until 2106, and next to kind they fill the word kind was padded to:
	// that is what takes the entry from 120 bytes to 112, a size class down.
	born uint32

	value     float64 // counter / gauge: last delivered value
	changed   uint64  // last scrape whose value differed from the delivered one
	lastRound uint64  // last scrape this was seen in
	// picked is the last scrape that picked it, see TrackedExposer.pick: an
	// instance goes out once in a scrape. It fills the entry to 64 bytes, the
	// size class it was in.
	picked uint64

	// A histogram's last delivered sum, count and buckets, allocated when it is
	// first committed. Most instances are counters: inline, the 40 bytes put
	// every entry a size class up, 96 bytes instead of 64.
	hist *histDelivered

	// What Options.Delete is called with; nil unless one is set. The family and
	// labels were held inline on every entry, 24 bytes, for a callback most
	// configurations never set.
	del *deleteKey

	// What only some configurations need, allocated when one of them first
	// does. Stamping the generation on the dto is the Gather path only -- the
	// cached encoder writes it into the label string it keeps -- and the bases
	// only exist under RebaseAfterGap, which ReportIncrements rules out. Holding
	// them inline cost 96 of the entry's 264 bytes on every instance of every
	// configuration, whether or not it had any use for them.
	extra *entryExtra
}

type deleteKey struct {
	family string
	labels prometheus.Labels
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
	return &Exposer{g: g, opts: opts, state: map[string]*entry{}, ack: newAckState(), vecs: indexVecs(opts.Vecs, opts.Delete != nil)}
}

// Replace makes the exposer read from g from the next scrape on, and forgets
// the series it was keeping state for: what they delivered was the old
// metrics', and the new ones report from zero. vecs take the place of
// Options.Vecs; they are the new registry's.
//
// It is for replacing a registry with a new one whose metrics all start from
// zero. Without it, a series the new registry has again, under the same name
// and labels, is reported against what the old one delivered: its first
// increment comes out short or negative, and the old series that do not come
// back are kept until IdleScrapes scrapes later. Swapping a Gatherer of one's
// own and then telling the exposer would let a scrape fall in between, read
// against the wrong state; Replace does both in one step.
//
// A series the new Gatherer shares with the old one -- the same metric, still
// counting from where it was -- is reported in full again, so replace only
// what is replaced throughout.
func (e *Exposer) Replace(g prometheus.Gatherer, vecs ...Vec) {
	idx := indexVecs(vecs, e.opts.Delete != nil)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.g, e.vecs = g, idx
	// A round still held for acknowledgement commits into entries no longer
	// here, which is harmless: the old metrics are gone with them.
	e.state = map[string]*entry{}
}

// Tracked returns how many series this exposer is holding state for. It falls
// as idle series are deleted and as series the Gatherer no longer returns are
// forgotten.
func (e *Exposer) Tracked() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.state)
}

// indexVecs indexes vecs by the name of the family each gathers as.
func indexVecs(vecs []Vec, withDelete bool) map[string]Vec {
	if len(vecs) == 0 {
		return nil
	}
	if withDelete {
		panic("delta: Options.Vecs and Options.Delete are mutually exclusive; Vecs deletes what Delete would, and checks that nothing was written since the scrape")
	}
	idx := make(map[string]Vec, len(vecs))
	for _, v := range vecs {
		ch := make(chan *prometheus.Desc)
		go func() { v.Describe(ch); close(ch) }()
		var names []string
		for d := range ch {
			names = append(names, d.Name())
		}
		if len(names) != 1 {
			panic("delta: a Vec in Options.Vecs describes " + strconv.Itoa(len(names)) + " metrics, not one")
		}
		if _, dup := idx[names[0]]; dup {
			panic("delta: two Vecs in Options.Vecs gather as " + names[0])
		}
		idx[names[0]] = v
	}
	return idx
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
	// A round rolled back needs nothing more: its values were never committed,
	// so this round, read against the same ones, reports them again.
	if held, commit := e.ack.settle(r); held {
		if commit {
			e.commit(e.held)
			st.Settled = SettledCommitted
		} else {
			st.Settled = SettledRolledBack
		}
		e.held = nil
	}
	if final(r) {
		// the scraper's last acknowledgement: nothing more is taken
		w.Header().Set(FeaturesHeader, features)
		w.WriteHeader(http.StatusNoContent)
		return st
	}

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
	out, pending, idle, heartbeat := e.plan(mfs)
	st.Families, st.Heartbeat = len(out), heartbeat
	for _, mf := range out {
		st.Samples += len(mf.Metric)
	}
	// Idle series are dropped now, right after they were read, rather than once
	// the response is out. Options.Delete cannot tell whether one was written
	// since it was read, and a write in between is lost with it, so that window
	// is kept to the time since Gather; Options.Vecs closes it. A peek drops
	// nothing. Only a Gather without errors says a series is gone.
	if !peeks(r) {
		st.Deleted, st.Forgotten = e.sweep(idle, err == nil)
	}

	round := e.ack.roundID(e.round)
	w.Header().Set(RoundHeader, round)
	w.Header().Set(FeaturesHeader, features)
	rs := newResponse(w, r)
	enc := expfmt.NewEncoder(rs.w, expfmt.NewFormat(expfmt.TypeTextPlain))
	gatherErr := st.Err
	for _, mf := range out {
		if err := enc.Encode(mf); err != nil {
			st.Err = err
			st.Aborted = true
			break
		}
	}
	if st.Aborted {
		rs.discard()
		abort(w)
		return st
	}
	// A response written in full despite a Gather error carries what could be
	// gathered and is delivered for what it carries. Counting it undelivered, as
	// this did, left its values uncommitted, and the next scrape reported them
	// again on top of what this one did.
	cerr, delivered := rs.close(nil, r)
	st.Delivered = delivered
	if cerr != nil || gatherErr == nil {
		st.Err = cerr
	}

	switch {
	case peeks(r):
		// Looked at, not processed: nothing is committed, and the next round,
		// read against the same values, reports it all again.
	case st.Delivered && acknowledges(r):
		e.held = pending
		e.ack.held = round
		e.rb.delivered()
	case st.Delivered:
		e.commit(pending)
		e.rb.delivered()
	}
	return st
}

// plan picks the series to write and the values to write for them, records
// what to commit once the response is delivered, and lists the idle series
// sweep can delete.
func (e *Exposer) plan(mfs []*dto.MetricFamily) (out []*dto.MetricFamily, pending []pendingCommit, idle []idleSeries, heartbeat int) {
	for _, mf := range mfs {
		if !e.opts.Only.carries(mf.GetType()) {
			continue
		}
		kept := mf.Metric[:0]
		for _, m := range mf.Metric {
			e.keyBuf = appendSeriesKey(e.keyBuf[:0], mf.GetName(), m.Label)
			en := e.state[string(e.keyBuf)]
			if en != nil && (en.kind != mf.GetType() || rebuilt(m, en)) {
				// A new instance under an old name and labels: the old one was
				// deleted, or its registry replaced, without Replace. It has
				// delivered nothing, so it starts over as new rather than being
				// reported against what the old one delivered.
				en = nil
			}
			if en == nil {
				en = &entry{kind: mf.GetType(), changed: e.round, born: uint32(e.rb.clock().Unix())}
				if e.opts.Delete != nil {
					en.del = &deleteKey{family: mf.GetName(), labels: labelsOf(m.Label)}
				}
				e.state[string(e.keyBuf)] = en
			}
			en.lastRound = e.round
			labels := m.Label // decide may stamp the generation on m

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
			if e.isIdle(en) && e.canDelete(mf.GetName()) {
				idle = append(idle, idleSeries{key: string(e.keyBuf), en: en, family: mf.GetName(), labels: labels})
			}
		}
		if len(kept) > 0 {
			mf.Metric = kept
			out = append(out, mf)
		}
	}
	return out, pending, idle, heartbeat
}

// idleSeries is a series plan found idle, with the labels it was gathered with.
type idleSeries struct {
	key    string
	en     *entry
	family string
	labels []*dto.LabelPair
}

// isIdle reports whether a series is due for deletion.
func (e *Exposer) isIdle(en *entry) bool {
	return e.opts.IdleScrapes > 0 && e.round-en.changed > uint64(e.opts.IdleScrapes) && deletable(en)
}

// canDelete reports whether an idle series of family can be deleted, and logs a
// family that cannot, once.
func (e *Exposer) canDelete(family string) bool {
	if e.vecs[family] != nil || e.opts.Delete != nil {
		return true
	}
	if e.opts.ErrorLog != nil && !e.kept[family] {
		if e.kept == nil {
			e.kept = map[string]bool{}
		}
		e.kept[family] = true
		if e.vecs != nil {
			e.opts.ErrorLog.Println("delta: idle series of", family, "are kept, not deleted: no Vec in Options.Vecs gathers as it")
		} else {
			e.opts.ErrorLog.Println("delta: idle series of", family, "are kept, not deleted: on the Gather path idle cleanup deletes through Options.Vecs, and none is set")
		}
	}
	return false
}

// rebuilt reports whether m holds less than en delivered, which a counter or a
// classic histogram only does as a new instance.
func rebuilt(m *dto.Metric, en *entry) bool {
	switch en.kind {
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue() < en.value
	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		if isNative(h) {
			return false // not differenced, see decide
		}
		sum, count, buckets := en.delivered()
		if h.GetSampleCount() < count {
			return true
		}
		if h.GetSampleCount() == count && h.GetSampleSum() == sum {
			return false // unchanged
		}
		for i, b := range h.Bucket {
			if i < len(buckets) && b.GetCumulativeCount() < buckets[i] {
				return true
			}
		}
	}
	return false
}

type pendingCommit struct {
	en      *entry
	m       prometheus.Metric // the instance, on the change-tracking path: re-marked on a roll-back
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
		if !changed && !e.heartbeatTurn(mf.GetName(), en) {
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
			e.sawActivity(m, en)
			return true, false, nil
		}
		sum, count := h.GetSampleSum(), h.GetSampleCount()
		// Count and sum unchanged means no observation, so the buckets are the
		// delivered ones and there is nothing of them to commit. Those that did
		// change go in a slice of their own, which the entry adopts once they are
		// delivered: keeping a second buffer on every entry to rotate with cost 24
		// bytes on each and a bucket array on most histograms, for the part of a
		// scrape between reading a value and delivering it.
		var buckets []uint64
		dsum, dcount, dbuckets := en.delivered()
		changed := count != dcount || sum != dsum
		if changed {
			en.changed = e.round
			buckets = make([]uint64, len(h.Bucket))
			for i, b := range h.Bucket {
				buckets[i] = b.GetCumulativeCount()
			}
		}
		if !changed && !e.heartbeatTurn(mf.GetName(), en) {
			return false, false, nil
		}
		e.rb.rebaseEntry(en)
		if e.opts.ReportIncrements {
			*h.SampleSum = sum - dsum
			*h.SampleCount = count - dcount
			for i, b := range h.Bucket {
				var prev uint64
				if i < len(dbuckets) {
					prev = dbuckets[i]
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
		e.sawActivity(m, en)
		return true, false, nil
	}
}

// sawActivity records whether a series that is passed through as it is was
// written, for idle cleanup, which would otherwise take one observed every
// scrape for idle and delete it with everything it holds. Its entry's value,
// which nothing else uses, keeps what tells: see activity.
func (e *Exposer) sawActivity(m *dto.Metric, en *entry) {
	if v, ok := activity(m); ok && v != en.value {
		en.value = v
		en.changed = e.round
	}
}

// activity returns what changes when a series passed through as it is gets
// written: a summary's or a native histogram's count of observations, or an
// untyped value.
func activity(m *dto.Metric) (float64, bool) {
	switch {
	case m.Summary != nil:
		return float64(m.Summary.GetSampleCount()), true
	case m.Histogram != nil:
		if f := m.Histogram.GetSampleCountFloat(); f != 0 {
			return f, true
		}
		return float64(m.Histogram.GetSampleCount()), true
	case m.Untyped != nil:
		return m.Untyped.GetValue(), true
	}
	return 0, false
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

// heartbeatTurn reports whether an unchanged series of family is due for a
// top-up.
func (e *Exposer) heartbeatTurn(family string, en *entry) bool {
	if e.opts.DisableHeartbeat {
		return false
	}
	var labels prometheus.Labels
	if en.del != nil {
		labels = en.del.labels
	}
	return int(e.round%uint64(e.opts.HeartbeatScrapes)) == int(hash(family, labels)%uint64(e.opts.HeartbeatScrapes))
}

// commit records the values read this scrape as the last delivered ones.
func (e *Exposer) commit(pending []pendingCommit) {
	for i := range pending {
		pending[i].apply()
	}
}

// histDelivered is a histogram's last delivered _sum, _count and cumulative
// count per le, see entry.hist.
type histDelivered struct {
	sum     float64
	count   uint64
	buckets []uint64
}

// delivered returns the histogram's last delivered sum, count and buckets.
func (en *entry) delivered() (float64, uint64, []uint64) {
	if h := en.hist; h != nil {
		return h.sum, h.count, h.buckets
	}
	return 0, 0, nil
}

// apply makes the values read the last delivered ones.
func (pc *pendingCommit) apply() {
	pc.en.value = pc.value
	if pc.en.kind != dto.MetricType_HISTOGRAM {
		return
	}
	h := pc.en.hist
	if h == nil {
		h = &histDelivered{}
		pc.en.hist = h
	}
	h.sum, h.count = pc.sum, pc.count
	if pc.buckets != nil {
		h.buckets = pc.buckets
	}
}

// sweep deletes the idle series plan listed, and lets go of the state of the
// series not gathered for IdleScrapes scrapes, when complete says this scrape's
// Gather had no error and so returned every series there is. It returns how
// many it deleted and how many it let go.
func (e *Exposer) sweep(idle []idleSeries, complete bool) (deleted, forgotten int) {
	after := uint64(e.opts.IdleScrapes)
	if after == 0 {
		return 0, 0
	}
	for i := range idle {
		if e.deleteIdle(&idle[i]) {
			delete(e.state, idle[i].key)
			deleted++
		}
	}
	if !complete {
		return deleted, 0
	}
	for k, en := range e.state {
		// Not gathered for that long: its child is gone, deleted by the
		// application or its registry replaced. Nothing is deleted through Vecs
		// or Delete for it, which would find a new child of the same labels.
		if e.round-en.lastRound > after {
			delete(e.state, k)
			forgotten++
		}
	}
	return deleted, forgotten
}

// deleteIdle deletes the child of an idle series, and reports whether it went.
// A child that stays keeps its state: let go, the next scrape would report its
// whole value again.
func (e *Exposer) deleteIdle(s *idleSeries) bool {
	if v := e.vecs[s.family]; v != nil {
		// Written since it was gathered, it stays, and the next scrape reports
		// the write; found idle again later, it is deleted then.
		return v.DeleteCollectedIf(s.labels, func(m prometheus.Metric) bool { return unwritten(m, s.en) })
	}
	// Delete cannot say whether it removed the child, nor be asked to remove it
	// only if it was not written since it was gathered; see Options.Delete.
	e.opts.Delete(s.en.del.family, s.en.del.labels)
	return true
}

// unwritten reports whether m, the child of an idle series, still holds what
// the series' entry last saw: nothing was written to it since it was gathered.
func unwritten(m prometheus.Metric, en *entry) bool {
	var d dto.Metric
	if err := m.Write(&d); err != nil {
		return false
	}
	switch {
	case d.Counter != nil:
		return d.Counter.GetValue() == en.value
	case d.Gauge != nil:
		return d.Gauge.GetValue() == en.value
	case d.Histogram != nil && !isNative(d.Histogram):
		sum, count, _ := en.delivered()
		return d.Histogram.GetSampleCount() == count && d.Histogram.GetSampleSum() == sum
	}
	v, ok := activity(&d)
	return ok && v == en.value
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
