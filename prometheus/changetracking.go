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

package prometheus

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	dto "github.com/prometheus/client_model/go"
)

// Change tracking, used by the change-only and delta exposition in
// prometheus/promhttp/delta.
//
// When there are very many series but only a small share of them change between
// scrapes (per-API-key usage, say: 30k keys x 37 metrics = 4.4M series, of which
// only a few percent see traffic in any given minute), building a dto for *every*
// series in Gather() is the dominant cost. With tracking enabled, each write to a
// counter, gauge or histogram enlists the instance in a sharded dirty list, and a
// scrape takes only the instances written since the previous scrape.
//
// Tracking is off by default. When off, a write costs one nil check on a pointer
// that sits in the metric struct, and each instance carries that one extra pointer.
// When on, each instance created through a Vec also allocates a small dirtyState.
//
// Constraint: the dirty list has a single consumer, because delivery state is
// global. That is the same constraint change-only exposition has anyway.

const dirtyShardCount = 256

// dirtyRef is embedded in every metric that can be tracked. It is one pointer
// wide and stays nil unless the metric was created while a ChangeTracker was set.
//
// The pointer is atomic because a write reads it while the instance may be
// tracked at that moment. It is never cleared: an instance deleted from its Vec
// keeps it, so that a write by a caller that looked the instance up before the
// deletion is still recorded, and its consumer settles it, see ReviveTracked. On
// amd64 the load is a plain move and on arm64 a load-acquire, next to the
// atomic add every write already does.
type dirtyRef struct {
	d atomic.Pointer[dirtyState]
}

// mark enlists the metric in the dirty list. Keep this small enough to inline:
// it sits on the write path of every counter, gauge and histogram.
func (r *dirtyRef) mark() {
	if d := r.d.Load(); d != nil {
		d.mark()
	}
}

// count records a write of v to a counter: enlists it, and adds v to its
// increments on the current side of the cut. Keep this small enough to inline.
func (r *dirtyRef) count(v float64) {
	if d := r.d.Load(); d != nil {
		d.count(v)
	}
}

// count is dirtyRef.count for a tracked instance. It never waits: a write that
// sees the cut move on while it is being made goes again, on the new side.
func (d *dirtyState) count(v float64) {
	s := d.shard
	cut := &s.tracker.cut
	for {
		c := cut.Load()
		if countHook != nil {
			countHook(0)
		}
		in := &s.inflight[c&1]
		in.Add(1)
		if cut.Load() != c {
			in.Add(-1)
			continue
		}
		if countHook != nil {
			countHook(1)
		}
		since := &d.since[c&1]
		for {
			old := since.Load()
			if since.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
				break
			}
		}
		if countHook != nil {
			countHook(2)
		}
		d.mark()
		in.Add(-1)
		return
	}
}

// countHook, when set, is called by a counter write after it reads the cut (0),
// after it is registered on its side (1), and before it enlists the instance (2):
// for the tests of Cut only.
var countHook func(step int)

// dirtyState is allocated once per tracked instance.
//
// owner, home and hash are set when the instance is enlisted and never change:
// a deleted instance is marked dead instead of having them cleared, because a
// write, Take and RangeBucket may all be reading them at that moment. The fields
// stay at 80 bytes, a size class down from 96: dead shares a word with the
// instance's index in its shard, see pos, next to epoch.
type dirtyState struct {
	epoch atomic.Uint32 // shard epoch as of the last time this entered the dirty list
	// pos is posDead once deleted from its Vec, when owner is no longer handed
	// out, and the index into the shard's all slice plus one, 0 when it is in
	// none; the index is set under shard.allMu.
	pos   atomic.Uint32
	shard *dirtyShard
	owner Metric
	home  *metricMap     // the Vec this instance belongs to, so it can be dropped
	hash  uint64         // its bucket in home
	slot  unsafe.Pointer // the consumer's, see TrackerSlot
	// A counter's increments, as float64 bits, on each side of the last cut, by
	// the parity of the cut they were made in, and its value as of the last cut,
	// which only the consumer touches; see ChangeTracker.Cut and CutValue.
	since [2]atomic.Uint64
	atCut float64
}

// posDead marks dirtyState.pos of a deleted instance.
const posDead = 1 << 31

func (d *dirtyState) dead() bool { return d.pos.Load()&posDead != 0 }

// markDead marks d deleted from its Vec, and reports whether it was not yet.
func (d *dirtyState) markDead() bool {
	for {
		v := d.pos.Load()
		if v&posDead != 0 {
			return false
		}
		if d.pos.CompareAndSwap(v, v|posDead) {
			return true
		}
	}
}

func (d *dirtyState) clearDead() {
	for {
		v := d.pos.Load()
		if d.pos.CompareAndSwap(v, v&^posDead) {
			return
		}
	}
}

// index is the instance's index in its shard's all slice, -1 when in none.
func (d *dirtyState) index() int { return int(d.pos.Load()&^posDead) - 1 }

func (d *dirtyState) setIndex(i int) {
	for {
		v := d.pos.Load()
		if d.pos.CompareAndSwap(v, v&posDead|uint32(i+1)) {
			return
		}
	}
}

// live returns the instance, or nil once it has been deleted from its Vec.
func (d *dirtyState) live() Metric {
	if d.dead() {
		return nil
	}
	return d.owner
}

// written reports whether the instance has been written since the tracker last
// handed it out: it is waiting in the dirty list of its shard.
func (d *dirtyState) written() bool {
	return d.epoch.Load() == d.shard.epoch.Load()
}

// mark enlists d unless it is already waiting to be taken. An equal epoch means
// it is still in the shard's dirty list, which is the common case under load and
// takes no lock.
func (d *dirtyState) mark() {
	s := d.shard
	if d.epoch.Load() == s.epoch.Load() {
		return
	}
	s.add(d)
}

type dirtyShard struct {
	tracker *ChangeTracker
	// inflight counts the counter writes in progress on each side of the cut,
	// by the parity of the cut they were made in; see ChangeTracker.Cut.
	inflight [2]atomic.Int64

	mu        sync.Mutex
	epoch     atomic.Uint32
	dirty     []*dirtyState
	spare     []*dirtyState // backing array of the previous batch, reused
	batchPeak int           // recent high-water mark of a batch, so spare can fall back

	allMu   sync.RWMutex
	all     []*dirtyState // every instance on this shard, for heartbeats and idle cleanup
	allPeak int           // recent high-water mark of len(all), so it can fall back
}

// Neither of these slices is worth reallocating below this.
const minShrinkSlice = 1 << 6

// shrunk returns a slice holding the same elements with room for want more,
// when the one it is given is holding an array far larger than that. A slice
// that grew to fit every instance the process had keeps that array after the
// elements are gone: the pointers are cleared, so nothing is retained through
// them, but the array itself stays for the life of the process. Across the
// shards here that was 18MB of the 310MB live heap at 30k keys with 2k of them
// active.
func shrunk(s []*dirtyState, want int) []*dirtyState {
	if cap(s) < minShrinkSlice || cap(s) < 4*(want+1) {
		return s
	}
	fresh := make([]*dirtyState, len(s), want+want/4+1)
	copy(fresh, s)
	return fresh
}

func (s *dirtyShard) add(d *dirtyState) {
	s.mu.Lock()
	if ep := s.epoch.Load(); d.epoch.Load() != ep {
		d.epoch.Store(ep)
		s.dirty = append(s.dirty, d)
	}
	s.mu.Unlock()
}

// ChangeTracker collects the instances that have been written to. A process
// normally needs one, consumed by a single scraper; tests can hold their own
// without interfering with each other.
type ChangeTracker struct {
	shards [dirtyShardCount]dirtyShard
	// cut counts the cuts taken, see Cut; a counter write goes to the side of
	// the cut current when it is made.
	cut atomic.Uint64
	// atomic is held shared by Atomically and exclusively by Cut while it
	// moves the cut on.
	atomic sync.RWMutex
}

// Cut divides the counter writes into those before it and those after, and
// returns its number. A scrape reports what was written
// before its cut and nothing after it, all counters taken at one moment, so it
// never reports a write without the writes made before it, such as the cost of
// a request without its tokens; reading one counter after another as they are
// written could.
//
// No write waits for it: moving the cut on is one atomic add. Cut itself waits
// for the writes made before it to finish, which take nanoseconds, and for the
// groups of Atomically in progress. Take, after Cut, hands out every counter
// written before it, with what was written before it added into its value as of
// the cut, see CutValue.
func (t *ChangeTracker) Cut() uint64 {
	t.atomic.Lock()
	c := t.cut.Add(1) - 1
	t.atomic.Unlock()
	for i := range t.shards {
		for t.shards[i].inflight[c&1].Load() != 0 {
			runtime.Gosched()
		}
	}
	return c
}

// CutValue returns the value of the counter m as of the last cut that Take
// handed it out after: the sum of the writes made before that cut, none made
// after. It reports false for a metric that is not tracked, and is only
// meaningful for counters.
func CutValue(m Metric) (float64, bool) {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return 0, false
	}
	d := ref.d.Load()
	if d == nil {
		return 0, false
	}
	return d.atCut, true
}

// fold adds the writes made before cut into the value as of the cut, for Take,
// which hands out every instance written before it: done there, for each, they
// cannot be left behind on their side, to be mixed with writes made after the
// next cut. An instance already written after the cut is put back in the dirty
// list, which Take has just emptied.
func (d *dirtyState) fold(cut uint64) {
	d.atCut += math.Float64frombits(d.since[cut&1].Swap(0))
	if d.since[(cut+1)&1].Load() != 0 {
		d.mark()
	}
}

// Atomically runs f, writes to counters that belong together, such as the
// tokens and the cost of one request, so that a scrape reports all of them or
// none. Without it a scrape still never reports a write without the writes made
// before it, see Cut, but one whose cut falls between two writes of f reports
// the first and leaves the second to the next scrape.
//
// Writes in f wait for nothing, and nothing else waits for them; a cut waits for
// every f in progress before it moves on, and an f begun meanwhile waits for
// that, which is as long as the f in progress take. Keep f to the writes, with
// no I/O or other waiting in it, and do not call Atomically from within f, which
// can wait for itself. Without change tracking on it just runs f.
func Atomically(f func()) {
	t := tracker.Load()
	if t == nil {
		f()
		return
	}
	t.atomic.RLock()
	defer t.atomic.RUnlock()
	f()
}

// NewChangeTracker returns a new tracker.
func NewChangeTracker() *ChangeTracker {
	t := &ChangeTracker{}
	for i := range t.shards {
		t.shards[i].epoch.Store(1)
		t.shards[i].tracker = t
	}
	return t
}

var tracker atomic.Pointer[ChangeTracker]

// SetChangeTracker selects the tracker newly created metrics enlist in. Passing
// nil turns tracking off.
//
// Set it once at startup and leave it set: a Vec creates its children lazily on
// the first WithLabelValues, and a child is tracked only if a tracker is current
// at that moment. Clearing it midway loses every key that shows up afterwards.
func SetChangeTracker(t *ChangeTracker) { tracker.Store(t) }

// CurrentChangeTracker returns the current tracker, or nil when tracking is off.
func CurrentChangeTracker() *ChangeTracker { return tracker.Load() }

// EnableChangeTracking turns change tracking on. It is equivalent to
// SetChangeTracker(NewChangeTracker()) but leaves an existing tracker in place.
func EnableChangeTracking() *ChangeTracker {
	if t := tracker.Load(); t != nil {
		return t
	}
	t := NewChangeTracker()
	if tracker.CompareAndSwap(nil, t) {
		return t
	}
	return tracker.Load()
}

// ChangeTrackingEnabled reports whether change tracking is on.
func ChangeTrackingEnabled() bool { return tracker.Load() != nil }

// trackMetric enlists an instance a Vec has just created, and marks it changed
// right away: a new instance always has something to report.
//
// The caller holds the metricMap lock, so the instance is not reachable by any
// other goroutine yet and the plain pointer store below is safe.
func trackMetric(home *metricMap, m Metric, hash uint64) {
	t := tracker.Load()
	if t == nil {
		return
	}
	ref, ok := dirtyRefOf(m)
	if !ok {
		return
	}
	s := &t.shards[hash%dirtyShardCount]
	d := &dirtyState{shard: s, owner: m, home: home, hash: hash}
	ref.d.Store(d)

	s.allMu.Lock()
	if len(s.all) >= posDead-1 {
		panic("prometheus: more tracked instances in one change-tracking shard than it can index")
	}
	d.setIndex(len(s.all))
	s.all = append(s.all, d)
	s.allMu.Unlock()

	s.add(d)
}

// untrackMetric drops an instance a Vec has just deleted from the instances
// heartbeats and idle cleanup walk.
//
// It stays tracked otherwise: a caller that looked it up before the deletion
// and writes to it after enlists it, as any write does, and Take hands it out,
// for its consumer to settle, see ReviveTracked. Nothing is cleared, so an
// in-flight mark, and a Take or RangeBucket holding it, read what they read
// before.
func untrackMetric(m Metric) {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return
	}
	d := ref.d.Load()
	if d == nil || !d.markDead() {
		return
	}
	s := d.shard

	s.allMu.Lock()
	if i := d.index(); i >= 0 && i < len(s.all) && s.all[i] == d {
		last := len(s.all) - 1
		s.all[i] = s.all[last]
		s.all[i].setIndex(i)
		s.all[last] = nil
		s.all = s.all[:last]
		if n := len(s.all); n > s.allPeak {
			s.allPeak = n
		} else {
			s.allPeak -= s.allPeak / 8
			s.all = shrunk(s.all, s.allPeak)
		}
	}
	d.setIndex(-1)
	s.allMu.Unlock()
}

// DeleteTracked removes a tracked instance from the Vec that created it, which
// is what idle cleanup in change-only exposition needs: without it the process
// keeps every label combination it has ever seen.
//
// It reports whether the instance was found and removed. Metrics that are not
// tracked, or were already removed, report false.
//
// Deleting is not coordinated with writes. A request holding this instance while
// it is removed writes into a child no longer in the Vec; the write is still
// recorded, and Take hands the instance out for its consumer to settle, see
// ReviveTracked. The same holds for Vec.Delete of a tracked instance.
func DeleteTracked(m Metric) bool {
	d := liveDirtyState(m)
	if d == nil {
		return false
	}
	return d.home.deleteMetric(d.hash, m, nil)
}

// DeleteTrackedIfUnwritten is DeleteTracked for an instance the consumer found
// idle: it removes the instance only if it has not been written since the
// tracker last handed it out, checked under the lock of its Vec.
//
// A consumer decides an instance is idle from what it has seen, and that goes
// stale while it works: a request that looks the key up in the meantime gets this
// instance, still in the Vec, and its write would be lost with it. The check
// covers every write that has enlisted the instance by the time the Vec is
// locked, and no new lookup can find the instance while it is. A write between
// its value update and its enlisting, or by a caller that looked the instance up
// before the lock and writes after it, lands in the deleted instance, and is
// settled by the consumer as any write to a deleted instance is, see
// ReviveTracked.
func DeleteTrackedIfUnwritten(m Metric) bool {
	d := liveDirtyState(m)
	if d == nil {
		return false
	}
	return d.home.deleteMetric(d.hash, m, func() bool { return !d.written() })
}

// TrackedWritten reports whether a tracked instance has been written since the
// tracker last handed it out. A consumer deleting instances through a callback
// of its own, rather than DeleteTrackedIfUnwritten, checks this first.
func TrackedWritten(m Metric) bool {
	d := liveDirtyState(m)
	return d != nil && d.written()
}

func liveDirtyState(m Metric) *dirtyState {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return nil
	}
	d := ref.d.Load()
	if d == nil || d.dead() {
		return nil
	}
	return d
}

// TrackerSlot returns one word for the tracker's consumer to keep its own state
// for m in, or nil when m is not tracked: not created through a Vec while a
// tracker was set, or deleted from its Vec since.
//
// It saves the consumer a map from every instance to its state, which at a few
// million instances is tens of megabytes of table on top of the state itself,
// and it goes with the instance: a child deleted from its Vec by any means takes
// the state with it instead of leaving it in the consumer's map. The word fits
// in the size class dirtyState already occupied, so it costs nothing extra. It
// starts nil and is read and written only by the single consumer the tracker
// already assumes.
func TrackerSlot(m Metric) *unsafe.Pointer {
	d := liveDirtyState(m)
	if d == nil {
		return nil
	}
	return &d.slot
}

func dirtyRefOf(m Metric) (*dirtyRef, bool) {
	t, ok := m.(interface{ trackRef() *dirtyRef })
	if !ok {
		return nil, false
	}
	return t.trackRef(), true
}

// Take hands every instance written since the previous call to fn and advances
// the batch. It must be called by a single consumer, the same constraint that
// change-only exposition carries anyway.
//
// An instance deleted from its Vec and written since is handed out too, see
// TrackedDeleted: written by a caller that looked it up before the deletion, a
// write the consumer settles with ReviveTracked rather than losing.
func (t *ChangeTracker) Take(fn func(Metric)) {
	if t == nil {
		return
	}
	// the writes before the last cut, see Cut; none before the first
	c := t.cut.Load()
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		taken := s.dirty
		s.dirty = s.spare[:0]
		s.spare = nil
		s.epoch.Add(1)
		s.mu.Unlock()

		for _, d := range taken {
			if c > 0 {
				d.fold(c - 1)
			}
			// a deleted instance too: it was written after it was deleted, see
			// ReviveTracked
			fn(d.owner)
		}

		s.mu.Lock()
		if s.spare == nil {
			if n := len(taken); n > s.batchPeak {
				s.batchPeak = n
			} else {
				s.batchPeak -= s.batchPeak / 8
			}
			clear(taken)
			s.spare = shrunk(taken[:0], s.batchPeak)
		}
		s.mu.Unlock()
	}
}

// MarkChanged puts an instance back in the dirty list.
//
// A scrape that was not delivered has to do this for everything it took: those
// increments have not been committed, so the next scrape must report them again
// or they are lost.
func MarkChanged(m Metric) {
	if ref, ok := dirtyRefOf(m); ok {
		ref.mark()
	}
}

// RangeBucket walks bucket number bucket of buckets, used for heartbeat top-ups
// and idle cleanup: a scrape covers 1/buckets of all instances instead of
// walking every one of them every time.
func (t *ChangeTracker) RangeBucket(bucket, buckets int, fn func(Metric)) {
	if t == nil || buckets <= 0 {
		return
	}
	for i := bucket % buckets; i < dirtyShardCount; i += buckets {
		s := &t.shards[i]
		s.allMu.RLock()
		for _, d := range s.all {
			if m := d.live(); m != nil {
				fn(m)
			}
		}
		s.allMu.RUnlock()
	}
}

// TrackerCapacity reports how many entries the tracker's per-shard slices have
// room for. It exists for tests: the slices hold pointers that are cleared on
// removal, so the only way to see whether the arrays behind them were given
// back is to look at the capacity.
func TrackerCapacity(t *ChangeTracker) int {
	if t == nil {
		return 0
	}
	total := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.allMu.RLock()
		total += cap(s.all)
		s.allMu.RUnlock()
		s.mu.Lock()
		total += cap(s.dirty) + cap(s.spare)
		s.mu.Unlock()
	}
	return total
}

// TrackedDeleted reports whether m, a tracked instance, has been deleted from
// its Vec. Take hands out such an instance when it is written after the
// deletion; see ReviveTracked.
func TrackedDeleted(m Metric) bool {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return false
	}
	d := ref.d.Load()
	return d != nil && d.dead()
}

// ReviveTracked settles m, an instance deleted from its Vec that Take has handed
// out: written after the deletion, by a caller that looked it up before. Such a
// write is not lost.
//
// If the Vec has no instance with m's labels, m is put back in it, tracked as
// before, with its TrackerSlot and everything in it, as if it had never been
// deleted, and ReviveTracked returns m. Otherwise -- a later lookup has created
// a new instance with the same labels -- it returns that one: what m has not
// delivered goes into it, with Counter.Add or AddTrackedHistogram, and m stays
// deleted. slot is m's TrackerSlot either way.
//
// It must be called by the tracker's consumer. For an instance not deleted it
// returns m and its slot.
func ReviveTracked(m Metric) (Metric, *unsafe.Pointer) {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return m, nil
	}
	d := ref.d.Load()
	if d == nil {
		return m, nil
	}
	if !d.dead() {
		return m, &d.slot
	}
	home := d.home
	lvs, ok := labelValuesOf(m, home.desc)
	if !ok {
		return m, &d.slot
	}
	home.mtx.Lock()
	defer home.mtx.Unlock()
	if now, found := home.getMetricWithHashAndLabelValues(d.hash, lvs, nil); found {
		return now, &d.slot
	}
	home.metrics[d.hash] = append(home.metrics[d.hash], metricWithLabelValues{values: lvs, metric: m})
	if n := len(home.metrics); n > home.peak {
		home.peak = n
	}
	s := d.shard
	s.allMu.Lock()
	d.setIndex(len(s.all))
	s.all = append(s.all, d)
	s.allMu.Unlock()
	d.clearDead()
	return m, &d.slot
}

// labelValuesOf returns the values of m's variable labels, in the order of
// desc's: what its Vec keeps it under.
func labelValuesOf(m Metric, desc *Desc) ([]string, bool) {
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		return nil, false
	}
	names := desc.variableLabels.names
	lvs := make([]string, len(names))
	for i, name := range names {
		found := false
		for _, lp := range out.Label {
			if lp.GetName() == name {
				lvs[i], found = lp.GetValue(), true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return lvs, true
}

// AddTrackedCut adds v to c, a tracked counter, as written before the last cut:
// it is in CutValue(c) at once, and in c's value. It is what a deleted instance
// had not delivered of the writes made before that cut, see ReviveTracked,
// moved into the instance that has its labels now, and goes out with the
// scrape of that cut. It reports false, adding nothing, for anything else.
// Only the tracker's consumer calls it, between Take and the next Cut.
func AddTrackedCut(c Metric, v float64) bool {
	ct, ok := c.(*counter)
	if !ok || v <= 0 {
		return false
	}
	d := ct.track.d.Load()
	if d == nil {
		return false
	}
	if ival := uint64(v); float64(ival) == v {
		atomic.AddUint64(&ct.valInt, ival)
	} else {
		atomicAddFloat(&ct.valBits, v)
	}
	d.atCut += v
	d.mark()
	return true
}

// AddTrackedHistogram adds to h, a classic histogram, count observations
// summing to sum, cumulative[i] of them at or below its i-th upper bound, as
// if they had been observed one by one: what a deleted instance had not
// delivered, see ReviveTracked. It reports false, adding nothing, for anything
// else, a native histogram among it, whose sparse buckets these cannot fill.
func AddTrackedHistogram(h Metric, cumulative []uint64, count uint64, sum float64) bool {
	hi, ok := h.(*histogram)
	if !ok || hi.nativeHistogramSchema > math.MinInt32 || len(cumulative) > len(hi.upperBounds) || count == 0 {
		return false
	}
	n := atomic.AddUint64(&hi.countAndHotIdx, count)
	hot := hi.counts[n>>63]
	var prev uint64
	for i, c := range cumulative {
		if c > prev {
			atomic.AddUint64(&hot.buckets[i], c-prev)
		}
		prev = c
	}
	atomicAddFloat(&hot.sumBits, sum)
	// last, as observe does: the count marks the observations complete
	atomic.AddUint64(&hot.count, count)
	hi.track.mark()
	return true
}
