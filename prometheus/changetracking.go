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
// deleted from its Vec, which clears it. On amd64 the load is a plain move and
// on arm64 a load-acquire, next to the atomic add every write already does.
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
// stay at 64 bytes, dead taking the padding after epoch.
type dirtyState struct {
	epoch atomic.Uint32 // shard epoch as of the last time this entered the dirty list
	dead  atomic.Bool   // deleted from its Vec; owner is no longer handed out
	shard *dirtyShard
	owner Metric
	home  *metricMap     // the Vec this instance belongs to, so it can be dropped
	hash  uint64         // its bucket in home
	idx   int            // index into the shard's all slice, for removal; under shard.allMu
	slot  unsafe.Pointer // the consumer's, see TrackerSlot
	// A counter's increments, as float64 bits, on each side of the last cut, by
	// the parity of the cut they were made in, and its value as of the last cut,
	// which only the consumer touches; see ChangeTracker.Cut and CutValue.
	since [2]atomic.Uint64
	atCut float64
}

// live returns the instance, or nil once it has been deleted from its Vec.
func (d *dirtyState) live() Metric {
	if d.dead.Load() {
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
	d.idx = len(s.all)
	s.all = append(s.all, d)
	s.allMu.Unlock()

	s.add(d)
}

// untrackMetric drops an instance a Vec has just deleted.
//
// A write racing with the delete can still enlist the old dirtyState; Take skips
// it because it is dead by then. Nothing is cleared, so such an in-flight mark,
// and a Take or RangeBucket holding it, read what they read before.
func untrackMetric(m Metric) {
	ref, ok := dirtyRefOf(m)
	if !ok {
		return
	}
	d := ref.d.Swap(nil)
	if d == nil {
		return
	}
	d.dead.Store(true)
	s := d.shard

	s.allMu.Lock()
	if d.idx >= 0 && d.idx < len(s.all) && s.all[d.idx] == d {
		last := len(s.all) - 1
		s.all[d.idx] = s.all[last]
		s.all[d.idx].idx = d.idx
		s.all[last] = nil
		s.all = s.all[:last]
		if n := len(s.all); n > s.allPeak {
			s.allPeak = n
		} else {
			s.allPeak -= s.allPeak / 8
			s.all = shrunk(s.all, s.allPeak)
		}
	}
	d.idx = -1
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
// it is removed writes into a child no longer in the Vec, and that write is lost.
// That is how Vec.Delete behaves as well; only delete what has been idle long
// enough for this to be vanishingly unlikely.
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
// locked, and no new lookup can find the instance while it is. What is left is a
// write between its value update and its enlisting, and one by a caller that
// looked the instance up before the lock and writes after it, as with any
// Vec.Delete.
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
	if d == nil || d.dead.Load() {
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
			if m := d.live(); m != nil {
				fn(m)
			}
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
