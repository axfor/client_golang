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
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// A scrape reports what was written before one moment and nothing after it. A
// request writes its tokens and then its cost, three times as much: a scrape
// that reports the cost of a request without its tokens, which were written
// first, did not read at one moment. A request in the middle of its writes at
// that moment has its tokens in one scrape and its cost in the next, which
// only its writer can prevent; the other way round must never happen.
func TestTrackedScrapeIsOneCut(t *testing.T) {
	trk := prometheus.NewChangeTracker()
	prometheus.SetChangeTracker(trk)
	defer prometheus.SetChangeTracker(nil)
	reg := prometheus.NewRegistry()
	tokens := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tokens_total", Help: "h"}, []string{"key"})
	cost := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cost_total", Help: "h"}, []string{"key"})
	reg.MustRegister(tokens, cost)
	const keys, writers = 2000, 8
	for k := 0; k < keys; k++ { // instances enough for a scrape to take a while to read
		tokens.WithLabelValues(fmt.Sprint("k", k)).Add(0)
		cost.WithLabelValues(fmt.Sprint("k", k)).Add(0)
	}
	opts := Increments()
	opts.Scope = reg
	exp := NewTracked(trk, opts)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var written atomic.Int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for n := int64(1); ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprint("k", (int64(w)*7919+n)%keys)
				v := n%7 + 1
				tokens.WithLabelValues(key).Add(float64(v))
				if n%3 == 0 {
					time.Sleep(10 * time.Microsecond) // the work between the two writes
				}
				cost.WithLabelValues(key).Add(float64(3 * v))
				written.Add(v)
			}
		}(w)
	}

	tok, cst := map[string]float64{}, map[string]float64{}
	add := func(body string) {
		for k, v := range parseCounters(t, body) {
			switch {
			case strings.HasPrefix(k, "tokens"):
				tok[strings.TrimPrefix(k, "tokens")] += v
			case strings.HasPrefix(k, "cost"):
				cst[strings.TrimPrefix(k, "cost")] += v
			}
		}
	}
	inversions, split := 0, 0
	for i := 0; i < 400; i++ {
		rec := httptest.NewRecorder()
		exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
		add(rec.Body.String())
		for k, c := range cst {
			switch d := 3*tok[k] - c; {
			case d < 0:
				inversions++
			case d > 0:
				split++
				// at most one request in its writes per writer at the cut, of at most 7 tokens
				if d > 3*7*writers {
					t.Fatalf("scrape %d, key %s: tokens %v, cost %v: more held back than requests in progress", i, k, tok[k], c)
				}
			}
		}
	}
	close(stop)
	wg.Wait()
	rec := httptest.NewRecorder()
	exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	add(rec.Body.String())
	if inversions > 0 {
		t.Errorf("%d times a key's cost was reported without the tokens written before it: the scrape did not read at one moment", inversions)
	}
	var tt, cc float64
	for k := range tok {
		tt, cc = tt+tok[k], cc+cst[k]
	}
	if tt != float64(written.Load()) || cc != 3*tt {
		t.Fatalf("over all scrapes: tokens %v, written %v, cost %v; nothing may be lost or added", tt, written.Load(), cc)
	}
	t.Logf("%d key-scrapes with a request in its writes at the cut, %d inversions", split, inversions)
}

// With the writes of each request in prometheus.Atomically, no scrape reports
// any request in part, even with work between the two writes.
func TestTrackedAtomicallyKeepsWritesWhole(t *testing.T) {
	trk := prometheus.NewChangeTracker()
	prometheus.SetChangeTracker(trk)
	defer prometheus.SetChangeTracker(nil)
	reg := prometheus.NewRegistry()
	tokens := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tokens_total", Help: "h"}, []string{"key"})
	cost := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cost_total", Help: "h"}, []string{"key"})
	reg.MustRegister(tokens, cost)
	opts := Increments()
	opts.Scope = reg
	exp := NewTracked(trk, opts)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var written atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprint("k", w%4)
			for n := int64(1); ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				v := n%7 + 1
				prometheus.Atomically(func() {
					tokens.WithLabelValues(key).Add(float64(v))
					time.Sleep(20 * time.Microsecond) // as wide a gap as a scrape can hit
					cost.WithLabelValues(key).Add(float64(3 * v))
				})
				written.Add(v)
			}
		}(w)
	}
	var tt, cc float64
	torn := 0
	for i := 0; i < 300; i++ {
		rec := httptest.NewRecorder()
		exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
		var st, sc float64
		for k, v := range parseCounters(t, rec.Body.String()) {
			switch {
			case strings.HasPrefix(k, "tokens"):
				st += v
			case strings.HasPrefix(k, "cost"):
				sc += v
			}
		}
		if sc != 3*st {
			torn++
		}
		tt, cc = tt+st, cc+sc
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	rec := httptest.NewRecorder()
	exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	for k, v := range parseCounters(t, rec.Body.String()) {
		switch {
		case strings.HasPrefix(k, "tokens"):
			tt += v
		case strings.HasPrefix(k, "cost"):
			cc += v
		}
	}
	if torn > 0 {
		t.Errorf("%d of 300 scrapes reported a request in part", torn)
	}
	if tt != float64(written.Load()) || cc != 3*tt {
		t.Fatalf("over all scrapes: tokens %v, written %v, cost %v; nothing may be lost or added", tt, written.Load(), cc)
	}
}

// What the cut costs a request's usage writes, 30 of them; compare with an
// untracked registry, which takes no cut.
func BenchmarkCutRequestWrites(b *testing.B) {
	for _, tracked := range []bool{false, true} {
		trk := prometheus.NewChangeTracker()
		if tracked {
			prometheus.SetChangeTracker(trk)
		}
		vecs := make([]*prometheus.CounterVec, 30)
		for i := range vecs {
			vecs[i] = prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("u%d_total", i), Help: "h"}, []string{"key"})
		}
		keys := make([]string, 1000)
		for k := range keys {
			keys[k] = fmt.Sprint("k", k)
			for _, v := range vecs {
				v.WithLabelValues(keys[k]).Inc()
			}
		}
		prometheus.SetChangeTracker(nil)
		write := func(i int) {
			for _, v := range vecs {
				v.WithLabelValues(keys[i%1000]).Add(3)
			}
		}
		b.Run(fmt.Sprintf("tracked=%v/serial", tracked), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				write(i)
			}
		})
		b.Run(fmt.Sprintf("tracked=%v/parallel", tracked), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					write(i)
					i++
				}
			})
		})
	}
}

// How long a write waits for a scrape's cut, the most it can: the cut is held
// while the changed instances are taken and their values copied, and the rest
// of the scrape runs without it.
func BenchmarkCutWriterWait(b *testing.B) {
	for _, changed := range []int{50000, 500000} {
		b.Run(fmt.Sprintf("changed=%d", changed), func(b *testing.B) {
			trk := prometheus.NewChangeTracker()
			prometheus.SetChangeTracker(trk)
			defer prometheus.SetChangeTracker(nil)
			reg := prometheus.NewRegistry()
			vecs := make([]*prometheus.CounterVec, 30)
			for i := range vecs {
				vecs[i] = prometheus.NewCounterVec(prometheus.CounterOpts{Name: fmt.Sprintf("u%d_total", i), Help: "h"}, []string{"key"})
				reg.MustRegister(vecs[i])
			}
			ctrs := make([]prometheus.Counter, 0, changed)
			for k := 0; len(ctrs) < changed; k++ {
				for _, v := range vecs {
					ctrs = append(ctrs, v.WithLabelValues(fmt.Sprint("k", k)))
				}
			}
			opts := Increments()
			opts.Scope = reg
			exp := NewTracked(trk, opts)
			probe := ctrs[0]
			var maxWait, scrape time.Duration
			for i := 0; i < b.N; i++ {
				for _, c := range ctrs {
					c.Inc()
				}
				stop := make(chan struct{})
				waits := make(chan time.Duration)
				go func() {
					var worst time.Duration
					for {
						select {
						case <-stop:
							waits <- worst
							return
						default:
						}
						t0 := time.Now()
						probe.Add(0)
						worst = max(worst, time.Since(t0))
					}
				}()
				t0 := time.Now()
				exp.Serve(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
				scrape += time.Since(t0)
				close(stop)
				maxWait = max(maxWait, <-waits)
			}
			b.ReportMetric(float64(scrape.Milliseconds())/float64(b.N), "scrape-ms")
			b.ReportMetric(float64(maxWait.Microseconds())/1000, "max-writer-wait-ms")
		})
	}
}
