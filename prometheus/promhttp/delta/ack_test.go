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
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

// server is what both exposers have in common.
type serveFunc func(w http.ResponseWriter, r *http.Request) ScrapeStats

// ackScraper scrapes like vmagent does: every request names the last round it
// processed in full, "<session>:0" when it has had rounds of the session but
// processed none, and AckNew before it has had any.
type ackScraper struct {
	serve     serveFunc
	processed string             // last round processed in full
	session   string             // of the last round it got
	total     map[string]float64 // what it processed, by series
}

func newAckScraper(serve serveFunc) *ackScraper {
	return &ackScraper{serve: serve, total: map[string]float64{}}
}

// scrape runs one request. With process false the scraper fetches the response
// and throws it away, as a failed scrape or a look at the target's response does.
func (s *ackScraper) scrape(t *testing.T, process bool) ScrapeStats {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set(AckHeader, s.ack())
	rec := httptest.NewRecorder()
	st := s.serve(rec, req)
	round := rec.Header().Get(RoundHeader)
	if round == "" {
		t.Fatalf("the response carries no %s", RoundHeader)
	}
	if session, _, _ := strings.Cut(round, ":"); session != s.session {
		s.session, s.processed = session, ""
	}
	if process && st.Delivered {
		for k, v := range parseCounters(t, rec.Body.String()) {
			s.total[k] += v
		}
		s.processed = round
	}
	return st
}

func (s *ackScraper) ack() string {
	switch {
	case s.session == "":
		return AckNew
	case s.processed == "":
		return s.session + ":0"
	default:
		return s.processed
	}
}

// peek fetches the response to look at it, as vmagent's target status page does.
func (s *ackScraper) peek(t *testing.T) ScrapeStats {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set(AckHeader, AckPeek)
	return s.serve(httptest.NewRecorder(), req)
}

// parseCounters returns the value of every sample line by its series.
func parseCounters(t *testing.T, body string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			t.Fatalf("cannot parse %q: %s", line, err)
		}
		series := line[:i]
		// The fixtures name families c<random>_total: keep c and the labels.
		name, labels := series, ""
		if j := strings.IndexByte(series, '{'); j >= 0 {
			name, labels = series[:j], series[j:]
		}
		name = strings.TrimSuffix(name, "_total")
		name = strings.TrimRightFunc(name, func(r rune) bool { return r >= '0' && r <= '9' })
		out[name+labels] += v
	}
	return out
}

// A round the scraper fetched but did not process -- its scrape timed out after
// the response was written, or it only looked at it -- is rolled back by its next
// request and reported again; a round it processed is committed. Every increment
// is processed exactly once.
func TestTrackedAcknowledgedRounds(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	key := `c{key="a"}`

	f.c.WithLabelValues("a").Add(5)
	if st := s.scrape(t, true); st.Settled != SettledNone {
		t.Fatalf("first scrape settled %v, want none", st.Settled)
	}
	f.c.WithLabelValues("a").Add(3)
	s.scrape(t, false) // fetched, thrown away
	f.c.WithLabelValues("a").Add(1)
	if st := s.peek(t); st.Settled != SettledNone || st.Round == 0 {
		t.Fatalf("a peek settled %v, want the held round left alone", st.Settled)
	}
	if st := s.scrape(t, true); st.Settled != SettledRolledBack {
		t.Fatalf("a scrape after an unprocessed round settled %v, want rolled back", st.Settled)
	}
	if got := s.total[key]; got != 9 {
		t.Fatalf("processed %v, want 9 (5, then 3 and 1 reported again)", got)
	}
	f.c.WithLabelValues("a").Add(2)
	if st := s.scrape(t, true); st.Settled != SettledCommitted {
		t.Fatalf("a scrape after a processed round settled %v, want committed", st.Settled)
	}
	s.scrape(t, true)
	if got := s.total[key]; got != 11 {
		t.Fatalf("processed %v, want 11", got)
	}
}

// A look at the target's response while a round is held, after the scraper got
// the round and before it acknowledged it, leaves the round alone: the scraper
// processes it and acknowledges it next. What the look got is reported again.
func TestTrackedPeekLeavesHeldRound(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	f.c.WithLabelValues("a").Add(3)
	s.scrape(t, true) // held, being processed
	f.c.WithLabelValues("a").Add(1)
	s.peek(t) // the status page, at the same time
	if st := s.scrape(t, true); st.Settled != SettledCommitted {
		t.Fatalf("the round processed while a peek came in settled %v, want committed", st.Settled)
	}
	s.scrape(t, true)
	if got := s.total[`c{key="a"}`]; got != 9 {
		t.Fatalf("processed %v, want 9 exactly once", got)
	}
}

// A scraper that has just started says AckNew: it cannot tell whether it
// processed the round held, which is committed, as delivery did.
func TestTrackedNewScraperCommitsHeldRound(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	restarted := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(2)
	if st := restarted.scrape(t, true); st.Settled != SettledCommitted {
		t.Fatalf("settled %v, want committed", st.Settled)
	}
	if got := s.total[`c{key="a"}`] + restarted.total[`c{key="a"}`]; got != 7 {
		t.Fatalf("processed %v, want 7", got)
	}
}

// A request without the header -- a scraper that does not acknowledge, or a
// curl -- commits the held round, and its own round is committed on delivery,
// as before the protocol.
func TestTrackedRequestWithoutAckCommits(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	s.scrape(t, true) // now acknowledging; this round is held
	f.c.WithLabelValues("a").Add(4)
	rec := httptest.NewRecorder()
	st := f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	if st.Settled != SettledCommitted {
		t.Fatalf("a request without %s settled %v, want committed", AckHeader, st.Settled)
	}
	if got := parseCounters(t, rec.Body.String())[`c{key="a"}`]; got != 4 {
		t.Fatalf("a request without the header got %v, want 4", got)
	}
	// Committed on delivery: the next request reports nothing of it again.
	rec = httptest.NewRecorder()
	f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	if got := parseCounters(t, rec.Body.String())[`c{key="a"}`]; got != 0 {
		t.Fatalf("reported %v again after it was committed on delivery", got)
	}
}

// The Gather path holds and settles rounds the same way.
func TestGatherAcknowledgedRounds(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	reg.MustRegister(c)
	exp := New(reg, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(exp.Serve)
	c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	c.WithLabelValues("a").Add(3)
	s.scrape(t, false)
	c.WithLabelValues("a").Add(1)
	s.peek(t)
	s.scrape(t, true)
	s.scrape(t, true)
	if got := s.total[`c{key="a"}`]; got != 9 {
		t.Fatalf("processed %v, want 9", got)
	}
}

// A Gather error leaves out what could not be gathered, and the response still
// delivers the rest: it is committed, not reported again on top of itself.
func TestGatherErrorStillCommitsWhatWasDelivered(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	reg.MustRegister(c)
	exp := New(failingGatherer{reg}, Options{ReportIncrements: true, DisableHeartbeat: true})
	var total float64
	for i := 0; i < 3; i++ {
		c.WithLabelValues("a").Add(5)
		rec := httptest.NewRecorder()
		st := exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
		if !st.Delivered || st.Err == nil {
			t.Fatalf("scrape %d: delivered %v, err %v; want delivered with the Gather error", i, st.Delivered, st.Err)
		}
		total += parseCounters(t, rec.Body.String())[`c{key="a"}`]
	}
	if total != 15 {
		t.Fatalf("reported %v in all, want 15", total)
	}
}

type failingGatherer struct{ g prometheus.Gatherer }

func (f failingGatherer) Gather() ([]*dto.MetricFamily, error) {
	mfs, _ := f.g.Gather()
	return mfs, errors.New("one collector failed")
}

// A request that writes to a key while the scrape that found it idle is being
// written gets the instance still in its Vec, and must not lose its write when
// the scrape drops it.
func TestTrackedIdleDropKeepsWriteDuringScrape(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
	var reported float64
	scrape := func(hook func()) ScrapeStats {
		f.g.WithLabelValues("busy").Inc() // keep every response non-empty
		w := &hookWriter{h: http.Header{}, hook: hook}
		st := f.exp.Serve(w, httptest.NewRequest("GET", "/metrics", nil))
		reported += parseCounters(t, w.buf.String())[`c{key="a"}`]
		return st
	}
	f.c.WithLabelValues("a").Add(5)
	for i := 0; i < 3; i++ {
		scrape(nil)
	}
	// This scrape finds the key idle; a request adds 7 while it is written.
	if st := scrape(func() { f.c.WithLabelValues("a").Add(7) }); st.Deleted != 0 {
		t.Fatalf("dropped %d instances, want 0: the key was written during the scrape", st.Deleted)
	}
	for i := 0; i < 6; i++ {
		scrape(nil)
	}
	if reported != 12 {
		t.Fatalf("reported %v in all, want 12", reported)
	}
	// Idle again with nothing written, it is dropped.
	if f.exp.Tracked() != 1 {
		t.Fatalf("tracking %d instances, want only the busy gauge", f.exp.Tracked())
	}
}

type hookWriter struct {
	h    http.Header
	buf  bytes.Buffer
	hook func()
}

func (w *hookWriter) Header() http.Header { return w.h }
func (w *hookWriter) WriteHeader(int)     {}
func (w *hookWriter) Write(p []byte) (int, error) {
	if w.hook != nil {
		w.hook()
		w.hook = nil
	}
	return w.buf.Write(p)
}

// A response that breaks off midway is not ended as if it were whole: the
// connection is closed under it, nothing of it is committed, and the next round
// reports it all.
func TestTrackedBrokenResponseAborts(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	for i := 0; i < 20000; i++ {
		f.c.WithLabelValues(fmt.Sprintf("k%05d", i)).Add(1)
	}
	w := &failingHijackWriter{h: http.Header{}, failAfter: 64 << 10}
	st := f.exp.Serve(w, httptest.NewRequest("GET", "/metrics", nil))
	if !st.Aborted || st.Delivered || !w.closed {
		t.Fatalf("aborted %v, delivered %v, connection closed %v; want the response aborted and the connection closed", st.Aborted, st.Delivered, w.closed)
	}
	rec := httptest.NewRecorder()
	f.exp.Serve(rec, httptest.NewRequest("GET", "/metrics", nil))
	var total float64
	for _, v := range parseCounters(t, rec.Body.String()) {
		total += v
	}
	if total != 20000 {
		t.Fatalf("the next round reported %v, want all 20000 again", total)
	}
}

// Where the connection cannot be taken over the handler panics with
// http.ErrAbortHandler, which the server answers by resetting the stream.
func TestTrackedBrokenResponseAbortsWithoutHijack(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	for i := 0; i < 20000; i++ {
		f.c.WithLabelValues(fmt.Sprintf("k%05d", i)).Add(1)
	}
	defer func() {
		if r := recover(); r != http.ErrAbortHandler {
			t.Fatalf("recovered %v, want http.ErrAbortHandler", r)
		}
		// The lock was let go: the exposer serves again.
		f.exp.Serve(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
	}()
	f.exp.Serve(&failingWriter{h: http.Header{}, failAfter: 64 << 10}, httptest.NewRequest("GET", "/metrics", nil))
}

type failingWriter struct {
	h         http.Header
	n         int
	failAfter int
}

func (w *failingWriter) Header() http.Header { return w.h }
func (w *failingWriter) WriteHeader(int)     {}
func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.failAfter {
		return 0, errors.New("broken pipe")
	}
	w.n += len(p)
	return len(p), nil
}

type failingHijackWriter struct {
	h         http.Header
	n         int
	failAfter int
	closed    bool
}

func (w *failingHijackWriter) Header() http.Header { return w.h }
func (w *failingHijackWriter) WriteHeader(int)     {}
func (w *failingHijackWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.failAfter {
		return 0, errors.New("broken pipe")
	}
	w.n += len(p)
	return len(p), nil
}

func (w *failingHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c1, c2 := net.Pipe()
	_ = c2.Close()
	return &closeRecorder{Conn: c1, closed: &w.closed}, nil, nil
}

type closeRecorder struct {
	net.Conn
	closed *bool
}

func (c *closeRecorder) Close() error {
	*c.closed = true
	return c.Conn.Close()
}

// Whatever the order of writes, processed scrapes, scrapes thrown away and
// peeks, with idle instances dropped along the way, every increment is
// processed exactly once.
func TestTrackedAckRandomized(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true, IdleScrapes: 2, HeartbeatScrapes: 1})
		s := newAckScraper(f.exp.Serve)
		rnd := rand.New(rand.NewSource(seed))
		written := map[string]float64{}
		keys := []string{"a", "b", "c", "d", "e"}
		for step := 0; step < 300; step++ {
			for n := rnd.Intn(4); n > 0; n-- {
				k := keys[rnd.Intn(len(keys))]
				if rnd.Intn(3) == 0 {
					continue // keys go quiet long enough to be dropped
				}
				v := float64(1 + rnd.Intn(9))
				f.c.WithLabelValues(k).Add(v)
				written[`c{key="`+k+`"}`] += v
			}
			switch x := rnd.Intn(10); {
			case x < 6:
				s.scrape(t, true)
			case x < 8:
				s.scrape(t, false)
			default:
				s.peek(t)
			}
		}
		s.scrape(t, true)
		s.scrape(t, true)
		for k, w := range written {
			if got := s.total[k]; got != w {
				t.Errorf("seed %d: %s processed %v, written %v", seed, k, got, w)
			}
		}
	}
}
