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
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// final sends the scraper's last acknowledgement, as vmagent does when it stops.
func (s *ackScraper) final(t *testing.T) (ScrapeStats, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set(AckHeader, s.ack())
	req.Header.Set(FinalHeader, "1")
	rec := httptest.NewRecorder()
	return s.serve(rec, req), rec
}

// A scraper that fetched a round and stopped before processing it says so in
// its last acknowledgement: the round is rolled back, and the scraper after it,
// which starts with AckNew, gets it again. Without the last acknowledgement the
// new scraper's AckNew commits the round nobody processed, and it is lost.
func TestFinalAckRollsBackRoundNotProcessed(t *testing.T) {
	for _, withFinal := range []bool{true, false} {
		f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
		s := newAckScraper(f.exp.Serve)
		f.c.WithLabelValues("a").Add(5)
		s.scrape(t, true)
		f.c.WithLabelValues("a").Add(3)
		s.scrape(t, false) // fetched, then the scraper stopped before processing it
		if withFinal {
			if st, _ := s.final(t); st.Settled != SettledRolledBack {
				t.Fatalf("the last acknowledgement settled %v, want rolled back", st.Settled)
			}
		}
		restarted := newAckScraper(f.exp.Serve)
		f.c.WithLabelValues("a").Add(1)
		restarted.scrape(t, true)
		restarted.scrape(t, true)
		got := s.total[`c{key="a"}`] + restarted.total[`c{key="a"}`]
		switch {
		case withFinal && got != 9:
			t.Fatalf("with the last acknowledgement processed %v, want 9, every increment once", got)
		case !withFinal && got != 6:
			t.Fatalf("without it processed %v; the test expects the round not processed to be lost, 6", got)
		}
	}
}

// A round processed is committed by the last acknowledgement and not reported
// to the scraper after it.
func TestFinalAckCommitsRoundProcessed(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	f.c.WithLabelValues("a").Add(3)
	s.scrape(t, true)
	if st, _ := s.final(t); st.Settled != SettledCommitted {
		t.Fatalf("the last acknowledgement settled %v, want committed", st.Settled)
	}
	restarted := newAckScraper(f.exp.Serve)
	restarted.scrape(t, true)
	if got := s.total[`c{key="a"}`] + restarted.total[`c{key="a"}`]; got != 8 {
		t.Fatalf("processed %v, want 8", got)
	}
}

// The last acknowledgement takes nothing: no round, no body, and what is
// written before it goes out with the next scrape. Every round says the target
// takes it.
func TestFinalAckTakesNothing(t *testing.T) {
	f := newTrackedFixture(t, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(f.exp.Serve)
	f.c.WithLabelValues("a").Add(5)
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set(AckHeader, AckNew)
	rec := httptest.NewRecorder()
	f.exp.Serve(rec, req)
	if rec.Header().Get(FeaturesHeader) != "final" {
		t.Fatalf("a round does not say the target takes a last acknowledgement: %v", rec.Header())
	}
	s.scrape(t, true)
	f.c.WithLabelValues("a").Add(2)
	st, rec := s.final(t)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 || rec.Header().Get(RoundHeader) != "" || st.Round != 0 {
		t.Fatalf("the last acknowledgement got code %d, body %q, round %q (%d); want 204 and nothing", rec.Code, rec.Body.String(), rec.Header().Get(RoundHeader), st.Round)
	}
	restarted := newAckScraper(f.exp.Serve)
	restarted.scrape(t, true)
	if got := restarted.total[`c{key="a"}`]; got != 2 {
		t.Fatalf("the scrape after it got %v, want 2, written before it", got)
	}
}

// The Gather path takes the last acknowledgement the same way.
func TestGatherFinalAck(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total", Help: "c"}, []string{"key"})
	reg.MustRegister(c)
	exp := New(reg, Options{ReportIncrements: true, DisableHeartbeat: true})
	s := newAckScraper(exp.Serve)
	c.WithLabelValues("a").Add(5)
	s.scrape(t, true)
	c.WithLabelValues("a").Add(3)
	s.scrape(t, false)
	if st, rec := s.final(t); st.Settled != SettledRolledBack || rec.Code != http.StatusNoContent {
		t.Fatalf("settled %v, code %d; want rolled back, 204", st.Settled, rec.Code)
	}
	restarted := newAckScraper(exp.Serve)
	restarted.scrape(t, true)
	if got := s.total[`c{key="a"}`] + restarted.total[`c{key="a"}`]; got != 8 {
		t.Fatalf("processed %v, want 8", got)
	}
}
