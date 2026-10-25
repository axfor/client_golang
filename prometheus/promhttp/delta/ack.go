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
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
)

// Acknowledged delivery.
//
// A scrape counts as delivered once the whole response has been written, but
// written is not received: a response sitting in the socket's send buffer when
// the scraper gives up on it, its scrape timeout passing, is committed here and
// thrown away there, and its increments are lost. Only the scraper knows what it
// processed, so it says so.
//
// Every response carries RoundHeader, "<session>:<round>", the session telling
// this process's rounds from an earlier one's. A scraper that acknowledges sends
// AckHeader on every request:
//
//	AckNew              it has never had a round from this target: it has
//	                    just started, so it cannot tell whether it processed
//	                    the one held, which is committed, as delivery always
//	                    did; this round is held
//	"<session>:<round>" the last round it processed in full, "<session>:0"
//	                    when it has had rounds of the session but processed
//	                    none: the held round is committed if that is it and
//	                    rolled back otherwise, to be reported again; this
//	                    round is held
//	AckPeek             it only looks at the response, as vmagent's target
//	                    status page does, and will not process it: the held
//	                    round is left alone, and this one is rolled back as
//	                    soon as it is written
//
// A request without AckHeader, from a scraper that does not acknowledge or a
// curl, commits the held round and its own on delivery, as before the protocol:
// the single scraper rule stands for anything that does not acknowledge.
//
// A scraper about to stop sends its last acknowledgement on a request of its
// own, with FinalHeader: the held round is settled by it, and nothing is taken,
// so the process after it, which starts with AckNew, finds no round held for it
// to commit unprocessed. Every response says the target takes it, in
// FeaturesHeader; a scraper sends it only to a target that says so, since one
// that does not would take the request for a scrape and hold a round nobody
// processes.
//
// What is left: a scraper that stops without sending it -- killed, or crashed --
// between a round and its next request, which commits the round, as delivery
// did; and one that processes a response in part and then fails, which the
// protocol cannot express, so the round is rolled back and what was processed
// is counted again.
const (
	RoundHeader    = "X-Prometheus-Delta-Round"
	AckHeader      = "X-Prometheus-Delta-Ack"
	FinalHeader    = "X-Prometheus-Delta-Final"
	FeaturesHeader = "X-Prometheus-Delta-Features"
	AckNew         = "new"
	AckPeek        = "peek"

	// features is the value of FeaturesHeader.
	features = "final"
)

// ackState holds the round awaiting acknowledgement.
type ackState struct {
	session string // distinguishes the rounds of this process from those of an earlier one
	held    string // the round awaiting its acknowledgement, "" when none is
}

func newAckState() ackState {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("delta: cannot read random bytes: " + err.Error())
	}
	return ackState{session: hex.EncodeToString(b[:])}
}

// ackOf returns the request's acknowledgement and whether it carries one.
func ackOf(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}
	v, ok := r.Header[AckHeader]
	if !ok || len(v) == 0 {
		return "", false
	}
	return v[0], true
}

// acknowledges reports whether r comes from a scraper that will acknowledge the
// round it gets, so that the round is held rather than committed on delivery.
func acknowledges(r *http.Request) bool {
	v, ok := ackOf(r)
	return ok && v != AckPeek
}

// final reports whether r is a scraper's last acknowledgement, see FinalHeader:
// it settles the held round and takes nothing.
func final(r *http.Request) bool {
	return r != nil && r.Header.Get(FinalHeader) != "" && acknowledges(r)
}

// peeks reports whether r only looks at the response.
func peeks(r *http.Request) bool {
	v, ok := ackOf(r)
	return ok && v == AckPeek
}

func (a *ackState) roundID(round uint64) string {
	return a.session + ":" + strconv.FormatUint(round, 10)
}

// settle decides what becomes of the held round on a new request: whether
// there was one, and whether it is committed or rolled back. A peek leaves it.
func (a *ackState) settle(r *http.Request) (held, commit bool) {
	if a.held == "" || peeks(r) {
		return false, false
	}
	id := a.held
	a.held = ""
	v, ok := ackOf(r)
	if !ok || v == AckNew {
		return true, true
	}
	return true, v == id
}
