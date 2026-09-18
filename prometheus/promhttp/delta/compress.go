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
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Compression negotiation: zstd when the scraper accepts it, gzip otherwise, and
// no compression when it accepts neither.
//
// zstd runs at the fastest level with a single encoder goroutine, from a pool.
// The default concurrency starts one goroutine per GOMAXPROCS, which finishes
// sooner in wall-clock but burns more CPU in total; a sidecar wants to occupy
// few cores and spend little CPU, so concurrency is pinned to one. Measured on a
// 1,573 MiB response: gzip -6 costs 5.37 s of CPU, gzip -1 2.39 s, and zstd
// fastest at single concurrency 1.63 s, producing 66.7 MiB, 30% smaller than
// gzip -1.
var (
	zstdPool = sync.Pool{New: func() any {
		w, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		return w
	}}
	gzipPool = sync.Pool{New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	}}
	bufioPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, 256<<10) }}
)

type response struct {
	w  *bufio.Writer
	gz *gzip.Writer
	zw *zstd.Encoder
}

func newResponse(w http.ResponseWriter, r *http.Request) *response {
	rs := &response{}
	var out io.Writer = w
	if r != nil {
		switch negotiateEncoding(r.Header.Get("Accept-Encoding")) {
		case "zstd":
			w.Header().Set("Content-Encoding", "zstd")
			rs.zw = zstdPool.Get().(*zstd.Encoder)
			rs.zw.Reset(w)
			out = rs.zw
		case "gzip":
			w.Header().Set("Content-Encoding", "gzip")
			rs.gz = gzipPool.Get().(*gzip.Writer)
			rs.gz.Reset(w)
			out = rs.gz
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	rs.w = bufioPool.Get().(*bufio.Writer)
	rs.w.Reset(out)
	return rs
}

// close flushes and returns each layer to its pool, reporting the final error
// and whether the response was delivered in full.
func (rs *response) close(err error, r *http.Request) (error, bool) {
	if ferr := rs.w.Flush(); err == nil {
		err = ferr
	}
	rs.w.Reset(nil)
	bufioPool.Put(rs.w)
	if rs.zw != nil {
		if cerr := rs.zw.Close(); err == nil {
			err = cerr
		}
		rs.zw.Reset(nil)
		zstdPool.Put(rs.zw)
	}
	if rs.gz != nil {
		if cerr := rs.gz.Close(); err == nil {
			err = cerr
		}
		rs.gz.Reset(nil)
		gzipPool.Put(rs.gz)
	}
	if err == nil && r != nil {
		err = r.Context().Err()
	}
	return err, err == nil
}

// negotiateEncoding parses Accept-Encoding, matching whole tokens and treating
// q=0 as a refusal, with zstd winning ties.
//
// promhttp's own negotiation breaks ties by the order it offers encodings in
// (identity, gzip, zstd by default), so a scraper sending "zstd, gzip" gets gzip
// back. This prefers zstd explicitly.
func negotiateEncoding(acceptEncoding string) string {
	var zstdOK, gzipOK bool
	for _, part := range strings.Split(acceptEncoding, ",") {
		name, params, _ := strings.Cut(part, ";")
		if q, ok := strings.CutPrefix(strings.TrimSpace(params), "q="); ok {
			if v, err := strconv.ParseFloat(q, 64); err == nil && v == 0 {
				continue // explicitly refused
			}
		}
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "zstd":
			zstdOK = true
		case "gzip":
			gzipOK = true
		}
	}
	switch {
	case zstdOK:
		return "zstd"
	case gzipOK:
		return "gzip"
	default:
		return ""
	}
}
