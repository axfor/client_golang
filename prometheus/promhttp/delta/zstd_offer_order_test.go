package delta_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	kzstd "github.com/klauspost/compress/zstd"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/promhttp/internal"
	_ "github.com/prometheus/client_golang/prometheus/promhttp/zstd"
)

// vmagent sends exactly this when -promscrape.zstdCompression is on.
const vmagentAccept = "zstd, gzip"

func serve(t *testing.T, opts promhttp.HandlerOpts, accept string) (string, []byte) {
	t.Helper()
	reg := prometheus.NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "probe_metric", Help: "h"})
	reg.MustRegister(g)
	g.Set(42)

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept-Encoding", accept)
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, opts).ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return rec.Header().Get("Content-Encoding"), body
}

// Equal q values are broken by the order of the server's offers, so the default
// list -- identity, gzip, zstd -- always settles on gzip for "zstd, gzip".
func TestDefaultOffersPickGzipEvenWhenZstdIsAccepted(t *testing.T) {
	enc, _ := serve(t, promhttp.HandlerOpts{}, vmagentAccept)
	if enc != "gzip" {
		t.Fatalf("default offers: got %q, want gzip", enc)
	}
}

func TestZstdFirstOffersPickZstd(t *testing.T) {
	opts := promhttp.HandlerOpts{
		OfferedCompressions: []promhttp.Compression{promhttp.Zstd, promhttp.Gzip, promhttp.Identity},
	}
	enc, body := serve(t, opts, vmagentAccept)
	if enc != "zstd" {
		t.Fatalf("zstd-first offers: got %q, want zstd", enc)
	}
	d, err := kzstd.NewReader(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	plain, err := io.ReadAll(d)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plain), "probe_metric 42") {
		t.Fatalf("body does not decompress to the exposition: %q", string(plain))
	}
}

// Offering Zstd without importing promhttp/zstd is worse than not offering it:
// negotiation settles on zstd, the writer cannot be built, and the handler
// falls back to no compression at all.
func TestOfferingZstdWithoutTheImportFallsBackToNoCompression(t *testing.T) {
	saved := internal.NewZstdWriter
	internal.NewZstdWriter = nil
	defer func() { internal.NewZstdWriter = saved }()

	opts := promhttp.HandlerOpts{
		OfferedCompressions: []promhttp.Compression{promhttp.Zstd, promhttp.Gzip, promhttp.Identity},
		ErrorHandling:       promhttp.ContinueOnError,
	}
	enc, body := serve(t, opts, vmagentAccept)
	if enc != "" {
		t.Fatalf("got Content-Encoding %q, want none (identity)", enc)
	}
	if !strings.Contains(string(body), "probe_metric 42") {
		t.Fatalf("body is not plain exposition: %q", string(body))
	}
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	t.Logf("没有空导入时:Content-Encoding=%q,响应 %d 字节(未压缩)", enc, len(body))
}

// A client that only takes gzip still gets gzip from the zstd-first list.
func TestZstdFirstOffersStillServeGzipOnlyClients(t *testing.T) {
	opts := promhttp.HandlerOpts{
		OfferedCompressions: []promhttp.Compression{promhttp.Zstd, promhttp.Gzip, promhttp.Identity},
	}
	if enc, _ := serve(t, opts, "gzip"); enc != "gzip" {
		t.Fatalf("gzip-only client: got %q, want gzip", enc)
	}
	if enc, _ := serve(t, opts, ""); enc != "" {
		t.Fatalf("no Accept-Encoding: got %q, want none", enc)
	}
}

var _ = http.StatusOK
