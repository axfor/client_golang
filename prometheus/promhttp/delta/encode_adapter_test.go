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

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// encodeFamilies writes whole families through the path a scrape uses --
// familyRows' start, add and encode -- for tests that build their dtos by hand
// rather than going through a registry. rows[i] are the entries behind
// mfs[i].Metric; a missing one sends the family to expfmt, as a scrape would.
func encodeFamilies(w *bufio.Writer, enc expfmt.Encoder, mfs []*dto.MetricFamily, rows [][]*entry, gen func(*entry) int64, es *encState, genLabel, typeLabel string) error {
	var fr familyRows
	for i, mf := range mfs {
		if len(mf.Metric) == 0 {
			continue
		}
		fr.start(&dto.MetricFamily{Name: mf.Name, Help: mf.Help, Type: mf.Type}, es, genLabel, typeLabel)
		for j, m := range mf.Metric {
			var en *entry
			if i < len(rows) && j < len(rows[i]) {
				en = rows[i][j]
			}
			var g int64
			if en != nil {
				g = gen(en)
			}
			fr.add(m, en, g, es)
		}
		err := fr.encode(w, enc, es, gen, genLabel, typeLabel)
		fr.reset()
		if err != nil {
			return err
		}
	}
	return nil
}
