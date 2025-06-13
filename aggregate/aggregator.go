//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of GoETL.
//
// GoETL is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// GoETL is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with GoETL. If not, see https://www.gnu.org/licenses/.

// join.go - JoinTask implementation
package aggregate

import (
	"context"

	"github.com/aaronlmathis/goetl/core"
)

// Aggregator defines the interface for data aggregation operations.
// Aggregators process multiple records and produce a summary or grouped result.
type Aggregator interface {
	// Add processes a record for aggregation.
	Add(ctx context.Context, record core.Record) error
	// Result returns the aggregated result as a Record.
	Result() (core.Record, error)
	// Reset clears the aggregator state for reuse.
	Reset()
}
