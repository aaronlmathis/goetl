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

package core

import (
	"context"
)

// Package core defines the core interfaces for the GoETL library.
//
// GoETL is a high-performance, interface-driven ETL (Extract, Transform, Load) library for Go,
// designed for streaming large datasets efficiently with extensibility, type safety, and minimal dependencies.
//
// This file contains the primary interfaces for data sources, sinks, transformation, filtering, and aggregation.

// DataSource defines the interface for data extraction.
// Implementations stream records from a source (e.g., CSV, Parquet, PostgreSQL).
type DataSource interface {
	// Read returns the next record or io.EOF when no more records are available.
	Read(ctx context.Context) (Record, error)
	// Close releases any resources held by the data source.
	Close() error
}

// DataSink defines the interface for data loading.
// Implementations write records to a destination (e.g., CSV, Parquet, PostgreSQL).
type DataSink interface {
	// Write outputs a single record to the sink.
	Write(ctx context.Context, record Record) error
	// Flush ensures all buffered data is written to the sink.
	Flush() error
	// Close releases any resources held by the data sink.
	Close() error
}

// Transformer defines the interface for data transformation operations.
// Transformers modify or enrich records as they pass through the pipeline.
type Transformer interface {
	// Transform applies the transformation to a record and returns the result.
	Transform(ctx context.Context, record Record) (Record, error)
}

// Filter defines the interface for record filtering.
// Filters determine whether a record should be included in the output.
type Filter interface {
	// ShouldInclude returns true if the record should be included in the output.
	ShouldInclude(ctx context.Context, record Record) (bool, error)
}
