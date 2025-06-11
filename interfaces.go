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

package goetl

import (
	"context"
)

// Package goetl defines the core interfaces and types for the GoETL library.
//
// GoETL is a high-performance, interface-driven ETL (Extract, Transform, Load) library for Go, designed for streaming large datasets efficiently with extensibility, type safety, and minimal dependencies.
//
// This file contains the primary interfaces for data sources, sinks, transformation, filtering, aggregation, and error handling.

// Record represents a single data record in the pipeline.
// Each record is a map from field names to values, supporting heterogeneous data.
type Record map[string]interface{}

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

// TransformFunc is a function adapter for the Transformer interface.
// Allows ordinary functions to be used as Transformers.
type TransformFunc func(ctx context.Context, record Record) (Record, error)

// Transform implements the Transformer interface for TransformFunc.
func (f TransformFunc) Transform(ctx context.Context, record Record) (Record, error) {
	return f(ctx, record)
}

// Filter defines the interface for record filtering.
// Filters determine whether a record should be included in the output.
type Filter interface {
	// ShouldInclude returns true if the record should be included in the output.
	ShouldInclude(ctx context.Context, record Record) (bool, error)
}

// FilterFunc is a function adapter for the Filter interface.
// Allows ordinary functions to be used as Filters.
type FilterFunc func(ctx context.Context, record Record) (bool, error)

// ShouldInclude implements the Filter interface for FilterFunc.
func (f FilterFunc) ShouldInclude(ctx context.Context, record Record) (bool, error) {
	return f(ctx, record)
}

// Aggregator defines the interface for data aggregation operations.
// Aggregators process multiple records and produce a summary or grouped result.
type Aggregator interface {
	// Add processes a record for aggregation.
	Add(ctx context.Context, record Record) error
	// Result returns the aggregated result as a Record.
	Result() (Record, error)
	// Reset clears the aggregator state for reuse.
	Reset()
}

// ErrorStrategy defines how to handle transformation errors in the pipeline.
type ErrorStrategy int

const (
	// FailFast stops processing on the first error encountered.
	FailFast ErrorStrategy = iota
	// SkipErrors continues processing, skipping failed records.
	SkipErrors
	// CollectErrors continues processing, collecting all errors for later inspection.
	CollectErrors
)

// Pipeline represents a data transformation pipeline.
// See pipeline.go for implementation and GoDoc.
// type Pipeline struct { ... } // (removed, see pipeline.go)

// ErrorHandler defines how errors are handled during processing.
// Custom error handlers can be used to log, collect, or transform errors.
type ErrorHandler interface {
	// HandleError processes an error that occurred during transformation.
	// Returning a non-nil error will stop the pipeline; returning nil will continue.
	HandleError(ctx context.Context, record Record, err error) error
}

// ErrorHandlerFunc is a function adapter for the ErrorHandler interface.
// Allows ordinary functions to be used as error handlers.
type ErrorHandlerFunc func(ctx context.Context, record Record, err error) error

// HandleError implements the ErrorHandler interface for ErrorHandlerFunc.
func (f ErrorHandlerFunc) HandleError(ctx context.Context, record Record, err error) error {
	return f(ctx, record, err)
}
