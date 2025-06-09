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

// Record represents a single data record in the pipeline
type Record map[string]interface{}

// DataSource defines the interface for data extraction
type DataSource interface {
	// Read returns the next record or io.EOF when no more records
	Read(ctx context.Context) (Record, error)
	// Close releases any resources held by the data source
	Close() error
}

// DataSink defines the interface for data loading
type DataSink interface {
	// Write outputs a single record
	Write(ctx context.Context, record Record) error
	// Flush ensures all buffered data is written
	Flush() error
	// Close releases any resources held by the data sink
	Close() error
}

// Transformer defines the interface for data transformation operations
type Transformer interface {
	// Transform applies the transformation to a record
	Transform(ctx context.Context, record Record) (Record, error)
}

// TransformFunc is a function adapter for the Transformer interface
type TransformFunc func(ctx context.Context, record Record) (Record, error)

// Transform implements the Transformer interface
func (f TransformFunc) Transform(ctx context.Context, record Record) (Record, error) {
	return f(ctx, record)
}

// Filter defines the interface for record filtering
type Filter interface {
	// ShouldInclude returns true if the record should be included in the output
	ShouldInclude(ctx context.Context, record Record) (bool, error)
}

// FilterFunc is a function adapter for the Filter interface
type FilterFunc func(ctx context.Context, record Record) (bool, error)

// ShouldInclude implements the Filter interface
func (f FilterFunc) ShouldInclude(ctx context.Context, record Record) (bool, error) {
	return f(ctx, record)
}

// Aggregator defines the interface for data aggregation operations
type Aggregator interface {
	// Add processes a record for aggregation
	Add(ctx context.Context, record Record) error
	// Result returns the aggregated result
	Result() (Record, error)
	// Reset clears the aggregator state
	Reset()
}

// ErrorStrategy defines how to handle transformation errors
type ErrorStrategy int

const (
	// FailFast stops processing on the first error
	FailFast ErrorStrategy = iota
	// SkipErrors continues processing, skipping failed records
	SkipErrors
	// CollectErrors continues processing, collecting all errors
	CollectErrors
)

// Pipeline represents a data transformation pipeline
type Pipeline struct {
	source       DataSource
	transformers []Transformer
	filters      []Filter
	sink         DataSink
	errorHandler ErrorHandler
	strategy     ErrorStrategy
}

// ErrorHandler defines how errors are handled during processing
type ErrorHandler interface {
	// HandleError processes an error that occurred during transformation
	HandleError(ctx context.Context, record Record, err error) error
}

// ErrorHandlerFunc is a function adapter for the ErrorHandler interface
type ErrorHandlerFunc func(ctx context.Context, record Record, err error) error

// HandleError implements the ErrorHandler interface
func (f ErrorHandlerFunc) HandleError(ctx context.Context, record Record, err error) error {
	return f(ctx, record, err)
}
