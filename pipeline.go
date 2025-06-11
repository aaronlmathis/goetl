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
	"fmt"
	"io"
)

// Package goetl provides a high-performance, interface-driven ETL (Extract, Transform, Load) library for Go.
//
// The library is designed for streaming large datasets efficiently, with a focus on extensibility, type safety, and minimal dependencies.
//
// Core Concepts:
//   - DataSource: Interface for reading records from a data source (e.g., CSV, Parquet, PostgreSQL).
//   - DataSink: Interface for writing records to a data sink (e.g., CSV, Parquet, PostgreSQL).
//   - Transformer: Interface for transforming records (mapping, projection, normalization, etc).
//   - Filter: Interface for filtering records based on custom logic.
//   - Pipeline: Composable, chainable ETL pipeline for record-by-record processing.
//   - ErrorStrategy: Configurable error handling (fail fast, skip, collect, custom handler).
//
// The Pipeline API enables fluent construction of ETL flows, supporting mapping, filtering, aggregation, joining, and more.
//
// Example usage:
//
//   pipeline, err := goetl.NewPipeline().
//       From(csvReader).
//       Transform(myTransformer).
//       Filter(myFilter).
//       To(csvWriter).
//       WithErrorStrategy(goetl.SkipErrors).
//       Build()
//   if err != nil { log.Fatal(err) }
//   if err := pipeline.Execute(context.Background()); err != nil { log.Fatal(err) }
//
// All operations are streaming and memory-efficient, suitable for large-scale data processing.

// PipelineBuilder provides a fluent API for constructing transformation pipelines.
// Use NewPipeline() to create a new builder, then chain From, Transform, Filter, To, and configuration methods.
type PipelineBuilder struct {
	pipeline *Pipeline
}

// NewPipeline creates a new PipelineBuilder for constructing an ETL pipeline.
//
// Returns a new builder instance.
func NewPipeline() *PipelineBuilder {
	return &PipelineBuilder{
		pipeline: &Pipeline{
			transformers: make([]Transformer, 0),
			filters:      make([]Filter, 0),
			strategy:     FailFast,
		},
	}
}

// From sets the DataSource for the pipeline.
//
// source: a DataSource implementation (e.g., CSVReader, ParquetReader, PostgreSQLReader)
// Returns the builder for chaining.
func (pb *PipelineBuilder) From(source DataSource) *PipelineBuilder {
	pb.pipeline.source = source
	return pb
}

// Transform adds a Transformer to the pipeline.
//
// transformer: a Transformer implementation or TransformFunc
// Returns the builder for chaining.
func (pb *PipelineBuilder) Transform(transformer Transformer) *PipelineBuilder {
	pb.pipeline.transformers = append(pb.pipeline.transformers, transformer)
	return pb
}

// Filter adds a Filter to the pipeline.
//
// filter: a Filter implementation or FilterFunc
// Returns the builder for chaining.
func (pb *PipelineBuilder) Filter(filter Filter) *PipelineBuilder {
	pb.pipeline.filters = append(pb.pipeline.filters, filter)
	return pb
}

// Map adds a mapping transformation to the pipeline using a function.
//
// fn: function with signature func(ctx, record) (Record, error)
// Returns the builder for chaining.
func (pb *PipelineBuilder) Map(fn func(ctx context.Context, record Record) (Record, error)) *PipelineBuilder {
	return pb.Transform(TransformFunc(fn))
}

// Where adds a filtering condition to the pipeline using a function.
//
// fn: function with signature func(ctx, record) (bool, error)
// Returns the builder for chaining.
func (pb *PipelineBuilder) Where(fn func(ctx context.Context, record Record) (bool, error)) *PipelineBuilder {
	return pb.Filter(FilterFunc(fn))
}

// To sets the DataSink for the pipeline.
//
// sink: a DataSink implementation (e.g., CSVWriter, ParquetWriter, PostgreSQLWriter)
// Returns the builder for chaining.
func (pb *PipelineBuilder) To(sink DataSink) *PipelineBuilder {
	pb.pipeline.sink = sink
	return pb
}

// WithErrorStrategy sets the error handling strategy for the pipeline.
//
// strategy: ErrorStrategy (FailFast, SkipErrors, CollectErrors)
// Returns the builder for chaining.
func (pb *PipelineBuilder) WithErrorStrategy(strategy ErrorStrategy) *PipelineBuilder {
	pb.pipeline.strategy = strategy
	return pb
}

// WithErrorHandler sets a custom error handler for the pipeline.
//
// handler: ErrorHandler implementation
// Returns the builder for chaining.
func (pb *PipelineBuilder) WithErrorHandler(handler ErrorHandler) *PipelineBuilder {
	pb.pipeline.errorHandler = handler
	return pb
}

// Build validates and constructs the Pipeline from the builder.
//
// Returns the constructed pipeline, or an error if required components are missing.
func (pb *PipelineBuilder) Build() (*Pipeline, error) {
	if pb.pipeline.source == nil {
		return nil, fmt.Errorf("pipeline requires a data source")
	}
	if pb.pipeline.sink == nil {
		return nil, fmt.Errorf("pipeline requires a data sink")
	}
	return pb.pipeline, nil
}

// Pipeline represents a data processing pipeline for streaming ETL operations.
//
// Use Execute to process all records from the DataSource through transformations and filters, writing to the DataSink.
type Pipeline struct {
	transformers []Transformer
	filters      []Filter
	source       DataSource
	sink         DataSink
	strategy     ErrorStrategy
	errorHandler ErrorHandler
}

// Execute runs the pipeline, processing all records from source to sink.
//
// ctx: context for cancellation and deadlines
// Returns an error if a fatal error occurs or context is cancelled.
//
// The pipeline reads records from the DataSource, applies transformations and filters, and writes to the DataSink.
// Error handling is governed by the configured ErrorStrategy and ErrorHandler.
func (p *Pipeline) Execute(ctx context.Context) error {
	defer func() {
		if p.source != nil {
			p.source.Close()
		}
		if p.sink != nil {
			p.sink.Flush()
			p.sink.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next record
		record, err := p.source.Read(ctx)

		if err == io.EOF {
			break
		}
		if err != nil {
			if err := p.handleError(ctx, record, err); err != nil {
				return err
			}
			continue
		}

		// Skip empty records early
		if len(record) == 0 {
			continue
		}

		// Apply transformations
		transformedRecord, err := p.applyTransformations(ctx, record)
		if err != nil {
			if err := p.handleError(ctx, record, err); err != nil {
				return err
			}
			continue
		}

		// Skip empty transformed records
		if len(transformedRecord) == 0 {
			continue
		}

		// Apply filters
		shouldInclude, err := p.applyFilters(ctx, transformedRecord)
		if err != nil {
			if err := p.handleError(ctx, record, err); err != nil {
				return err
			}
			continue
		}
		if !shouldInclude {
			continue
		}

		// Write to sink
		if err := p.sink.Write(ctx, transformedRecord); err != nil {
			if err := p.handleError(ctx, transformedRecord, err); err != nil {
				return err
			}
		}
	}

	return nil
}

// applyFilters applies all configured filters to a record.
//
// ctx: context
// record: the record to filter
// Returns true if the record should be included, false otherwise, or an error if a filter returns an error.
func (p *Pipeline) applyFilters(ctx context.Context, record Record) (bool, error) {
	for _, filter := range p.filters {
		include, err := filter.ShouldInclude(ctx, record)
		if err != nil {
			return false, err
		}
		if !include {
			return false, nil
		}
	}
	return true, nil
}

// applyTransformations applies all configured transformers to a record in sequence.
//
// ctx: context
// record: the record to transform
// Returns the transformed record, or an error if a transformer returns an error.
func (p *Pipeline) applyTransformations(ctx context.Context, record Record) (Record, error) {
	current := record
	for _, transformer := range p.transformers {
		transformed, err := transformer.Transform(ctx, current)
		if err != nil {
			return nil, err
		}
		current = transformed
	}
	return current, nil
}

// handleError handles errors according to the pipeline's error strategy and handler.
//
// ctx: context
// record: the record being processed when the error occurred
// err: the error encountered
// Returns an error if processing should stop, or nil to continue.
func (p *Pipeline) handleError(ctx context.Context, record Record, err error) error {
	switch p.strategy {
	case FailFast:
		return err
	case SkipErrors:
		if p.errorHandler != nil {
			return p.errorHandler.HandleError(ctx, record, err)
		}
		return nil
	case CollectErrors:
		if p.errorHandler != nil {
			return p.errorHandler.HandleError(ctx, record, err)
		}
		return nil
	default:
		return err
	}
}
