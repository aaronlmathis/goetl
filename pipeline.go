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

// PipelineBuilder provides a fluent API for constructing transformation pipelines
type PipelineBuilder struct {
	pipeline *Pipeline
}

// NewPipeline creates a new pipeline builder
func NewPipeline() *PipelineBuilder {
	return &PipelineBuilder{
		pipeline: &Pipeline{
			transformers: make([]Transformer, 0),
			filters:      make([]Filter, 0),
			strategy:     FailFast,
		},
	}
}

// From sets the data source for the pipeline
func (pb *PipelineBuilder) From(source DataSource) *PipelineBuilder {
	pb.pipeline.source = source
	return pb
}

// Transform adds a transformation step to the pipeline
func (pb *PipelineBuilder) Transform(transformer Transformer) *PipelineBuilder {
	pb.pipeline.transformers = append(pb.pipeline.transformers, transformer)
	return pb
}

// Filter adds a filter step to the pipeline
func (pb *PipelineBuilder) Filter(filter Filter) *PipelineBuilder {
	pb.pipeline.filters = append(pb.pipeline.filters, filter)
	return pb
}

// Map adds a mapping transformation to the pipeline
func (pb *PipelineBuilder) Map(fn func(ctx context.Context, record Record) (Record, error)) *PipelineBuilder {
	return pb.Transform(TransformFunc(fn))
}

// Where adds a filtering condition to the pipeline
func (pb *PipelineBuilder) Where(fn func(ctx context.Context, record Record) (bool, error)) *PipelineBuilder {
	return pb.Filter(FilterFunc(fn))
}

// To sets the data sink for the pipeline
func (pb *PipelineBuilder) To(sink DataSink) *PipelineBuilder {
	pb.pipeline.sink = sink
	return pb
}

// WithErrorStrategy sets the error handling strategy
func (pb *PipelineBuilder) WithErrorStrategy(strategy ErrorStrategy) *PipelineBuilder {
	pb.pipeline.strategy = strategy
	return pb
}

// WithErrorHandler sets a custom error handler
func (pb *PipelineBuilder) WithErrorHandler(handler ErrorHandler) *PipelineBuilder {
	pb.pipeline.errorHandler = handler
	return pb
}

// Build returns the constructed pipeline
func (pb *PipelineBuilder) Build() (*Pipeline, error) {
	if pb.pipeline.source == nil {
		return nil, fmt.Errorf("pipeline requires a data source")
	}
	if pb.pipeline.sink == nil {
		return nil, fmt.Errorf("pipeline requires a data sink")
	}
	return pb.pipeline, nil
}

// Execute runs the pipeline and processes all records
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
		fmt.Printf("Pipeline read: %+v, err: %v\n", record, err)
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

		// Write to sink - removed the duplicate empty check since we already checked above
		if err := p.sink.Write(ctx, transformedRecord); err != nil {
			if err := p.handleError(ctx, transformedRecord, err); err != nil {
				return err
			}
		}
	}

	return nil
}

// Pipeline represents a data processing pipeline
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
