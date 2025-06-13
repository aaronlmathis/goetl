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

// transform.go - TransformTask and related implementations
package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/aaronlmathis/goetl/aggregate"
	"github.com/aaronlmathis/goetl/core"
)

// TransformTask wraps a Transformer in the DAG framework
type TransformTask struct {
	id           string
	transformer  core.Transformer
	dependencies []string
	metadata     TaskMetadata
}

func (tt *TransformTask) ID() string             { return tt.id }
func (tt *TransformTask) Dependencies() []string { return tt.dependencies }
func (tt *TransformTask) Metadata() TaskMetadata { return tt.metadata }

func (tt *TransformTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()
	var transformedRecords []core.Record
	recordsIn := int64(len(input.Records))

	for _, record := range input.Records {
		select {
		case <-ctx.Done():
			return TaskOutput{}, ctx.Err()
		default:
		}

		transformed, err := tt.transformer.Transform(ctx, record)
		if err != nil {
			return TaskOutput{}, fmt.Errorf("transform failed: %w", err)
		}

		transformedRecords = append(transformedRecords, transformed)
	}

	return TaskOutput{
		Records: transformedRecords,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  recordsIn,
			RecordsOut: int64(len(transformedRecords)),
			Success:    true,
		},
	}, nil
}

func (tt *TransformTask) SetRetryConfig(config *RetryConfig) { tt.metadata.RetryConfig = config }
func (tt *TransformTask) SetTimeout(timeout time.Duration)   { tt.metadata.Timeout = timeout }
func (tt *TransformTask) SetTriggerRule(rule TriggerRule)    { tt.metadata.TriggerRule = rule }

// FilterTask wraps a Filter in the DAG framework
type FilterTask struct {
	id           string
	filter       core.Filter
	dependencies []string
	metadata     TaskMetadata
}

func (ft *FilterTask) ID() string             { return ft.id }
func (ft *FilterTask) Dependencies() []string { return ft.dependencies }
func (ft *FilterTask) Metadata() TaskMetadata { return ft.metadata }

func (ft *FilterTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()
	var filteredRecords []core.Record
	recordsIn := int64(len(input.Records))

	for _, record := range input.Records {
		select {
		case <-ctx.Done():
			return TaskOutput{}, ctx.Err()
		default:
		}

		shouldInclude, err := ft.filter.ShouldInclude(ctx, record)
		if err != nil {
			return TaskOutput{}, fmt.Errorf("filter failed: %w", err)
		}

		if shouldInclude {
			filteredRecords = append(filteredRecords, record)
		}
	}

	return TaskOutput{
		Records: filteredRecords,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  recordsIn,
			RecordsOut: int64(len(filteredRecords)),
			Success:    true,
		},
	}, nil
}
func (ft *FilterTask) SetDescription(description string) {
	ft.metadata.Description = description
}

func (ft *FilterTask) SetTags(tags ...string) {
	ft.metadata.Tags = append(ft.metadata.Tags, tags...)
}

func (ft *FilterTask) SetOwner(owner string) {
	ft.metadata.Owner = owner
}

func (ft *FilterTask) SetCustomField(key string, value interface{}) {
	if ft.metadata.CustomFields == nil {
		ft.metadata.CustomFields = make(map[string]interface{})
	}
	ft.metadata.CustomFields[key] = value
}

func (ft *FilterTask) SetRetryConfig(config *RetryConfig) { ft.metadata.RetryConfig = config }
func (ft *FilterTask) SetTimeout(timeout time.Duration)   { ft.metadata.Timeout = timeout }
func (ft *FilterTask) SetTriggerRule(rule TriggerRule)    { ft.metadata.TriggerRule = rule }

// AggregateTask wraps aggregation operations in the DAG framework
type AggregateTask struct {
	id           string
	aggregator   aggregate.Aggregator
	dependencies []string
	metadata     TaskMetadata
}

func (at *AggregateTask) ID() string             { return at.id }
func (at *AggregateTask) Dependencies() []string { return at.dependencies }
func (at *AggregateTask) Metadata() TaskMetadata { return at.metadata }

func (at *AggregateTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()
	recordsIn := int64(len(input.Records))

	// Reset aggregator for clean state
	at.aggregator.Reset()

	// Process all input records
	for _, record := range input.Records {
		select {
		case <-ctx.Done():
			return TaskOutput{}, ctx.Err()
		default:
		}

		if err := at.aggregator.Add(ctx, record); err != nil {
			return TaskOutput{}, fmt.Errorf("aggregation failed: %w", err)
		}
	}

	// Get aggregated result
	result, err := at.aggregator.Result()
	if err != nil {
		return TaskOutput{}, fmt.Errorf("aggregation result failed: %w", err)
	}

	return TaskOutput{
		Records: []core.Record{result}, // Aggregation produces a single result record
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  recordsIn,
			RecordsOut: 1,
			Success:    true,
		},
	}, nil
}

func (tt *TransformTask) SetDescription(description string) {
	tt.metadata.Description = description
}

func (tt *TransformTask) SetTags(tags ...string) {
	tt.metadata.Tags = append(tt.metadata.Tags, tags...)
}

func (tt *TransformTask) SetOwner(owner string) {
	tt.metadata.Owner = owner
}

func (tt *TransformTask) SetCustomField(key string, value interface{}) {
	if tt.metadata.CustomFields == nil {
		tt.metadata.CustomFields = make(map[string]interface{})
	}
	tt.metadata.CustomFields[key] = value
}

func (at *AggregateTask) SetRetryConfig(config *RetryConfig) { at.metadata.RetryConfig = config }
func (at *AggregateTask) SetTimeout(timeout time.Duration)   { at.metadata.Timeout = timeout }
func (at *AggregateTask) SetTriggerRule(rule TriggerRule)    { at.metadata.TriggerRule = rule }
func (at *AggregateTask) SetDescription(description string) {
	at.metadata.Description = description
}

func (at *AggregateTask) SetTags(tags ...string) {
	at.metadata.Tags = append(at.metadata.Tags, tags...)
}

func (at *AggregateTask) SetOwner(owner string) {
	at.metadata.Owner = owner
}

func (at *AggregateTask) SetCustomField(key string, value interface{}) {
	if at.metadata.CustomFields == nil {
		at.metadata.CustomFields = make(map[string]interface{})
	}
	at.metadata.CustomFields[key] = value
}

// NewTransformTask creates a new TransformTask
func NewTransformTask(id string, transformer core.Transformer, dependencies []string, options ...TaskOption) *TransformTask {
	task := &TransformTask{
		id:           id,
		transformer:  transformer,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeTransform,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}

// NewFilterTask creates a new FilterTask
func NewFilterTask(id string, filter core.Filter, dependencies []string, options ...TaskOption) *FilterTask {
	task := &FilterTask{
		id:           id,
		filter:       filter,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeFilter,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}

// NewAggregateTask creates a new AggregateTask
func NewAggregateTask(id string, aggregator aggregate.Aggregator, dependencies []string, options ...TaskOption) *AggregateTask {
	task := &AggregateTask{
		id:           id,
		aggregator:   aggregator,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeAggregate,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}
