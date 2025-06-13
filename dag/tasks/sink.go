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

// sink.go - SinkTask implementation
package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// SinkTask wraps a DataSink in the DAG framework
type SinkTask struct {
	id           string
	sink         core.DataSink
	dependencies []string
	metadata     TaskMetadata
}

func (st *SinkTask) ID() string             { return st.id }
func (st *SinkTask) Dependencies() []string { return st.dependencies }
func (st *SinkTask) Metadata() TaskMetadata { return st.metadata }

func (st *SinkTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()
	recordsIn := int64(len(input.Records))
	recordsProcessed := int64(0)

	for _, record := range input.Records {
		select {
		case <-ctx.Done():
			return TaskOutput{}, ctx.Err()
		default:
		}

		if err := st.sink.Write(ctx, record); err != nil {
			return TaskOutput{}, fmt.Errorf("sink write failed: %w", err)
		}
		recordsProcessed++
	}

	if err := st.sink.Flush(); err != nil {
		return TaskOutput{}, fmt.Errorf("sink flush failed: %w", err)
	}

	return TaskOutput{
		Records: []core.Record{}, // Sinks don't produce output records
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  recordsIn,
			RecordsOut: recordsProcessed,
			Success:    true,
		},
	}, nil
}

func (ct *SinkTask) SetDescription(description string) {
	ct.metadata.Description = description
}

func (ct *SinkTask) SetTags(tags ...string) {
	ct.metadata.Tags = append(ct.metadata.Tags, tags...)
}

func (ct *SinkTask) SetOwner(owner string) {
	ct.metadata.Owner = owner
}

func (ct *SinkTask) SetCustomField(key string, value interface{}) {
	if ct.metadata.CustomFields == nil {
		ct.metadata.CustomFields = make(map[string]interface{})
	}
	ct.metadata.CustomFields[key] = value
}

func (st *SinkTask) SetRetryConfig(config *RetryConfig) { st.metadata.RetryConfig = config }
func (st *SinkTask) SetTimeout(timeout time.Duration)   { st.metadata.Timeout = timeout }
func (st *SinkTask) SetTriggerRule(rule TriggerRule)    { st.metadata.TriggerRule = rule }

// NewSinkTask creates a new SinkTask
func NewSinkTask(id string, sink core.DataSink, dependencies []string, options ...TaskOption) *SinkTask {
	task := &SinkTask{
		id:           id,
		sink:         sink,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeSink,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}
