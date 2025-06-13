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

// source.go - SourceTask implementation
package tasks

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// SourceTask wraps a DataSource in the DAG framework
type SourceTask struct {
	id       string
	source   core.DataSource
	metadata TaskMetadata
}

func (st *SourceTask) ID() string             { return st.id }
func (st *SourceTask) Dependencies() []string { return []string{} }
func (st *SourceTask) Metadata() TaskMetadata { return st.metadata }

func (st *SourceTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()
	var records []core.Record
	var recordCount int64

	// Read all records from source
	for {
		select {
		case <-ctx.Done():
			return TaskOutput{}, ctx.Err()
		default:
		}

		record, err := st.source.Read(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return TaskOutput{}, fmt.Errorf("source read failed: %w", err)
		}

		records = append(records, record)

	}
	recordCount = int64(len(records))

	return TaskOutput{
		Records: records,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  0,
			RecordsOut: recordCount,
			Success:    true,
		},
	}, nil
}

func (ct *SourceTask) SetDescription(description string) {
	ct.metadata.Description = description
}

func (ct *SourceTask) SetTags(tags ...string) {
	ct.metadata.Tags = append(ct.metadata.Tags, tags...)
}

func (ct *SourceTask) SetOwner(owner string) {
	ct.metadata.Owner = owner
}

func (ct *SourceTask) SetCustomField(key string, value interface{}) {
	if ct.metadata.CustomFields == nil {
		ct.metadata.CustomFields = make(map[string]interface{})
	}
	ct.metadata.CustomFields[key] = value
}

func (st *SourceTask) SetRetryConfig(config *RetryConfig) { st.metadata.RetryConfig = config }
func (st *SourceTask) SetTimeout(timeout time.Duration)   { st.metadata.Timeout = timeout }
func (st *SourceTask) SetTriggerRule(rule TriggerRule)    { st.metadata.TriggerRule = rule }

// NewSourceTask creates a new SourceTask with the given ID and source
func NewSourceTask(id string, source core.DataSource, options ...TaskOption) *SourceTask {
	task := &SourceTask{
		id:     id,
		source: source,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeSource,
		},
	}

	// Apply options
	for _, opt := range options {
		opt(task)
	}

	return task
}
