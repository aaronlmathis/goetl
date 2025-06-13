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

// conditional.go - ConditionalTask implementation
package tasks

import (
	"context"
	"fmt"
	"time"
)

// ConditionalLogic defines the interface for conditional evaluation
type ConditionalLogic interface {
	Evaluate(ctx context.Context, input TaskInput) (bool, error)
	OnTrue() []string  // Task IDs to execute if condition is true
	OnFalse() []string // Task IDs to execute if condition is false
}

// ConditionalTask provides branching logic
type ConditionalTask struct {
	id           string
	condition    ConditionalLogic
	dependencies []string
	metadata     TaskMetadata
}

func (ct *ConditionalTask) ID() string             { return ct.id }
func (ct *ConditionalTask) Dependencies() []string { return ct.dependencies }
func (ct *ConditionalTask) Metadata() TaskMetadata { return ct.metadata }

func (ct *ConditionalTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()

	// Evaluate the condition
	result, err := ct.condition.Evaluate(ctx, input)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("condition evaluation failed: %w", err)
	}

	// For conditional tasks, we typically pass through the input records
	// The actual branching logic would be handled by the DAG executor
	// based on the condition result stored in the context

	updatedContext := make(map[string]interface{})
	for k, v := range input.Context {
		updatedContext[k] = v
	}
	updatedContext[ct.id+"_condition_result"] = result

	return TaskOutput{
		Records: input.Records, // Pass through records
		Context: updatedContext,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  int64(len(input.Records)),
			RecordsOut: int64(len(input.Records)),
			Success:    true,
		},
	}, nil
}

func (ct *ConditionalTask) SetRetryConfig(config *RetryConfig) { ct.metadata.RetryConfig = config }
func (ct *ConditionalTask) SetTimeout(timeout time.Duration)   { ct.metadata.Timeout = timeout }
func (ct *ConditionalTask) SetTriggerRule(rule TriggerRule)    { ct.metadata.TriggerRule = rule }
func (ct *ConditionalTask) SetDescription(description string) { 
    ct.metadata.Description = description 
}

func (ct *ConditionalTask) SetTags(tags ...string) { 
    ct.metadata.Tags = append(ct.metadata.Tags, tags...) 
}

func (ct *ConditionalTask) SetOwner(owner string) { 
    ct.metadata.Owner = owner 
}

func (ct *ConditionalTask) SetCustomField(key string, value interface{}) {
    if ct.metadata.CustomFields == nil {
        ct.metadata.CustomFields = make(map[string]interface{})
    }
    ct.metadata.CustomFields[key] = value
}

// NewConditionalTask creates a new ConditionalTask
func NewConditionalTask(id string, condition ConditionalLogic, dependencies []string, options ...TaskOption) *ConditionalTask {
	task := &ConditionalTask{
		id:           id,
		condition:    condition,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeConditional,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}
