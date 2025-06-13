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

// base.go - Task interface and base types
package tasks

import (
	"context"
	"math/rand"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// TaskType represents the type of task
type TaskType string

const (
	TaskTypeSource      TaskType = "source"
	TaskTypeTransform   TaskType = "transform"
	TaskTypeSink        TaskType = "sink"
	TaskTypeJoin        TaskType = "join"
	TaskTypeCDC         TaskType = "cdc"
	TaskTypeSCD         TaskType = "scd"
	TaskTypeConditional TaskType = "conditional"
	TaskTypeFilter      TaskType = "filter"
	TaskTypeAggregate   TaskType = "aggregate"
)

// TriggerRule defines when a task should be triggered
type TriggerRule string

const (
	TriggerRuleAllSuccess    TriggerRule = "all_success"
	TriggerRuleAllFailed     TriggerRule = "all_failed"
	TriggerRuleAllDone       TriggerRule = "all_done"
	TriggerRuleOneSuccess    TriggerRule = "one_success"
	TriggerRuleOneFailed     TriggerRule = "one_failed"
	TriggerRuleNoneFailedMin TriggerRule = "none_failed_min_one_success"
)

// BackoffStrategy interface for advanced retry strategies
type BackoffStrategy interface {
	Delay(attempt int) time.Duration
}

// RetryConfig defines retry behavior for tasks
type RetryConfig struct {
	MaxRetries int
	Backoff    time.Duration   // Simple backoff duration
	Strategy   BackoffStrategy // Advanced backoff strategy (optional)
	RetryOn    []error         // Specific errors to retry on
}

// GetDelay returns the delay for a given attempt
func (rc *RetryConfig) GetDelay(attempt int) time.Duration {
	if rc.Strategy != nil {
		return rc.Strategy.Delay(attempt)
	}
	return rc.Backoff
}

// ExponentialBackoff implements BackoffStrategy
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (eb *ExponentialBackoff) Delay(attempt int) time.Duration {
	delay := eb.BaseDelay * time.Duration(1<<uint(attempt))
	if delay > eb.MaxDelay {
		delay = eb.MaxDelay
	}
	return delay
}

// LinearBackoff implements linear backoff strategy
type LinearBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (lb *LinearBackoff) Delay(attempt int) time.Duration {
	delay := lb.BaseDelay * time.Duration(attempt)
	if delay > lb.MaxDelay {
		delay = lb.MaxDelay
	}
	return delay
}

// FixedBackoff implements fixed delay backoff strategy
type FixedBackoff struct {
	FixedDelay time.Duration
}

func (fb *FixedBackoff) Delay(attempt int) time.Duration {
	return fb.FixedDelay
}

// JitteredBackoff adds randomness to exponential backoff
type JitteredBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    float64 // 0.0 to 1.0
}

func (jb *JitteredBackoff) Delay(attempt int) time.Duration {
	delay := jb.BaseDelay * time.Duration(1<<uint(attempt))
	if delay > jb.MaxDelay {
		delay = jb.MaxDelay
	}

	// Add jitter (randomness)
	if jb.Jitter > 0 {
		jitterAmount := float64(delay) * jb.Jitter * (rand.Float64() - 0.5)
		delay += time.Duration(jitterAmount)
	}

	return delay
}

// NoBackoff implements no delay strategy
type NoBackoff struct{}

func (nb *NoBackoff) Delay(attempt int) time.Duration {
	return 0
}

// TaskMetadata holds metadata about a task
type TaskMetadata struct {
	Name           string
	Description    string // Add description for debugging
	TaskType       TaskType
	StartTime      time.Time
	EndTime        time.Time
	Duration       time.Duration
	RecordsRead    int64
	RecordsWritten int64
	Errors         []error
	RetryConfig    *RetryConfig
	Timeout        time.Duration
	TriggerRule    TriggerRule
	Tags           []string               // Add tags for grouping/filtering
	Owner          string                 // Add ownership information
	CustomFields   map[string]interface{} // Extensible metadata
}

func (tm *TaskMetadata) IsComplete() bool {
	return !tm.EndTime.IsZero()
}

func (tm *TaskMetadata) HasErrors() bool {
	return len(tm.Errors) > 0
}

func (tm *TaskMetadata) GetExecutionTime() time.Duration {
	if tm.IsComplete() {
		return tm.EndTime.Sub(tm.StartTime)
	}
	return tm.Duration
}

// TaskInput represents input data for task execution
type TaskInput struct {
	Records   []core.Record
	Context   map[string]interface{}
	SourceMap map[string][]core.Record
	Metadata  map[string]TaskResultMetadata
}

// TaskOutput represents output data from task execution
type TaskOutput struct {
	Records  []core.Record
	Context  map[string]interface{}
	Metadata TaskResultMetadata
}

// TaskResultMetadata holds execution result metadata
type TaskResultMetadata struct {
	StartTime    time.Time
	EndTime      time.Time
	RecordsIn    int64
	RecordsOut   int64
	Success      bool
	Error        error
	AttemptCount int
}

// Task defines the interface that all tasks must implement
type Task interface {
	ID() string
	Dependencies() []string
	Execute(ctx context.Context, input TaskInput) (TaskOutput, error)
	Metadata() TaskMetadata
	SetRetryConfig(config *RetryConfig)
	SetTimeout(timeout time.Duration)
	SetTriggerRule(rule TriggerRule)
	SetDescription(description string)            // New method for setting description
	SetTags(tags ...string)                       // New method for setting tags
	SetOwner(owner string)                        // New method for setting owner
	SetCustomField(key string, value interface{}) // New method for setting custom fields
}

// TaskOption is a functional option for configuring tasks
type TaskOption func(Task)

// WithRetries sets the retry configuration for a task
func WithRetries(maxRetries int, backoff time.Duration) TaskOption {
	return func(t Task) {
		t.SetRetryConfig(&RetryConfig{
			MaxRetries: maxRetries,
			Backoff:    backoff,
		})
	}
}

// WithTimeout sets the timeout for a task
func WithTimeout(timeout time.Duration) TaskOption {
	return func(t Task) {
		t.SetTimeout(timeout)
	}
}

// WithTriggerRule sets the trigger rule for a task
func WithTriggerRule(rule TriggerRule) TaskOption {
	return func(t Task) {
		t.SetTriggerRule(rule)
	}
}

// WithRetryConfig sets the retry configuration for a task
func WithRetryConfig(config *RetryConfig) TaskOption {
	return func(t Task) {
		t.SetRetryConfig(config)
	}
}

// WithAdvancedRetries sets advanced retry configuration for a task
func WithAdvancedRetries(config *RetryConfig) TaskOption {
	return func(t Task) {
		t.SetRetryConfig(config)
	}
}

// WithDescription sets the description for a task
func WithDescription(description string) TaskOption {
	return func(t Task) {
		t.SetDescription(description) // Use interface method
	}
}

// WithTags adds tags to a task
func WithTags(tags ...string) TaskOption {
	return func(t Task) {
		t.SetTags(tags...) // Use interface method
	}
}

// WithOwner sets the owner for a task
func WithOwner(owner string) TaskOption {
	return func(t Task) {
		t.SetOwner(owner) // Use interface method
	}
}

// WithCustomField adds a custom field to a task
func WithCustomField(key string, value interface{}) TaskOption {
	return func(t Task) {
		t.SetCustomField(key, value) // Use interface method
	}
}
