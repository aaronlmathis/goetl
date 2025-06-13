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

// cdc.go - Change Data Capture (CDC) task implementation
package tasks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// CDCConfig defines change data capture parameters
type CDCConfig struct {
	KeyFields      []string // Fields that identify a unique record
	CompareFields  []string // Fields to compare for changes
	ChangeTypes    []string // Types of changes to detect: INSERT, UPDATE, DELETE
	TimestampField string   // Field to use for change ordering
}

// CDCTask detects changes between two datasets
type CDCTask struct {
	id           string
	cdcConfig    CDCConfig
	dependencies []string
	metadata     TaskMetadata
}

func (ct *CDCTask) ID() string             { return ct.id }
func (ct *CDCTask) Dependencies() []string { return ct.dependencies }
func (ct *CDCTask) Metadata() TaskMetadata { return ct.metadata }

func (ct *CDCTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()

	if len(ct.dependencies) < 2 {
		return TaskOutput{}, fmt.Errorf("CDC task requires 2 dependencies (current and previous), got %d", len(ct.dependencies))
	}

	// Get current and previous datasets
	currentRecords := input.SourceMap[ct.dependencies[0]]  // Current state
	previousRecords := input.SourceMap[ct.dependencies[1]] // Previous state

	// Detect changes
	changes, err := ct.detectChanges(ctx, currentRecords, previousRecords)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("change detection failed: %w", err)
	}

	return TaskOutput{
		Records: changes,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  int64(len(currentRecords) + len(previousRecords)),
			RecordsOut: int64(len(changes)),
			Success:    true,
		},
	}, nil
}

func (ct *CDCTask) SetDescription(description string) {
	ct.metadata.Description = description
}

func (ct *CDCTask) SetTags(tags ...string) {
	ct.metadata.Tags = append(ct.metadata.Tags, tags...)
}

func (ct *CDCTask) SetOwner(owner string) {
	ct.metadata.Owner = owner
}

func (ct *CDCTask) SetCustomField(key string, value interface{}) {
	if ct.metadata.CustomFields == nil {
		ct.metadata.CustomFields = make(map[string]interface{})
	}
	ct.metadata.CustomFields[key] = value
}

// detectChanges implements CDC logic
func (ct *CDCTask) detectChanges(ctx context.Context, current, previous []core.Record) ([]core.Record, error) {
	// Build index of previous records by key
	previousIndex := make(map[string]core.Record)
	for _, record := range previous {
		key, err := ct.buildRecordKey(record)
		if err != nil {
			continue // Skip invalid records
		}
		previousIndex[key] = record
	}

	var changes []core.Record
	processedKeys := make(map[string]bool)

	// Process current records to find INSERTs and UPDATEs
	for _, currentRecord := range current {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key, err := ct.buildRecordKey(currentRecord)
		if err != nil {
			continue
		}

		processedKeys[key] = true

		if previousRecord, exists := previousIndex[key]; exists {
			// Record exists in both - check for changes
			if ct.hasChanges(currentRecord, previousRecord) {
				changeRecord := ct.createChangeRecord(currentRecord, "UPDATE")
				changes = append(changes, changeRecord)
			}
		} else {
			// New record - INSERT
			changeRecord := ct.createChangeRecord(currentRecord, "INSERT")
			changes = append(changes, changeRecord)
		}
	}

	// Process previous records to find DELETEs
	for _, previousRecord := range previous {
		key, err := ct.buildRecordKey(previousRecord)
		if err != nil {
			continue
		}

		if !processedKeys[key] {
			// Record exists in previous but not current - DELETE
			changeRecord := ct.createChangeRecord(previousRecord, "DELETE")
			changes = append(changes, changeRecord)
		}
	}

	return changes, nil
}

// buildRecordKey creates a unique key for a record
func (ct *CDCTask) buildRecordKey(record core.Record) (string, error) {
	var keyParts []string
	for _, field := range ct.cdcConfig.KeyFields {
		value, exists := record[field]
		if !exists || value == nil {
			return "", fmt.Errorf("missing key field: %s", field)
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyParts, "|"), nil
}

// hasChanges compares records for differences
func (ct *CDCTask) hasChanges(current, previous core.Record) bool {
	for _, field := range ct.cdcConfig.CompareFields {
		currentVal := current[field]
		previousVal := previous[field]

		if fmt.Sprintf("%v", currentVal) != fmt.Sprintf("%v", previousVal) {
			return true
		}
	}
	return false
}

// createChangeRecord creates a change record with metadata
func (ct *CDCTask) createChangeRecord(record core.Record, changeType string) core.Record {
	result := make(core.Record)

	// Copy original record
	for k, v := range record {
		result[k] = v
	}

	// Add change metadata
	result["change_type"] = changeType
	result["change_timestamp"] = time.Now().Format(time.RFC3339)

	return result
}

// NewCDCTask creates a new CDCTask
func NewCDCTask(id string, config CDCConfig, dependencies []string, options ...TaskOption) *CDCTask {
	task := &CDCTask{
		id:           id,
		cdcConfig:    config,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeCDC,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}

func (ct *CDCTask) SetRetryConfig(config *RetryConfig) { ct.metadata.RetryConfig = config }
func (ct *CDCTask) SetTimeout(timeout time.Duration)   { ct.metadata.Timeout = timeout }
func (ct *CDCTask) SetTriggerRule(rule TriggerRule)    { ct.metadata.TriggerRule = rule }
