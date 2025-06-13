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

// join.go - JoinTask implementation
package tasks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// JoinConfig defines join operation parameters
type JoinConfig struct {
	JoinType    string            // "inner", "left", "right", "full"
	LeftKeys    []string          // Join keys for left dataset
	RightKeys   []string          // Join keys for right dataset
	FieldPrefix map[string]string // Prefixes to avoid field name conflicts
	Strategy    string            // "hash", "sort", "broadcast" for optimization
}

// JoinTask performs SQL-style joins between multiple data sources
type JoinTask struct {
	id           string
	joinConfig   JoinConfig
	dependencies []string
	metadata     TaskMetadata
}

func (jt *JoinTask) ID() string             { return jt.id }
func (jt *JoinTask) Dependencies() []string { return jt.dependencies }
func (jt *JoinTask) Metadata() TaskMetadata { return jt.metadata }

func (jt *JoinTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()

	if len(jt.dependencies) < 2 {
		return TaskOutput{}, fmt.Errorf("join task requires at least 2 dependencies, got %d", len(jt.dependencies))
	}

	// Get records from each dependency using source tracking
	leftRecords := input.SourceMap[jt.dependencies[0]]
	rightRecords := input.SourceMap[jt.dependencies[1]]

	if leftRecords == nil || rightRecords == nil {
		return TaskOutput{}, fmt.Errorf("missing source data for join operation")
	}

	// Perform join operation based on configuration
	joinedRecords, err := jt.performJoin(ctx, leftRecords, rightRecords)
	if err != nil {
		return TaskOutput{}, fmt.Errorf("join operation failed: %w", err)
	}

	return TaskOutput{
		Records: joinedRecords,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  int64(len(leftRecords) + len(rightRecords)),
			RecordsOut: int64(len(joinedRecords)),
			Success:    true,
		},
	}, nil
}
func (jt *JoinTask) SetDescription(description string) {
	jt.metadata.Description = description
}

func (jt *JoinTask) SetTags(tags ...string) {
	jt.metadata.Tags = append(jt.metadata.Tags, tags...)
}

func (jt *JoinTask) SetOwner(owner string) {
	jt.metadata.Owner = owner
}

func (jt *JoinTask) SetCustomField(key string, value interface{}) {
	if jt.metadata.CustomFields == nil {
		jt.metadata.CustomFields = make(map[string]interface{})
	}
	jt.metadata.CustomFields[key] = value
}

// performJoin implements the actual join logic using hash join strategy
func (jt *JoinTask) performJoin(ctx context.Context, leftRecords, rightRecords []core.Record) ([]core.Record, error) {
	// Build hash map for right side (smaller dataset typically)
	rightIndex := make(map[string][]core.Record)

	for _, rightRecord := range rightRecords {
		// Create composite key from right keys
		key, err := jt.buildJoinKey(rightRecord, jt.joinConfig.RightKeys)
		if err != nil {
			continue // Skip records with invalid keys
		}
		rightIndex[key] = append(rightIndex[key], rightRecord)
	}

	var result []core.Record

	// Process left side records
	for _, leftRecord := range leftRecords {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		leftKey, err := jt.buildJoinKey(leftRecord, jt.joinConfig.LeftKeys)
		if err != nil {
			if jt.joinConfig.JoinType == "left" || jt.joinConfig.JoinType == "full" {
				// Include left record with null right side
				result = append(result, jt.mergeRecords(leftRecord, nil))
			}
			continue
		}

		// Find matching right records
		if rightMatches, exists := rightIndex[leftKey]; exists {
			// Inner/Left join: merge with each matching right record
			for _, rightRecord := range rightMatches {
				merged := jt.mergeRecords(leftRecord, rightRecord)
				result = append(result, merged)
			}

			// Mark right records as used for full join
			if jt.joinConfig.JoinType == "full" {
				delete(rightIndex, leftKey)
			}
		} else {
			// No match found
			if jt.joinConfig.JoinType == "left" || jt.joinConfig.JoinType == "full" {
				result = append(result, jt.mergeRecords(leftRecord, nil))
			}
		}
	}

	// Handle remaining right records for full/right joins
	if jt.joinConfig.JoinType == "right" || jt.joinConfig.JoinType == "full" {
		for _, rightMatches := range rightIndex {
			for _, rightRecord := range rightMatches {
				result = append(result, jt.mergeRecords(nil, rightRecord))
			}
		}
	}

	return result, nil
}

// buildJoinKey creates a composite key from specified fields
func (jt *JoinTask) buildJoinKey(record core.Record, keyFields []string) (string, error) {
	if record == nil {
		return "", fmt.Errorf("cannot build key from nil record")
	}

	var keyParts []string
	for _, field := range keyFields {
		value, exists := record[field]
		if !exists || value == nil {
			return "", fmt.Errorf("missing join key field: %s", field)
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}

	return strings.Join(keyParts, "|"), nil
}

// mergeRecords combines left and right records with optional field prefixes
func (jt *JoinTask) mergeRecords(leftRecord, rightRecord core.Record) core.Record {
	result := make(core.Record)

	// Add left record fields
	if leftRecord != nil {
		leftPrefix := jt.joinConfig.FieldPrefix["left"]
		for key, value := range leftRecord {
			if leftPrefix != "" {
				result[leftPrefix+key] = value
			} else {
				result[key] = value
			}
		}
	}

	// Add right record fields
	if rightRecord != nil {
		rightPrefix := jt.joinConfig.FieldPrefix["right"]
		for key, value := range rightRecord {
			finalKey := key
			if rightPrefix != "" {
				finalKey = rightPrefix + key
			}

			// Handle field name conflicts
			if _, exists := result[finalKey]; exists && rightPrefix == "" {
				finalKey = "right_" + key
			}

			result[finalKey] = value
		}
	}

	return result
}

// NewJoinTask creates a new JoinTask
func NewJoinTask(id string, config JoinConfig, dependencies []string, options ...TaskOption) *JoinTask {
	task := &JoinTask{
		id:           id,
		joinConfig:   config,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeJoin,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}

func (jt *JoinTask) SetRetryConfig(config *RetryConfig) { jt.metadata.RetryConfig = config }
func (jt *JoinTask) SetTimeout(timeout time.Duration)   { jt.metadata.Timeout = timeout }
func (jt *JoinTask) SetTriggerRule(rule TriggerRule)    { jt.metadata.TriggerRule = rule }
