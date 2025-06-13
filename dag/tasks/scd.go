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

// scd.go - Slowly Changing Dimensions (SCD) task implementation
package tasks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/core"
)

// SCDConfig defines slowly changing dimension parameters
type SCDConfig struct {
	Type               string   // "SCD1", "SCD2", "SCD3"
	KeyFields          []string // Business key fields
	TrackingFields     []string // Fields to track for changes
	EffectiveFromField string   // Start date field for SCD2
	EffectiveToField   string   // End date field for SCD2
	CurrentFlag        string   // Current record flag for SCD2
	VersionField       string   // Version number field for SCD3
}

// SCDTask implements slowly changing dimension processing
type SCDTask struct {
	id           string
	scdConfig    SCDConfig
	dependencies []string
	metadata     TaskMetadata
}

func (st *SCDTask) ID() string             { return st.id }
func (st *SCDTask) Dependencies() []string { return st.dependencies }
func (st *SCDTask) Metadata() TaskMetadata { return st.metadata }

func (st *SCDTask) Execute(ctx context.Context, input TaskInput) (TaskOutput, error) {
	start := time.Now()

	if len(st.dependencies) < 2 {
		return TaskOutput{}, fmt.Errorf("SCD task requires 2 dependencies (source and dimension), got %d", len(st.dependencies))
	}

	sourceRecords := input.SourceMap[st.dependencies[0]]    // New source data
	dimensionRecords := input.SourceMap[st.dependencies[1]] // Existing dimension

	var result []core.Record
	var err error

	switch st.scdConfig.Type {
	case "SCD1":
		result, err = st.processSCD1(ctx, sourceRecords, dimensionRecords)
	case "SCD2":
		result, err = st.processSCD2(ctx, sourceRecords, dimensionRecords)
	case "SCD3":
		result, err = st.processSCD3(ctx, sourceRecords, dimensionRecords)
	default:
		return TaskOutput{}, fmt.Errorf("unsupported SCD type: %s", st.scdConfig.Type)
	}

	if err != nil {
		return TaskOutput{}, fmt.Errorf("SCD processing failed: %w", err)
	}

	return TaskOutput{
		Records: result,
		Context: input.Context,
		Metadata: TaskResultMetadata{
			StartTime:  start,
			EndTime:    time.Now(),
			RecordsIn:  int64(len(sourceRecords) + len(dimensionRecords)),
			RecordsOut: int64(len(result)),
			Success:    true,
		},
	}, nil
}

// processSCD2 implements Type 2 slowly changing dimensions
func (st *SCDTask) processSCD2(ctx context.Context, source, dimension []core.Record) ([]core.Record, error) {
	// Build index of existing dimension records
	dimensionIndex := make(map[string]core.Record)
	for _, record := range dimension {
		key, err := st.buildBusinessKey(record)
		if err != nil {
			continue
		}

		// Only index current records
		if currentFlag, exists := record[st.scdConfig.CurrentFlag]; exists {
			if current, ok := currentFlag.(bool); ok && current {
				dimensionIndex[key] = record
			}
		}
	}

	var result []core.Record
	now := time.Now().Format("2006-01-02")

	// Process source records
	for _, sourceRecord := range source {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key, err := st.buildBusinessKey(sourceRecord)
		if err != nil {
			continue
		}

		if existingRecord, exists := dimensionIndex[key]; exists {
			// Check if tracking fields have changed
			if st.hasTrackingChanges(sourceRecord, existingRecord) {
				// Close existing record
				closedRecord := st.copyRecord(existingRecord)
				closedRecord[st.scdConfig.EffectiveToField] = now
				closedRecord[st.scdConfig.CurrentFlag] = false
				result = append(result, closedRecord)

				// Create new current record
				newRecord := st.copyRecord(sourceRecord)
				newRecord[st.scdConfig.EffectiveFromField] = now
				newRecord[st.scdConfig.EffectiveToField] = "9999-12-31"
				newRecord[st.scdConfig.CurrentFlag] = true
				result = append(result, newRecord)
			} else {
				// No changes - keep existing record
				result = append(result, existingRecord)
			}
		} else {
			// New record
			newRecord := st.copyRecord(sourceRecord)
			newRecord[st.scdConfig.EffectiveFromField] = now
			newRecord[st.scdConfig.EffectiveToField] = "9999-12-31"
			newRecord[st.scdConfig.CurrentFlag] = true
			result = append(result, newRecord)
		}
	}

	return result, nil
}

func (ct *SCDTask) SetDescription(description string) {
	ct.metadata.Description = description
}

func (ct *SCDTask) SetTags(tags ...string) {
	ct.metadata.Tags = append(ct.metadata.Tags, tags...)
}

func (ct *SCDTask) SetOwner(owner string) {
	ct.metadata.Owner = owner
}

func (ct *SCDTask) SetCustomField(key string, value interface{}) {
	if ct.metadata.CustomFields == nil {
		ct.metadata.CustomFields = make(map[string]interface{})
	}
	ct.metadata.CustomFields[key] = value
}

// processSCD1 implements Type 1 slowly changing dimensions (simple overwrite)
func (st *SCDTask) processSCD1(ctx context.Context, source, dimension []core.Record) ([]core.Record, error) {
	// For SCD1, source records simply overwrite dimension records
	return source, nil
}

// processSCD3 implements Type 3 slowly changing dimensions (add columns for changes)
func (st *SCDTask) processSCD3(ctx context.Context, source, dimension []core.Record) ([]core.Record, error) {
	// Build index of existing dimension records
	dimensionIndex := make(map[string]core.Record)
	for _, record := range dimension {
		key, err := st.buildBusinessKey(record)
		if err != nil {
			continue
		}
		dimensionIndex[key] = record
	}

	var result []core.Record

	for _, sourceRecord := range source {
		key, err := st.buildBusinessKey(sourceRecord)
		if err != nil {
			continue
		}

		if existingRecord, exists := dimensionIndex[key]; exists {
			// Merge with version tracking
			mergedRecord := st.copyRecord(sourceRecord)

			// Add previous values as "previous_" fields
			for _, field := range st.scdConfig.TrackingFields {
				if existingVal, exists := existingRecord[field]; exists {
					mergedRecord["previous_"+field] = existingVal
				}
			}

			// Increment version
			version := 1
			if existingVersion, exists := existingRecord[st.scdConfig.VersionField]; exists {
				if v, ok := existingVersion.(int); ok {
					version = v + 1
				}
			}
			mergedRecord[st.scdConfig.VersionField] = version

			result = append(result, mergedRecord)
		} else {
			// New record
			newRecord := st.copyRecord(sourceRecord)
			newRecord[st.scdConfig.VersionField] = 1
			result = append(result, newRecord)
		}
	}

	return result, nil
}

// NewSCDTask creates a new SCDTask
func NewSCDTask(id string, config SCDConfig, dependencies []string, options ...TaskOption) *SCDTask {
	task := &SCDTask{
		id:           id,
		scdConfig:    config,
		dependencies: dependencies,
		metadata: TaskMetadata{
			Name:     id,
			TaskType: TaskTypeSCD,
		},
	}

	for _, opt := range options {
		opt(task)
	}

	return task
}

// Helper methods
func (st *SCDTask) buildBusinessKey(record core.Record) (string, error) {
	var keyParts []string
	for _, field := range st.scdConfig.KeyFields {
		value, exists := record[field]
		if !exists || value == nil {
			return "", fmt.Errorf("missing key field: %s", field)
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", value))
	}
	return strings.Join(keyParts, "|"), nil
}

func (st *SCDTask) hasTrackingChanges(source, existing core.Record) bool {
	for _, field := range st.scdConfig.TrackingFields {
		sourceVal := source[field]
		existingVal := existing[field]

		if fmt.Sprintf("%v", sourceVal) != fmt.Sprintf("%v", existingVal) {
			return true
		}
	}
	return false
}

func (st *SCDTask) copyRecord(record core.Record) core.Record {
	result := make(core.Record, len(record))
	for k, v := range record {
		result[k] = v
	}
	return result
}

func (st *SCDTask) SetRetryConfig(config *RetryConfig) { st.metadata.RetryConfig = config }
func (st *SCDTask) SetTimeout(timeout time.Duration)   { st.metadata.Timeout = timeout }
func (st *SCDTask) SetTriggerRule(rule TriggerRule)    { st.metadata.TriggerRule = rule }
