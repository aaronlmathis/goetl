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

// validators.go - Data quality validation and conditional logic implementations
package validators

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/dag/tasks"
)

// DataQualityValidator implements ConditionalLogic for data quality checks
// Provides comprehensive validation including record counts, field presence,
// null value rates, and custom validation functions
type DataQualityValidator struct {
	MinRecords       int                                 // Minimum number of records required
	MaxRecords       int                                 // Maximum number of records allowed (0 = unlimited)
	MaxNullRate      float64                             // Maximum allowed null rate (0.0-1.0)
	RequiredFields   []string                            // Fields that must be present in all records
	ForbiddenFields  []string                            // Fields that must not be present
	FieldValidators  map[string]FieldValidator           // Per-field validation rules
	CustomValidators []func([]core.Record) (bool, error) // Custom validation functions
	OnTrueTasks      []string                            // Tasks to execute when validation passes
	OnFalseTasks     []string                            // Tasks to execute when validation fails
}

// FieldValidator defines validation rules for individual fields
type FieldValidator struct {
	DataType      FieldDataType                   // Expected data type
	Pattern       *regexp.Regexp                  // Regex pattern for string fields
	MinValue      interface{}                     // Minimum value (for numeric fields)
	MaxValue      interface{}                     // Maximum value (for numeric fields)
	AllowedValues []interface{}                   // Whitelist of allowed values
	CustomFunc    func(interface{}) (bool, error) // Custom validation function
}

// FieldDataType represents expected data types for validation
type FieldDataType string

const (
	FieldTypeString FieldDataType = "string"
	FieldTypeInt    FieldDataType = "int"
	FieldTypeFloat  FieldDataType = "float"
	FieldTypeBool   FieldDataType = "bool"
	FieldTypeDate   FieldDataType = "date"
	FieldTypeEmail  FieldDataType = "email"
	FieldTypeURL    FieldDataType = "url"
	FieldTypeUUID   FieldDataType = "uuid"
	FieldTypeAny    FieldDataType = "any"
)

// Evaluate implements ConditionalLogic interface
// Performs comprehensive data quality validation on input records
func (dqv *DataQualityValidator) Evaluate(ctx context.Context, input tasks.TaskInput) (bool, error) {
	records := input.Records
	recordCount := len(records)

	// Check record count constraints
	if recordCount < dqv.MinRecords {
		return false, fmt.Errorf("insufficient records: got %d, need at least %d", recordCount, dqv.MinRecords)
	}

	if dqv.MaxRecords > 0 && recordCount > dqv.MaxRecords {
		return false, fmt.Errorf("too many records: got %d, maximum allowed %d", recordCount, dqv.MaxRecords)
	}

	if recordCount == 0 {
		return true, nil // No records to validate
	}

	// Check required and forbidden fields
	if err := dqv.validateFieldPresence(records); err != nil {
		return false, err
	}

	// Check null value rates
	if err := dqv.validateNullRates(records); err != nil {
		return false, err
	}

	// Validate individual field values
	if err := dqv.validateFieldValues(records); err != nil {
		return false, err
	}

	// Run custom validators
	for i, validator := range dqv.CustomValidators {
		valid, err := validator(records)
		if err != nil {
			return false, fmt.Errorf("custom validator %d failed: %w", i, err)
		}
		if !valid {
			return false, fmt.Errorf("custom validator %d failed validation", i)
		}
	}

	return true, nil
}

// OnTrue implements ConditionalLogic interface
func (dqv *DataQualityValidator) OnTrue() []string {
	return dqv.OnTrueTasks
}

// OnFalse implements ConditionalLogic interface
func (dqv *DataQualityValidator) OnFalse() []string {
	return dqv.OnFalseTasks
}

// validateFieldPresence checks for required and forbidden fields
func (dqv *DataQualityValidator) validateFieldPresence(records []core.Record) error {
	if len(dqv.RequiredFields) == 0 && len(dqv.ForbiddenFields) == 0 {
		return nil
	}

	for recordIdx, record := range records {
		// Check required fields
		for _, field := range dqv.RequiredFields {
			if _, exists := record[field]; !exists {
				return fmt.Errorf("record %d missing required field: %s", recordIdx, field)
			}
		}

		// Check forbidden fields
		for _, field := range dqv.ForbiddenFields {
			if _, exists := record[field]; exists {
				return fmt.Errorf("record %d contains forbidden field: %s", recordIdx, field)
			}
		}
	}

	return nil
}

// validateNullRates checks null value rates across all records
func (dqv *DataQualityValidator) validateNullRates(records []core.Record) error {
	if dqv.MaxNullRate <= 0 {
		return nil // No null rate validation
	}

	// Collect all field names
	fieldNames := make(map[string]bool)
	for _, record := range records {
		for field := range record {
			fieldNames[field] = true
		}
	}

	// Calculate null rates per field
	for field := range fieldNames {
		nullCount := 0
		for _, record := range records {
			if value, exists := record[field]; !exists || value == nil {
				nullCount++
			}
		}

		nullRate := float64(nullCount) / float64(len(records))
		if nullRate > dqv.MaxNullRate {
			return fmt.Errorf("field %s has null rate %.2f, exceeds maximum %.2f",
				field, nullRate, dqv.MaxNullRate)
		}
	}

	return nil
}

// validateFieldValues validates individual field values using field validators
func (dqv *DataQualityValidator) validateFieldValues(records []core.Record) error {
	if len(dqv.FieldValidators) == 0 {
		return nil
	}

	for recordIdx, record := range records {
		for fieldName, validator := range dqv.FieldValidators {
			value, exists := record[fieldName]
			if !exists {
				continue // Field not present, handled by required field validation
			}

			if err := dqv.validateSingleFieldValue(fieldName, value, validator, recordIdx); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateSingleFieldValue validates a single field value against its validator
func (dqv *DataQualityValidator) validateSingleFieldValue(fieldName string, value interface{}, validator FieldValidator, recordIdx int) error {
	if value == nil {
		return nil // Null values handled by null rate validation
	}

	// Type validation
	if !dqv.validateDataType(value, validator.DataType) {
		return fmt.Errorf("record %d field %s has invalid type, expected %s",
			recordIdx, fieldName, validator.DataType)
	}

	// Pattern validation (for strings)
	if validator.Pattern != nil {
		if str, ok := value.(string); ok {
			if !validator.Pattern.MatchString(str) {
				return fmt.Errorf("record %d field %s value '%s' does not match pattern",
					recordIdx, fieldName, str)
			}
		}
	}

	// Range validation
	if err := dqv.validateRange(value, validator.MinValue, validator.MaxValue, fieldName, recordIdx); err != nil {
		return err
	}

	// Allowed values validation
	if len(validator.AllowedValues) > 0 {
		valid := false
		for _, allowedValue := range validator.AllowedValues {
			if value == allowedValue {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("record %d field %s value '%v' not in allowed values",
				recordIdx, fieldName, value)
		}
	}

	// Custom field validation
	if validator.CustomFunc != nil {
		valid, err := validator.CustomFunc(value)
		if err != nil {
			return fmt.Errorf("record %d field %s custom validation failed: %w",
				recordIdx, fieldName, err)
		}
		if !valid {
			return fmt.Errorf("record %d field %s failed custom validation", recordIdx, fieldName)
		}
	}

	return nil
}

// validateDataType checks if a value matches the expected data type
func (dqv *DataQualityValidator) validateDataType(value interface{}, expectedType FieldDataType) bool {
	if expectedType == FieldTypeAny {
		return true
	}

	switch expectedType {
	case FieldTypeString:
		_, ok := value.(string)
		return ok
	case FieldTypeInt:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case FieldTypeFloat:
		switch value.(type) {
		case float32, float64:
			return true
		}
		return false
	case FieldTypeBool:
		_, ok := value.(bool)
		return ok
	case FieldTypeEmail:
		if str, ok := value.(string); ok {
			return strings.Contains(str, "@") && strings.Contains(str, ".")
		}
		return false
	case FieldTypeURL:
		if str, ok := value.(string); ok {
			return strings.HasPrefix(str, "http://") || strings.HasPrefix(str, "https://")
		}
		return false
	default:
		return true // Unknown types pass validation
	}
}

// validateRange validates numeric ranges
func (dqv *DataQualityValidator) validateRange(value, minValue, maxValue interface{}, fieldName string, recordIdx int) error {
	if minValue == nil && maxValue == nil {
		return nil
	}

	// Convert to float64 for comparison
	val, ok := dqv.toFloat64(value)
	if !ok {
		return nil // Not numeric, skip range validation
	}

	if minValue != nil {
		if min, ok := dqv.toFloat64(minValue); ok && val < min {
			return fmt.Errorf("record %d field %s value %v below minimum %v",
				recordIdx, fieldName, value, minValue)
		}
	}

	if maxValue != nil {
		if max, ok := dqv.toFloat64(maxValue); ok && val > max {
			return fmt.Errorf("record %d field %s value %v above maximum %v",
				recordIdx, fieldName, value, maxValue)
		}
	}

	return nil
}

// toFloat64 converts numeric types to float64 for comparison
func (dqv *DataQualityValidator) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// Convenience constructors following functional options pattern

// NewDataQualityValidator creates a basic data quality validator
func NewDataQualityValidator(minRecords int, requiredFields []string) *DataQualityValidator {
	return &DataQualityValidator{
		MinRecords:      minRecords,
		RequiredFields:  requiredFields,
		FieldValidators: make(map[string]FieldValidator),
		OnTrueTasks:     []string{},
		OnFalseTasks:    []string{},
	}
}

// DataQualityOption is a functional option for configuring DataQualityValidator
type DataQualityOption func(*DataQualityValidator)

// WithMaxRecords sets the maximum record count
func WithMaxRecords(max int) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		dqv.MaxRecords = max
	}
}

// WithMaxNullRate sets the maximum null value rate
func WithMaxNullRate(rate float64) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		dqv.MaxNullRate = rate
	}
}

// WithForbiddenFields sets fields that must not be present
func WithForbiddenFields(fields []string) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		dqv.ForbiddenFields = fields
	}
}

// WithFieldValidator adds a field-specific validator
func WithFieldValidator(fieldName string, validator FieldValidator) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		if dqv.FieldValidators == nil {
			dqv.FieldValidators = make(map[string]FieldValidator)
		}
		dqv.FieldValidators[fieldName] = validator
	}
}

// WithCustomValidator adds a custom validation function
func WithCustomValidator(validator func([]core.Record) (bool, error)) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		dqv.CustomValidators = append(dqv.CustomValidators, validator)
	}
}

// WithBranchingTasks sets the tasks to execute based on validation results
func WithBranchingTasks(onTrue, onFalse []string) DataQualityOption {
	return func(dqv *DataQualityValidator) {
		dqv.OnTrueTasks = onTrue
		dqv.OnFalseTasks = onFalse
	}
}

// NewConfigurableDataQualityValidator creates a validator with functional options
func NewConfigurableDataQualityValidator(minRecords int, requiredFields []string, options ...DataQualityOption) *DataQualityValidator {
	dqv := NewDataQualityValidator(minRecords, requiredFields)

	for _, option := range options {
		option(dqv)
	}

	return dqv
}
