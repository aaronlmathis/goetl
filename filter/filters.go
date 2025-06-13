//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of core.
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
// along with core. If not, see https://www.gnu.org/licenses/.

package filter

import (
	"context"
	"reflect"
	"regexp"
	"strings"

	"github.com/aaronlmathis/goetl/core"
)

// Package filter provides reusable, composable record filtering functions for GoETL pipelines.
//
// This package includes field-based, value-based, and custom logic filters for conditional record removal or selection.
// All functions return core.Filter implementations for use in ETL pipelines.

// NotNull creates a filter that excludes records where the specified field is nil or empty
func NotNull(field string) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		if value == nil {
			return false, nil
		}
		if str, ok := value.(string); ok && str == "" {
			return false, nil
		}
		return true, nil
	})
}

// Equals creates a filter that includes records where the field equals the specified value
func Equals(field string, expectedValue interface{}) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		return reflect.DeepEqual(value, expectedValue), nil
	})
}

// Contains creates a filter that includes records where the string field contains the substring
func Contains(field, substring string) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		if str, ok := value.(string); ok {
			return strings.Contains(str, substring), nil
		}
		return false, nil
	})
}

// StartsWith creates a filter that includes records where the string field starts with the prefix
func StartsWith(field, prefix string) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		if str, ok := value.(string); ok {
			return strings.HasPrefix(str, prefix), nil
		}
		return false, nil
	})
}

// EndsWith creates a filter that includes records where the string field ends with the suffix
func EndsWith(field, suffix string) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		if str, ok := value.(string); ok {
			return strings.HasSuffix(str, suffix), nil
		}
		return false, nil
	})
}

// MatchesRegex creates a filter that includes records where the string field matches the regex pattern
func MatchesRegex(field, pattern string) core.Filter {
	regex := regexp.MustCompile(pattern)
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}
		if str, ok := value.(string); ok {
			return regex.MatchString(str), nil
		}
		return false, nil
	})
}

// GreaterThan creates a filter that includes records where the numeric field is greater than the value
func GreaterThan(field string, threshold float64) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]

		if !exists {
			return false, nil
		}

		num, err := convertToFloat64(value)
		if err != nil {
			return false, nil
		}

		return num > threshold, nil
	})
}

// LessThan creates a filter that includes records where the numeric field is less than the value
func LessThan(field string, threshold float64) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}

		num, err := convertToFloat64(value)
		if err != nil {
			return false, nil
		}

		return num < threshold, nil
	})
}

// Between creates a filter that includes records where the numeric field is between min and max (inclusive)
func Between(field string, min, max float64) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}

		num, err := convertToFloat64(value)
		if err != nil {
			return false, nil
		}

		return num >= min && num <= max, nil
	})
}

// In creates a filter that includes records where the field value is in the provided set
func In(field string, values ...interface{}) core.Filter {
	valueSet := make(map[interface{}]bool)
	for _, v := range values {
		valueSet[v] = true
	}

	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		value, exists := record[field]
		if !exists {
			return false, nil
		}

		return valueSet[value], nil
	})
}

// And creates a filter that requires all provided filters to pass
func And(filters ...core.Filter) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		for _, filter := range filters {
			include, err := filter.ShouldInclude(ctx, record)
			if err != nil {
				return false, err
			}
			if !include {
				return false, nil
			}
		}
		return true, nil
	})
}

// Or creates a filter that requires at least one of the provided filters to pass
func Or(filters ...core.Filter) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		for _, filter := range filters {
			include, err := filter.ShouldInclude(ctx, record)
			if err != nil {
				return false, err
			}
			if include {
				return true, nil
			}
		}
		return false, nil
	})
}

// Not creates a filter that negates the provided filter
func Not(filter core.Filter) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		include, err := filter.ShouldInclude(ctx, record)
		if err != nil {
			return false, err
		}
		return !include, nil
	})
}

// Custom creates a filter using a user-provided predicate function
// The predicate function receives a record and returns true if the record should be included
func Custom(predicate func(core.Record) bool) core.Filter {
	return core.FilterFunc(func(ctx context.Context, record core.Record) (bool, error) {
		return predicate(record), nil
	})
}

// CustomWithContext creates a filter using a user-provided predicate function that has access to context
// The predicate function receives context and a record, returns (include bool, error)
func CustomWithContext(predicate func(context.Context, core.Record) (bool, error)) core.Filter {
	return core.FilterFunc(predicate)
}

// convertToFloat64 converts various numeric types to float64
func convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, nil
	}
}
