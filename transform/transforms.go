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

package transform

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl"
)

// Select creates a transformer that selects only the specified fields
func Select(fields ...string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for _, field := range fields {
			if value, exists := record[field]; exists {
				result[field] = value
			}
		}
		return result, nil
	})
}

// Rename creates a transformer that renames fields
func Rename(mapping map[string]string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for key, value := range record {
			if newKey, exists := mapping[key]; exists {
				result[newKey] = value
			} else {
				result[key] = value
			}
		}
		return result, nil
	})
}

// AddField creates a transformer that adds a new field with a computed value
func AddField(field string, fn func(goetl.Record) interface{}) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}
		result[field] = fn(record)
		return result, nil
	})
}

// ConvertType creates a transformer that converts field types
func ConvertType(field string, targetType reflect.Type) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}

		if value, exists := record[field]; exists {
			converted, err := convertValue(value, targetType)
			if err != nil {
				return nil, fmt.Errorf("failed to convert field %s: %w", field, err)
			}
			result[field] = converted
		}

		return result, nil
	})
}

// ToString creates a transformer that converts a field to string
func ToString(field string) goetl.Transformer {
	return ConvertType(field, reflect.TypeOf(""))
}

// ToInt creates a transformer that converts a field to int
func ToInt(field string) goetl.Transformer {
	return ConvertType(field, reflect.TypeOf(0))
}

// ToFloat creates a transformer that converts a field to float64
func ToFloat(field string) goetl.Transformer {
	return ConvertType(field, reflect.TypeOf(0.0))
}

// TrimSpace creates a transformer that trims whitespace from string fields
func TrimSpace(fields ...string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}

		for _, field := range fields {
			if value, exists := record[field]; exists {
				if str, ok := value.(string); ok {
					result[field] = strings.TrimSpace(str)
				}
			}
		}

		return result, nil
	})
}

// ToUpper creates a transformer that converts string fields to uppercase
func ToUpper(fields ...string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}

		for _, field := range fields {
			if value, exists := record[field]; exists {
				if str, ok := value.(string); ok {
					result[field] = strings.ToUpper(str)
				}
			}
		}

		return result, nil
	})
}

// ToLower creates a transformer that converts string fields to lowercase
func ToLower(fields ...string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}

		for _, field := range fields {
			if value, exists := record[field]; exists {
				if str, ok := value.(string); ok {
					result[field] = strings.ToLower(str)
				}
			}
		}

		return result, nil
	})
}

// ParseTime creates a transformer that parses a string field into a time.Time
func ParseTime(field, layout string) goetl.Transformer {
	return goetl.TransformFunc(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
		result := make(goetl.Record)
		for k, v := range record {
			result[k] = v
		}

		if value, exists := record[field]; exists {
			if str, ok := value.(string); ok {
				parsed, err := time.Parse(layout, str)
				if err != nil {
					return nil, fmt.Errorf("failed to parse time field %s: %w", field, err)
				}
				result[field] = parsed
			}
		}

		return result, nil
	})
}

// convertValue converts a value to the specified type
func convertValue(value interface{}, targetType reflect.Type) (interface{}, error) {
	if value == nil {
		return reflect.Zero(targetType).Interface(), nil
	}

	sourceValue := reflect.ValueOf(value)
	if sourceValue.Type() == targetType {
		return value, nil
	}

	switch targetType.Kind() {
	case reflect.String:
		return fmt.Sprintf("%v", value), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return convertToInt(value)
	case reflect.Float32, reflect.Float64:
		return convertToFloat(value)
	case reflect.Bool:
		return convertToBool(value)
	default:
		return nil, fmt.Errorf("unsupported target type: %s", targetType)
	}
}

func convertToInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case string:
		return strconv.Atoi(strings.TrimSpace(v))
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

func convertToFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseBool(strings.TrimSpace(v))
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}
