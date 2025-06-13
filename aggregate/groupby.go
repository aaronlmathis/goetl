//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Copyright (C) 2025 Aaron Mathis aaron.mathis@gmail.com
//
// This file is part of GoETL
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

// Package aggregate provides grouping and aggregation utilities for GoETL pipelines.
//
// This package includes the GroupBy aggregator, which supports grouping records by one or more fields
// and applying common aggregations such as count, sum, average, min, and max. Aggregators are composable
// and can be extended for custom aggregation logic.

package aggregate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aaronlmathis/goetl/core"
)

// GroupBy implements grouping and aggregation operations for records in a GoETL pipeline.
// It follows the library's performance-focused design with efficient key generation and streaming support.
type GroupBy struct {
	groupFields []string
	aggregators map[string]Aggregator
	keyBuilder  strings.Builder // Reusable key builder for performance
}

// NewGroupBy creates a new GroupBy aggregator for the specified group fields.
// Follows the library's interface-driven design principles.
func NewGroupBy(groupFields ...string) *GroupBy {
	return &GroupBy{
		groupFields: groupFields,
		aggregators: make(map[string]Aggregator),
	}
}

// Count adds a count aggregator following the fluent API pattern.
func (g *GroupBy) Count(outputField string) *GroupBy {
	g.aggregators[outputField] = &CountAggregator{}
	return g
}

// Sum adds a sum aggregator with type-safe numeric conversion.
func (g *GroupBy) Sum(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &SumAggregator{Field: field}
	return g
}

// Avg adds an average aggregator with precision handling.
func (g *GroupBy) Avg(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &AvgAggregator{Field: field}
	return g
}

// Min adds a minimum aggregator with type-safe comparison.
func (g *GroupBy) Min(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &MinAggregator{Field: field}
	return g
}

// Max adds a maximum aggregator with type-safe comparison.
func (g *GroupBy) Max(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &MaxAggregator{Field: field}
	return g
}

// ProcessRecords processes records using the streaming pipeline pattern.
// This integrates better with your pipeline architecture.
func (g *GroupBy) ProcessRecords(ctx context.Context, source core.DataSource) ([]core.Record, error) {
	groupAggregators := make(map[string]map[string]Aggregator)

	// Stream records from source following your pipeline pattern
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		record, err := source.Read(ctx)
		if err != nil {
			if err.Error() == "EOF" || err.Error() == "no more records" {
				break
			}
			return nil, fmt.Errorf("failed to read record: %w", err)
		}

		groupKey := g.buildGroupKey(record)

		// Initialize aggregators for this group if not exists
		if _, exists := groupAggregators[groupKey]; !exists {
			groupAggregators[groupKey] = make(map[string]Aggregator)
			for outputField, aggregator := range g.aggregators {
				groupAggregators[groupKey][outputField] = g.cloneAggregator(aggregator)
			}
		}

		// Add record to each aggregator in this group
		for outputField, aggregator := range groupAggregators[groupKey] {
			if err := aggregator.Add(ctx, record); err != nil {
				return nil, fmt.Errorf("aggregation error for field %s: %w", outputField, err)
			}
		}
	}

	return g.collectResults(groupAggregators)
}

// Process maintains compatibility with channel-based processing.
func (g *GroupBy) Process(ctx context.Context, records <-chan core.Record) ([]core.Record, error) {
	groupAggregators := make(map[string]map[string]Aggregator)

	for record := range records {
		groupKey := g.buildGroupKey(record)

		if _, exists := groupAggregators[groupKey]; !exists {
			groupAggregators[groupKey] = make(map[string]Aggregator)
			for outputField, aggregator := range g.aggregators {
				groupAggregators[groupKey][outputField] = g.cloneAggregator(aggregator)
			}
		}

		for outputField, aggregator := range groupAggregators[groupKey] {
			if err := aggregator.Add(ctx, record); err != nil {
				return nil, fmt.Errorf("aggregation error for field %s: %w", outputField, err)
			}
		}
	}

	return g.collectResults(groupAggregators)
}

// buildGroupKey creates a deterministic key for grouping - performance optimized.
func (g *GroupBy) buildGroupKey(record core.Record) string {
	g.keyBuilder.Reset()

	for i, field := range g.groupFields {
		if i > 0 {
			g.keyBuilder.WriteByte('|') // Delimiter
		}
		if value, exists := record[field]; exists && value != nil {
			g.keyBuilder.WriteString(fmt.Sprintf("%v", value))
		} else {
			g.keyBuilder.WriteString("__NULL__")
		}
	}

	return g.keyBuilder.String()
}

// parseGroupKey reconstructs group field values from key - enhanced implementation.
func (g *GroupBy) parseGroupKey(groupKey string) core.Record {
	result := make(core.Record, len(g.groupFields))
	parts := strings.Split(groupKey, "|")

	for i, field := range g.groupFields {
		if i < len(parts) {
			value := parts[i]
			if value == "__NULL__" {
				result[field] = nil
			} else {
				result[field] = value
			}
		} else {
			result[field] = nil
		}
	}

	return result
}

// collectResults assembles final grouped results with consistent field naming.
func (g *GroupBy) collectResults(groupAggregators map[string]map[string]Aggregator) ([]core.Record, error) {
	results := make([]core.Record, 0, len(groupAggregators))

	// Sort keys for deterministic output following your library's type-safe principles
	keys := make([]string, 0, len(groupAggregators))
	for key := range groupAggregators {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, groupKey := range keys {
		aggregators := groupAggregators[groupKey]
		result := g.parseGroupKey(groupKey)

		// Add aggregated values with clean field naming
		for outputField, aggregator := range aggregators {
			value, err := aggregator.Result()
			if err != nil {
				return nil, fmt.Errorf("failed to get result for field %s: %w", outputField, err)
			}

			// Flatten the result record into the output
			for k, v := range value {
				// Use the output field name directly instead of concatenating
				if k == "count" || k == "sum" || k == "avg" || k == "min" || k == "max" {
					result[outputField] = v
				} else {
					result[outputField+"_"+k] = v
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// Enhanced aggregator cloning with better type safety
func (g *GroupBy) cloneAggregator(aggregator Aggregator) Aggregator {
	switch agg := aggregator.(type) {
	case *CountAggregator:
		return &CountAggregator{}
	case *SumAggregator:
		return &SumAggregator{Field: agg.Field}
	case *AvgAggregator:
		return &AvgAggregator{Field: agg.Field}
	case *MinAggregator:
		return &MinAggregator{Field: agg.Field}
	case *MaxAggregator:
		return &MaxAggregator{Field: agg.Field}
	default:
		// Support for custom aggregators with Clone method
		if cloner, ok := aggregator.(interface{ Clone() Aggregator }); ok {
			return cloner.Clone()
		}
		return aggregator
	}
}

// Enhanced aggregators with improved type conversion following your library's patterns

// CountAggregator with performance optimization
type CountAggregator struct {
	count int64 // Use int64 for consistency with your library
}

func (c *CountAggregator) Add(ctx context.Context, record core.Record) error {
	c.count++
	return nil
}

func (c *CountAggregator) Result() (core.Record, error) {
	return core.Record{"count": c.count}, nil
}

func (c *CountAggregator) Reset() {
	c.count = 0
}

// SumAggregator with enhanced numeric type handling
type SumAggregator struct {
	Field string
	sum   float64
	count int64 // Track count for debugging
}

func (s *SumAggregator) Add(ctx context.Context, record core.Record) error {
	if value, exists := record[s.Field]; exists && value != nil {
		if num, err := convertToFloat64(value); err == nil {
			s.sum += num
			s.count++
		}
	}
	return nil
}

func (s *SumAggregator) Result() (core.Record, error) {
	return core.Record{"sum": s.sum}, nil
}

func (s *SumAggregator) Reset() {
	s.sum = 0
	s.count = 0
}

// AvgAggregator with precision handling
type AvgAggregator struct {
	Field string
	sum   float64
	count int64
}

func (a *AvgAggregator) Add(ctx context.Context, record core.Record) error {
	if value, exists := record[a.Field]; exists && value != nil {
		if num, err := convertToFloat64(value); err == nil {
			a.sum += num
			a.count++
		}
	}
	return nil
}

func (a *AvgAggregator) Result() (core.Record, error) {
	if a.count == 0 {
		return core.Record{"avg": nil}, nil // Return nil instead of 0 for no values
	}
	return core.Record{"avg": a.sum / float64(a.count)}, nil
}

func (a *AvgAggregator) Reset() {
	a.sum = 0
	a.count = 0
}

// MinAggregator with enhanced type-safe comparison
type MinAggregator struct {
	Field string
	min   interface{}
	set   bool
}

func (m *MinAggregator) Add(ctx context.Context, record core.Record) error {
	if value, exists := record[m.Field]; exists && value != nil {
		if !m.set || compareValues(value, m.min) < 0 {
			m.min = value
			m.set = true
		}
	}
	return nil
}

func (m *MinAggregator) Result() (core.Record, error) {
	return core.Record{"min": m.min}, nil
}

func (m *MinAggregator) Reset() {
	m.min = nil
	m.set = false
}

// MaxAggregator with enhanced type-safe comparison
type MaxAggregator struct {
	Field string
	max   interface{}
	set   bool
}

func (m *MaxAggregator) Add(ctx context.Context, record core.Record) error {
	if value, exists := record[m.Field]; exists && value != nil {
		if !m.set || compareValues(value, m.max) > 0 {
			m.max = value
			m.set = true
		}
	}
	return nil
}

func (m *MaxAggregator) Result() (core.Record, error) {
	return core.Record{"max": m.max}, nil
}

func (m *MaxAggregator) Reset() {
	m.max = nil
	m.set = false
}

// Enhanced type conversion following your transform package patterns
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
	case string:
		// Support string conversion like your transform package
		if f, err := fmt.Sscanf(v, "%f"); err == nil && f == 1 {
			var result float64
			fmt.Sscanf(v, "%f", &result)
			return result, nil
		}
		return 0, fmt.Errorf("cannot convert string %q to float64", v)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// Enhanced comparison with better type handling
func compareValues(a, b interface{}) int {
	// Try to convert both to float64 for numeric comparison
	if aFloat, aErr := convertToFloat64(a); aErr == nil {
		if bFloat, bErr := convertToFloat64(b); bErr == nil {
			if aFloat < bFloat {
				return -1
			} else if aFloat > bFloat {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}
