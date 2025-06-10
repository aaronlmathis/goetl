package aggregate

import (
	"context"
	"fmt"

	"github.com/aaronlmathis/goetl"
)

// GroupBy implements grouping and aggregation operations
type GroupBy struct {
	groupFields []string
	aggregators map[string]goetl.Aggregator
	groups      map[string]goetl.Record
}

// NewGroupBy creates a new GroupBy aggregator
func NewGroupBy(groupFields ...string) *GroupBy {
	return &GroupBy{
		groupFields: groupFields,
		aggregators: make(map[string]goetl.Aggregator),
		groups:      make(map[string]goetl.Record),
	}
}

// Count adds a count aggregator for the specified output field
func (g *GroupBy) Count(outputField string) *GroupBy {
	g.aggregators[outputField] = &CountAggregator{}
	return g
}

// Sum adds a sum aggregator for the specified field
func (g *GroupBy) Sum(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &SumAggregator{Field: field}
	return g
}

// Avg adds an average aggregator for the specified field
func (g *GroupBy) Avg(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &AvgAggregator{Field: field}
	return g
}

// Min adds a minimum aggregator for the specified field
func (g *GroupBy) Min(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &MinAggregator{Field: field}
	return g
}

// Max adds a maximum aggregator for the specified field
func (g *GroupBy) Max(field, outputField string) *GroupBy {
	g.aggregators[outputField] = &MaxAggregator{Field: field}
	return g
}

// Process aggregates records and returns the results
func (g *GroupBy) Process(ctx context.Context, records <-chan goetl.Record) ([]goetl.Record, error) {
	groupAggregators := make(map[string]map[string]goetl.Aggregator)

	// Process all records
	for record := range records {
		groupKey := g.buildGroupKey(record)

		// Initialize aggregators for this group if not exists
		if _, exists := groupAggregators[groupKey]; !exists {
			groupAggregators[groupKey] = make(map[string]goetl.Aggregator)
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

	// Collect results
	var results []goetl.Record
	for groupKey, aggregators := range groupAggregators {
		result := g.parseGroupKey(groupKey)

		// Add aggregated values
		for outputField, aggregator := range aggregators {
			value, err := aggregator.Result()
			if err != nil {
				return nil, fmt.Errorf("failed to get result for field %s: %w", outputField, err)
			}
			// Extract the actual value from the record
			for k, v := range value {
				result[outputField+"_"+k] = v
			}
		}

		results = append(results, result)
	}

	return results, nil
}

func (g *GroupBy) buildGroupKey(record goetl.Record) string {
	var keyParts []string
	for _, field := range g.groupFields {
		if value, exists := record[field]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", value))
		} else {
			keyParts = append(keyParts, "")
		}
	}
	return fmt.Sprintf("%v", keyParts)
}

func (g *GroupBy) parseGroupKey(groupKey string) goetl.Record {
	result := make(goetl.Record)
	// This is a simplified implementation
	// In a production version, you'd want proper key encoding/decoding
	for i, field := range g.groupFields {
		result[field] = fmt.Sprintf("group_%d", i) // Placeholder
	}
	return result
}

func (g *GroupBy) cloneAggregator(aggregator goetl.Aggregator) goetl.Aggregator {
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
		// For custom aggregators, assume they implement a Clone method
		if cloner, ok := aggregator.(interface{ Clone() goetl.Aggregator }); ok {
			return cloner.Clone()
		}
		return aggregator // Fallback, might not work correctly
	}
}

// CountAggregator counts the number of records
type CountAggregator struct {
	count int
}

func (c *CountAggregator) Add(ctx context.Context, record goetl.Record) error {
	c.count++
	return nil
}

func (c *CountAggregator) Result() (goetl.Record, error) {
	return goetl.Record{"count": c.count}, nil
}

func (c *CountAggregator) Reset() {
	c.count = 0
}

// SumAggregator sums numeric values
type SumAggregator struct {
	Field string
	sum   float64
}

func (s *SumAggregator) Add(ctx context.Context, record goetl.Record) error {
	if value, exists := record[s.Field]; exists {
		if num, err := convertToFloat64(value); err == nil {
			s.sum += num
		}
	}
	return nil
}

func (s *SumAggregator) Result() (goetl.Record, error) {
	return goetl.Record{"sum": s.sum}, nil
}

func (s *SumAggregator) Reset() {
	s.sum = 0
}

// AvgAggregator calculates average of numeric values
type AvgAggregator struct {
	Field string
	sum   float64
	count int
}

func (a *AvgAggregator) Add(ctx context.Context, record goetl.Record) error {
	if value, exists := record[a.Field]; exists {
		if num, err := convertToFloat64(value); err == nil {
			a.sum += num
			a.count++
		}
	}
	return nil
}

func (a *AvgAggregator) Result() (goetl.Record, error) {
	if a.count == 0 {
		return goetl.Record{"avg": 0}, nil
	}
	return goetl.Record{"avg": a.sum / float64(a.count)}, nil
}

func (a *AvgAggregator) Reset() {
	a.sum = 0
	a.count = 0
}

// MinAggregator finds minimum value
type MinAggregator struct {
	Field string
	min   interface{}
	set   bool
}

func (m *MinAggregator) Add(ctx context.Context, record goetl.Record) error {
	if value, exists := record[m.Field]; exists {
		if !m.set || compareValues(value, m.min) < 0 {
			m.min = value
			m.set = true
		}
	}
	return nil
}

func (m *MinAggregator) Result() (goetl.Record, error) {
	return goetl.Record{"min": m.min}, nil
}

func (m *MinAggregator) Reset() {
	m.min = nil
	m.set = false
}

// MaxAggregator finds maximum value
type MaxAggregator struct {
	Field string
	max   interface{}
	set   bool
}

func (m *MaxAggregator) Add(ctx context.Context, record goetl.Record) error {
	if value, exists := record[m.Field]; exists {
		if !m.set || compareValues(value, m.max) > 0 {
			m.max = value
			m.set = true
		}
	}
	return nil
}

func (m *MaxAggregator) Result() (goetl.Record, error) {
	return goetl.Record{"max": m.max}, nil
}

func (m *MaxAggregator) Reset() {
	m.max = nil
	m.set = false
}

// Helper functions
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
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func compareValues(a, b interface{}) int {
	// Simple comparison for basic types
	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}
