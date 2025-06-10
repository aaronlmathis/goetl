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

package writers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/aaronlmathis/goetl"
)

// ParquetWriter implements DataSink for Parquet files
type ParquetWriter struct {
	file          *os.File
	writer        *pqarrow.FileWriter
	schema        *arrow.Schema
	recordCount   int64
	closed        bool
	batchSize     int64
	recordBuffer  []goetl.Record
	fieldOrder    []string // Track field order for consistent schema
	stats         WriterStats
	lastGoodState int64 // Track last successful flush
	errorState    bool  // Mark writer as errored
	builders      []array.Builder
	allocator     memory.Allocator
	builderPool   sync.Pool
	opts          *ParquetWriterOptions // Options for the writer
	fieldIndexMap map[string]int        // Cache field name to index mapping
}

// ParquetWriterOptions configures the Parquet writer
type ParquetWriterOptions struct {
	BatchSize       int64                // Number of records to buffer before writing
	Schema          *arrow.Schema        // Pre-defined schema (optional)
	Compression     compress.Compression // Compression algorithm
	FieldOrder      []string             // Explicit field ordering
	RowGroupSize    int64                // New: Control row group size
	PageSize        int64                // New: Control page size
	DictionaryLevel map[string]bool      // New: Per-field dictionary encoding
	Metadata        map[string]string    // New: File metadata
	ValidateSchema  bool                 // New: Enable strict schema validation

}

// WriterStats holds statistics about the Parquet writer's performance
type WriterStats struct {
	RecordsWritten  int64
	BatchesWritten  int64
	BytesWritten    int64
	FlushDuration   time.Duration
	LastFlushTime   time.Time
	ErrorCount      int64
	NullValueCounts map[string]int64
}

// WriterOption represents a configuration function
type WriterOption func(*ParquetWriterOptions)

// Individual option functions with clear intent
func WithBatchSize(size int64) WriterOption {
	return func(opts *ParquetWriterOptions) {
		opts.BatchSize = size
	}
}

func WithCompression(compression compress.Compression) WriterOption {
	return func(opts *ParquetWriterOptions) {
		opts.Compression = compression
	}
}

func WithFieldOrder(fields []string) WriterOption {
	return func(opts *ParquetWriterOptions) {
		// Defensive copy to avoid shared slices
		opts.FieldOrder = make([]string, len(fields))
		copy(opts.FieldOrder, fields)
	}
}

func WithSchemaValidation(validate bool) WriterOption {
	return func(opts *ParquetWriterOptions) {
		opts.ValidateSchema = validate
	}
}

func WithRowGroupSize(size int64) WriterOption {
	return func(opts *ParquetWriterOptions) {
		opts.RowGroupSize = size
	}
}

func WithMetadata(metadata map[string]string) WriterOption {
	return func(opts *ParquetWriterOptions) {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string)
		}
		// Defensive copy
		for k, v := range metadata {
			opts.Metadata[k] = v
		}
	}
}

// NewParquetWriter creates a new Parquet writer for a file
func NewParquetWriter(filename string, options ...WriterOption) (*ParquetWriter, error) {
	// Start with defaults
	opts := (&ParquetWriterOptions{}).withDefaults()

	// Apply all functional options
	for _, option := range options {
		option(opts)
	}

	return createParquetWriter(filename, opts)
}

// Shared creation logic (DRY principle)
func createParquetWriter(filename string, opts *ParquetWriterOptions) (*ParquetWriter, error) {
	// Create the output file
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file %s: %w", filename, err)
	}

	writer := &ParquetWriter{
		file:         file,
		batchSize:    opts.BatchSize,
		schema:       opts.Schema,
		fieldOrder:   opts.FieldOrder,
		recordBuffer: make([]goetl.Record, 0, opts.BatchSize),
		stats:        WriterStats{NullValueCounts: make(map[string]int64)},
		allocator:    memory.NewGoAllocator(),
		opts:         opts,
		builderPool: sync.Pool{
			New: func() interface{} {
				return make([]array.Builder, 0)
			},
		},
	}

	return writer, nil
}

// Write implements the DataSink interface
func (p *ParquetWriter) Write(ctx context.Context, record goetl.Record) error {
	if p.closed {
		return fmt.Errorf("parquet writer is closed")
	}

	if p.errorState {
		return fmt.Errorf("writer is in error state")
	}

	if p.schema == nil {
		if err := p.initializeSchemaFromRecord(record); err != nil {
			p.errorState = true
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	// Validate record if schema validation is enabled
	if p.schema != nil && p.opts.ValidateSchema {
		if err := p.validateRecord(record); err != nil {
			p.errorState = true
			return fmt.Errorf("record validation failed: %w", err)
		}
	}

	// Add to buffer
	p.recordBuffer = append(p.recordBuffer, record)
	p.recordCount++

	// Update stats
	p.stats.RecordsWritten++

	// Write batch if buffer is full
	if int64(len(p.recordBuffer)) >= p.batchSize {
		if err := p.flushBatch(); err != nil {
			return fmt.Errorf("failed to flush batch: %w", err)
		}
	}

	return nil
}

// Flush implements the DataSink interface
func (p *ParquetWriter) Flush() error {
	if len(p.recordBuffer) > 0 {
		return p.flushBatch()
	}
	return nil
}

func (opts *ParquetWriterOptions) withDefaults() *ParquetWriterOptions {
	result := &ParquetWriterOptions{}

	// Copy existing values if opts is not nil
	if opts != nil {
		*result = *opts
	}

	// Apply defaults for zero values only
	if result.BatchSize <= 0 {
		result.BatchSize = 1000
	}
	if result.RowGroupSize <= 0 {
		result.RowGroupSize = 10000
	}
	if result.PageSize <= 0 {
		result.PageSize = 1024 * 1024 // 1MB
	}
	if result.Compression == 0 {
		result.Compression = compress.Codecs.Snappy
	}

	// Initialize maps if nil
	if result.DictionaryLevel == nil {
		result.DictionaryLevel = make(map[string]bool)
	}
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}

	return result
}

// Close implements the DataSink interface
func (p *ParquetWriter) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	// Flush any remaining records
	if len(p.recordBuffer) > 0 {
		if err := p.flushBatch(); err != nil {
			return fmt.Errorf("failed to flush remaining records: %w", err)
		}
	}

	// Release builders first
	for _, builder := range p.builders {
		if builder != nil {
			builder.Release()
		}
	}
	p.builders = nil

	// Close the parquet writer
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
		p.writer = nil
	}

	// Clear file reference
	p.file = nil

	return nil
}

// initializeSchemaFromRecord creates an Arrow schema from the first record
func (p *ParquetWriter) initializeSchemaFromRecord(record goetl.Record) error {
	var fields []arrow.Field

	fieldNames := p.fieldOrder
	if fieldNames == nil {
		fieldNames = make([]string, 0, len(record))
		for name := range record {
			fieldNames = append(fieldNames, name)
		}

		sort.Strings(fieldNames)
		p.fieldOrder = fieldNames
	}

	// Create fields based on record values
	for _, name := range fieldNames {
		value, exists := record[name]

		var dataType arrow.DataType
		var err error

		if exists && value != nil {
			// Field exists in record - infer type from value
			if dataType, err = p.inferArrowType(value); err != nil {
				return fmt.Errorf("failed to infer arrow type for field %s: %w", name, err)
			}
		} else {
			// Field missing or null - default to string type
			dataType = arrow.BinaryTypes.String
		}

		field := arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: true,
		}
		fields = append(fields, field)
	}

	// Build field index map for efficient lookups
	p.fieldIndexMap = make(map[string]int, len(fieldNames))
	for i, name := range fieldNames {
		p.fieldIndexMap[name] = i
	}
	schema := arrow.NewSchema(fields, nil)
	p.schema = schema

	// Create the parquet writer using options
	props := parquet.NewWriterProperties(parquet.WithCompression(p.opts.Compression))
	if p.opts.RowGroupSize > 0 {
		props = parquet.NewWriterProperties(
			parquet.WithCompression(p.opts.Compression),
			parquet.WithMaxRowGroupLength(p.opts.RowGroupSize),
		)
	}

	arrowProps := pqarrow.DefaultWriterProps()
	if p.opts.PageSize > 0 {
		// Configure page size if supported by the library
	}

	writer, err := pqarrow.NewFileWriter(schema, p.file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %w", err)
	}
	p.writer = writer

	// Initialize builders once schema is ready
	p.initializeBuilders()

	return nil
}

func (p *ParquetWriter) inferArrowType(value interface{}) (arrow.DataType, error) {
	if value == nil {
		return arrow.BinaryTypes.String, nil
	}

	switch v := value.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case int8:
		return arrow.PrimitiveTypes.Int8, nil
	case int16:
		return arrow.PrimitiveTypes.Int16, nil
	case int32:
		return arrow.PrimitiveTypes.Int32, nil
	case int64:
		return arrow.PrimitiveTypes.Int64, nil
	case int:
		// Use int64 for consistency unless value fits in int32
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			return arrow.PrimitiveTypes.Int32, nil
		}
		return arrow.PrimitiveTypes.Int64, nil
	case float32:
		return arrow.PrimitiveTypes.Float32, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case time.Time:
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case []byte:
		return arrow.BinaryTypes.Binary, nil
	case json.RawMessage:
		return arrow.BinaryTypes.String, nil
	default:
		// Convert everything else to string for safety
		return arrow.BinaryTypes.String, nil
	}
}

// flushBatch writes the current buffer to the parquet file
func (p *ParquetWriter) flushBatch() error {

	if len(p.recordBuffer) == 0 {
		return nil
	}

	startTime := time.Now()

	// Create checkpoint
	checkpoint := p.recordCount

	defer func() {
		if r := recover(); r != nil {
			p.errorState = true
			p.recordCount = checkpoint // Rollback
		}
	}()

	//fmt.Printf("[DEBUG] ParquetWriter: flushing batch of %d records\n", len(p.recordBuffer))

	// Create Arrow record from buffer
	record, err := p.createArrowRecord(p.recordBuffer)
	if err != nil {
		return fmt.Errorf("failed to create arrow record: %w", err)
	}
	defer record.Release()

	// Write to parquet file
	if err := p.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Calculate duration and update stats
	flushDuration := time.Since(startTime)
	p.updateStats(len(p.recordBuffer), flushDuration)

	// Clear buffer
	p.recordBuffer = p.recordBuffer[:0]

	p.lastGoodState = p.recordCount

	return nil
}

// createArrowRecord converts a slice of goetl.Record to an Arrow Record
func (p *ParquetWriter) createArrowRecord(records []goetl.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to convert")
	}

	p.resetBuilders()

	// Process each record once, handling all fields in order
	for _, record := range records {
		// Process ALL fields in fieldOrder for consistent schema
		for i, fieldName := range p.fieldOrder {
			value, exists := record[fieldName]

			// Track null values immediately when encountered
			if !exists || value == nil {
				p.builders[i].AppendNull()
				if p.stats.NullValueCounts == nil {
					p.stats.NullValueCounts = make(map[string]int64)
				}
				p.stats.NullValueCounts[fieldName]++
				continue
			}

			// Append non-null value
			if err := p.appendValueToBuilder(p.builders[i], value, fieldName); err != nil {
				return nil, fmt.Errorf("failed to append value for field %s: %w", fieldName, err)
			}
		}
	}

	// Build arrays from builders
	arrays := make([]arrow.Array, len(p.builders))
	for i, builder := range p.builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	return array.NewRecord(p.schema, arrays, int64(len(records))), nil
}

func (p *ParquetWriter) appendValueToBuilder(builder array.Builder, value interface{}, fieldName string) error {

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}

	case *array.Int32Builder:
		switch v := value.(type) {
		case int:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				b.Append(int32(v))
			} else {
				return fmt.Errorf("int value %d out of range for int32", v)
			}
		case int32:
			b.Append(v)
		default:
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		default:
			b.AppendNull()
			if p.stats.NullValueCounts == nil {
				p.stats.NullValueCounts = make(map[string]int64)
			}
			p.stats.NullValueCounts[fieldName]++
		}
	default:
		return fmt.Errorf("unsupported builder type for field %s", fieldName)
	}
	return nil
}

// Stats returns the current statistics of the Parquet writer
func (p *ParquetWriter) Stats() WriterStats {
	return p.stats
}

// updateStats updates the statistics for the Parquet writer
func (p *ParquetWriter) updateStats(batchSize int, duration time.Duration) {
	p.stats.BatchesWritten++
	p.stats.FlushDuration += duration
	p.stats.LastFlushTime = time.Now()
}

// Fix initializeBuilders to use fieldIndexMap for efficiency
func (p *ParquetWriter) initializeBuilders() {
	if p.builders == nil {
		p.builders = make([]array.Builder, len(p.fieldOrder))
		for i, fieldName := range p.fieldOrder {
			// Find the field in schema by name, not by index
			var field arrow.Field
			found := false
			for _, f := range p.schema.Fields() {
				if f.Name == fieldName {
					field = f
					found = true
					break
				}
			}
			if !found {
				panic(fmt.Sprintf("field %s not found in schema", fieldName))
			}

			p.builders[i] = array.NewBuilder(p.allocator, field.Type)
		}
	}
}

func (p *ParquetWriter) resetBuilders() {
	for _, builder := range p.builders {
		// More robust reset approach
		if builder != nil {
			arr := builder.NewArray()
			if arr != nil {
				arr.Release()
			}
		}
	}
}

func (p *ParquetWriter) validateRecord(record goetl.Record) error {
	if p.schema == nil {
		return fmt.Errorf("schema not initialized")
	}

	// Check for required fields and type compatibility
	for _, field := range p.schema.Fields() {
		value, exists := record[field.Name]
		if !exists {
			continue // Allow missing fields (will be null)
		}

		if err := p.validateFieldType(field, value); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}
	return nil
}

func (p *ParquetWriter) validateFieldType(field arrow.Field, value interface{}) error {
	switch field.Type.ID() {
	case arrow.BOOL:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case arrow.INT32:
		switch value.(type) {
		case int, int32:
			// Valid
		default:
			return fmt.Errorf("expected int/int32, got %T", value)
		}
	case arrow.INT64:
		switch value.(type) {
		case int, int64:
			// Valid
		default:
			return fmt.Errorf("expected int/int64, got %T", value)
		}
	case arrow.FLOAT32, arrow.FLOAT64:
		switch value.(type) {
		case float32, float64:
			// Valid
		default:
			return fmt.Errorf("expected float32/float64, got %T", value)
		}
	case arrow.STRING:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case arrow.TIMESTAMP:
		if _, ok := value.(time.Time); !ok {
			return fmt.Errorf("expected time.Time, got %T", value)
		}
	}
	return nil
}
