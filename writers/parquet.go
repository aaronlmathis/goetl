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
	"fmt"
	"os"
	"reflect"

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
	file         *os.File
	writer       *pqarrow.FileWriter
	schema       *arrow.Schema
	recordCount  int64
	closed       bool
	batchSize    int64
	recordBuffer []goetl.Record
	fieldOrder   []string // Track field order for consistent schema
}

// ParquetWriterOptions configures the Parquet writer
type ParquetWriterOptions struct {
	BatchSize   int64                // Number of records to buffer before writing
	Schema      *arrow.Schema        // Pre-defined schema (optional)
	Compression compress.Compression // Compression algorithm
	FieldOrder  []string             // Explicit field ordering
}

// NewParquetWriter creates a new Parquet writer for a file
func NewParquetWriter(filename string, opts *ParquetWriterOptions) (*ParquetWriter, error) {
	if opts == nil {
		opts = &ParquetWriterOptions{
			BatchSize:   1000,
			Compression: compress.Codecs.Snappy,
		}
	}

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
	}

	return writer, nil
}

// Write implements the DataSink interface
func (p *ParquetWriter) Write(ctx context.Context, record goetl.Record) error {
	if p.closed {
		return fmt.Errorf("parquet writer is closed")
	}

	fmt.Printf("[DEBUG] ParquetWriter: buffering record: %+v\n", record)

	// Initialize schema from first record if not provided
	if p.schema == nil {
		if err := p.initializeSchemaFromRecord(record); err != nil {
			return fmt.Errorf("failed to initialize schema: %w", err)
		}
	}

	// Add to buffer
	p.recordBuffer = append(p.recordBuffer, record)
	p.recordCount++

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

// Close implements the DataSink interface
func (p *ParquetWriter) Close() error {
	if p.closed {
		fmt.Printf("[DEBUG] ParquetWriter: already closed, skipping\n")
		return nil
	}
	p.closed = true

	fmt.Printf("[DEBUG] ParquetWriter: closing with %d records written\n", p.recordCount)

	// Flush any remaining records
	if len(p.recordBuffer) > 0 {
		if err := p.flushBatch(); err != nil {
			fmt.Printf("[DEBUG] ParquetWriter: error flushing final batch: %v\n", err)
		}
	}

	// Close the writer
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			fmt.Printf("[DEBUG] ParquetWriter: error closing writer: %v\n", err)
		}
	}

	// Close the file
	if p.file != nil {
		return p.file.Close()
	}

	return nil
}

// initializeSchemaFromRecord creates an Arrow schema from the first record
func (p *ParquetWriter) initializeSchemaFromRecord(record goetl.Record) error {
	var fields []arrow.Field

	// Use explicit field order if provided, otherwise sort for consistency
	fieldNames := p.fieldOrder
	if fieldNames == nil {
		fieldNames = make([]string, 0, len(record))
		for name := range record {
			fieldNames = append(fieldNames, name)
		}
		// Sort field names for consistent ordering
		for i := 0; i < len(fieldNames)-1; i++ {
			for j := i + 1; j < len(fieldNames); j++ {
				if fieldNames[i] > fieldNames[j] {
					fieldNames[i], fieldNames[j] = fieldNames[j], fieldNames[i]
				}
			}
		}
	}
	p.fieldOrder = fieldNames

	// Create fields based on record values
	for _, name := range fieldNames {
		value, exists := record[name]
		if !exists {
			continue
		}

		var dataType arrow.DataType
		switch reflect.TypeOf(value).Kind() {
		case reflect.Bool:
			dataType = arrow.FixedWidthTypes.Boolean
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			dataType = arrow.PrimitiveTypes.Int32
		case reflect.Int64:
			dataType = arrow.PrimitiveTypes.Int64
		case reflect.Float32:
			dataType = arrow.PrimitiveTypes.Float32
		case reflect.Float64:
			dataType = arrow.PrimitiveTypes.Float64
		case reflect.String:
			dataType = arrow.BinaryTypes.String
		default:
			// Default to string for unknown types
			dataType = arrow.BinaryTypes.String
		}

		field := arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: true,
		}
		fields = append(fields, field)
	}

	schema := arrow.NewSchema(fields, nil)
	p.schema = schema

	// Create the parquet writer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(schema, p.file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet file writer: %w", err)
	}
	p.writer = writer

	fmt.Printf("[DEBUG] ParquetWriter: initialized schema with %d fields\n", len(fields))
	return nil
}

// flushBatch writes the current buffer to the parquet file
func (p *ParquetWriter) flushBatch() error {
	if len(p.recordBuffer) == 0 {
		return nil
	}

	fmt.Printf("[DEBUG] ParquetWriter: flushing batch of %d records\n", len(p.recordBuffer))

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

	// Clear buffer
	p.recordBuffer = p.recordBuffer[:0]
	return nil
}

// createArrowRecord converts a slice of goetl.Record to an Arrow Record
func (p *ParquetWriter) createArrowRecord(records []goetl.Record) (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	builders := make([]array.Builder, len(p.fieldOrder))
	for i := range p.fieldOrder {
		field := p.schema.Field(i)
		builder := array.NewBuilder(mem, field.Type)
		builders[i] = builder
	}

	// Build columns
	for _, record := range records {
		for i, fieldName := range p.fieldOrder {
			value, exists := record[fieldName]
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}

			switch builder := builders[i].(type) {
			case *array.BooleanBuilder:
				if v, ok := value.(bool); ok {
					builder.Append(v)
				} else {
					builder.AppendNull()
				}
			case *array.Int32Builder:
				if v, ok := value.(int); ok {
					builder.Append(int32(v))
				} else if v, ok := value.(int32); ok {
					builder.Append(v)
				} else {
					builder.AppendNull()
				}
			case *array.Int64Builder:
				if v, ok := value.(int64); ok {
					builder.Append(v)
				} else if v, ok := value.(int); ok {
					builder.Append(int64(v))
				} else {
					builder.AppendNull()
				}
			case *array.Float32Builder:
				if v, ok := value.(float32); ok {
					builder.Append(v)
				} else if v, ok := value.(float64); ok {
					builder.Append(float32(v))
				} else {
					builder.AppendNull()
				}
			case *array.Float64Builder:
				if v, ok := value.(float64); ok {
					builder.Append(v)
				} else if v, ok := value.(float32); ok {
					builder.Append(float64(v))
				} else {
					builder.AppendNull()
				}
			case *array.StringBuilder:
				if v, ok := value.(string); ok {
					builder.Append(v)
				} else {
					builder.Append(fmt.Sprintf("%v", value))
				}
			default:
				return nil, fmt.Errorf("unsupported builder type for field %s", fieldName)
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record
	return array.NewRecord(p.schema, arrays, int64(len(records))), nil
}
