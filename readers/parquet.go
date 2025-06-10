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

package readers

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/aaronlmathis/goetl"
)

// ParquetReader implements DataSource for Parquet files
type ParquetReader struct {
	reader          *file.Reader
	arrowReader     *pqarrow.FileReader
	recordReader    pqarrow.RecordReader
	currentBatch    arrow.Record
	currentBatchIdx int
	currentRow      int64
	totalRows       int64
	batchSize       int64
}

// ParquetReaderOptions configures the Parquet reader
type ParquetReaderOptions struct {
	BatchSize int64 // Number of rows to read in each batch
}

// NewParquetReader creates a new Parquet reader
func NewParquetReader(filename string, opts *ParquetReaderOptions) (*ParquetReader, error) {
	if opts == nil {
		opts = &ParquetReaderOptions{
			BatchSize: 1000, // Default batch size
		}
	}

	// Open the parquet file using the correct API
	parquetReader, err := file.OpenParquetFile(filename, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file %s: %w", filename, err)
	}

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Get schema information
	totalRows := parquetReader.NumRows()

	fmt.Printf("[DEBUG] ParquetReader: opened file with %d rows\n", totalRows)

	// Create record reader
	recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	fmt.Printf("[DEBUG] ParquetReader: created record reader successfully\n")

	return &ParquetReader{
		reader:          parquetReader,
		arrowReader:     arrowReader,
		recordReader:    recordReader,
		currentBatch:    nil,
		currentBatchIdx: 0,
		totalRows:       totalRows,
		currentRow:      0,
		batchSize:       opts.BatchSize,
	}, nil
}

// Read implements the DataSource interface
func (p *ParquetReader) Read(ctx context.Context) (goetl.Record, error) {
	fmt.Printf("[DEBUG] ParquetReader: Read() called, currentBatch=%v, currentBatchIdx=%d\n",
		p.currentBatch != nil, p.currentBatchIdx)

	// If we don't have a current batch or we've exhausted it, get the next one
	if p.currentBatch == nil || p.currentBatchIdx >= int(p.currentBatch.NumRows()) {
		if p.currentBatch != nil {
			fmt.Printf("[DEBUG] ParquetReader: releasing previous batch\n")
			p.currentBatch.Release()
			p.currentBatch = nil
		}

		fmt.Printf("[DEBUG] ParquetReader: calling recordReader.Read()\n")
		// Read next record batch
		record, err := p.recordReader.Read()
		if err != nil {
			fmt.Printf("[DEBUG] ParquetReader: recordReader.Read() returned error: %v\n", err)
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read record: %w", err)
		}

		fmt.Printf("[DEBUG] ParquetReader: recordReader.Read() returned batch with %d rows\n", record.NumRows())
		p.currentBatch = record
		p.currentBatchIdx = 0

		fmt.Printf("[DEBUG] ParquetReader: loaded batch with %d rows\n", record.NumRows())
	}

	// Extract current row from the batch
	if p.currentBatch.NumRows() == 0 {
		fmt.Printf("[DEBUG] ParquetReader: batch has 0 rows, returning EOF\n")
		return nil, io.EOF
	}

	// Extract row as a map
	result := p.extractRecordFromBatch(p.currentBatch, p.currentBatchIdx)
	p.currentBatchIdx++
	p.currentRow++

	fmt.Printf("[DEBUG] ParquetReader: read record %d: %+v\n", p.currentRow, result)
	return result, nil
}

// Close implements the DataSource interface
func (p *ParquetReader) Close() error {
	if p.currentBatch != nil {
		p.currentBatch.Release()
		p.currentBatch = nil
	}
	if p.recordReader != nil {
		p.recordReader.Release()
		p.recordReader = nil
	}
	// The file.Reader manages its own resources and doesn't need explicit closing
	return nil
}

// extractRecordFromBatch extracts a single record from an Arrow record at the given position
func (p *ParquetReader) extractRecordFromBatch(record arrow.Record, pos int) goetl.Record {
	result := make(goetl.Record)

	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		field := schema.Field(i)
		column := record.Column(i)

		// Extract value at position
		value := p.extractValueFromColumn(column, pos)
		result[field.Name] = value
	}

	return result
}

// extractValueFromColumn extracts a value from an Arrow column at the given row index
func (p *ParquetReader) extractValueFromColumn(column arrow.Array, rowIndex int) interface{} {
	if column.IsNull(rowIndex) {
		return nil
	}

	switch col := column.(type) {
	case *array.Boolean:
		return col.Value(rowIndex)
	case *array.Int32:
		return int(col.Value(rowIndex))
	case *array.Int64:
		return int(col.Value(rowIndex))
	case *array.Float32:
		return float64(col.Value(rowIndex))
	case *array.Float64:
		return col.Value(rowIndex)
	case *array.String:
		return col.Value(rowIndex)
	case *array.Binary:
		return string(col.Value(rowIndex))
	default:
		// For unknown types, try to convert to string
		return fmt.Sprintf("%v", column.GetOneForMarshal(rowIndex))
	}
}
