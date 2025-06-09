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
	"encoding/csv"
	"io"
	"strconv"

	"goetl"
)

// CSVReader implements DataSource for CSV files
type CSVReader struct {
	reader  *csv.Reader
	headers []string
	closer  io.Closer
}

// CSVReaderOptions configures the CSV reader
type CSVReaderOptions struct {
	Comma            rune
	Comment          rune
	FieldsPerRecord  int
	LazyQuotes       bool
	TrimLeadingSpace bool
	HasHeaders       bool
}

// NewCSVReader creates a new CSV reader
func NewCSVReader(r io.ReadCloser, opts *CSVReaderOptions) (*CSVReader, error) {
	if opts == nil {
		opts = &CSVReaderOptions{
			Comma:      ',',
			HasHeaders: true,
		}
	}

	csvReader := csv.NewReader(r)
	csvReader.Comma = opts.Comma
	csvReader.Comment = opts.Comment
	csvReader.FieldsPerRecord = opts.FieldsPerRecord
	csvReader.LazyQuotes = opts.LazyQuotes
	csvReader.TrimLeadingSpace = opts.TrimLeadingSpace

	reader := &CSVReader{
		reader: csvReader,
		closer: r,
	}

	// Read headers if specified
	if opts.HasHeaders {
		headers, err := csvReader.Read()
		if err != nil {
			return nil, err
		}
		reader.headers = headers
	}

	return reader, nil
}

// Read implements the DataSource interface
func (c *CSVReader) Read(ctx context.Context) (goetl.Record, error) {
	record, err := c.reader.Read()
	if err != nil {
		return nil, err
	}

	result := make(goetl.Record)

	// Use headers if available, otherwise use column indices
	if len(c.headers) > 0 {
		for i, value := range record {
			if i < len(c.headers) {
				result[c.headers[i]] = c.parseValue(value)
			} else {
				result["col_"+strconv.Itoa(i)] = c.parseValue(value)
			}
		}
	} else {
		for i, value := range record {
			result["col_"+strconv.Itoa(i)] = c.parseValue(value)
		}
	}

	return result, nil
}

// Close implements the DataSource interface
func (c *CSVReader) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// parseValue attempts to parse the string value into appropriate Go types
func (c *CSVReader) parseValue(value string) interface{} {
	// Try to parse as int
	if intVal, err := strconv.Atoi(value); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// Try to parse as bool
	if boolVal, err := strconv.ParseBool(value); err == nil {
		return boolVal
	}

	// Return as string if all else fails
	return value
}
