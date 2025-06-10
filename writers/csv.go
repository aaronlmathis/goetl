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
	"encoding/csv"
	"fmt"
	"io"
	"sort"

	"github.com/aaronlmathis/goetl"
)

// CSVWriter implements DataSink for CSV files
type CSVWriter struct {
	writer  *csv.Writer
	closer  io.Closer
	headers []string
	wrote   bool
}

// CSVWriterOptions configures the CSV writer
type CSVWriterOptions struct {
	Comma       rune
	UseCRLF     bool
	WriteHeader bool
	Headers     []string
}

// NewCSVWriter creates a new CSV writer
func NewCSVWriter(w io.WriteCloser, opts *CSVWriterOptions) *CSVWriter {
	if opts == nil {
		opts = &CSVWriterOptions{
			Comma:       ',',
			WriteHeader: true,
		}
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = opts.Comma
	csvWriter.UseCRLF = opts.UseCRLF

	return &CSVWriter{
		writer:  csvWriter,
		closer:  w,
		headers: opts.Headers,
	}
}

// Write implements the DataSink interface
func (c *CSVWriter) Write(ctx context.Context, record goetl.Record) error {
	// Write headers on first record if not explicitly set
	if !c.wrote {
		if len(c.headers) == 0 {
			// Extract headers from first record
			for key := range record {
				c.headers = append(c.headers, key)
			}
			// Sort for consistent ordering
			sort.Strings(c.headers)
		}

		if err := c.writer.Write(c.headers); err != nil {
			return fmt.Errorf("failed to write headers: %w", err)
		}
		c.wrote = true
	}

	// Convert record to string slice
	row := make([]string, len(c.headers))
	for i, header := range c.headers {
		if value, exists := record[header]; exists && value != nil {
			row[i] = fmt.Sprintf("%v", value)
		} else {
			row[i] = ""
		}
	}

	if err := c.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

// Flush implements the DataSink interface
func (c *CSVWriter) Flush() error {
	c.writer.Flush()
	return c.writer.Error()
}

// Close implements the DataSink interface
func (c *CSVWriter) Close() error {
	c.writer.Flush()
	if err := c.writer.Error(); err != nil {
		return err
	}
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}
