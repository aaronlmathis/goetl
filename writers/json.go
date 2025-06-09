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
	"io"

	"goetl"
)

// JSONWriter implements DataSink for JSON lines files
type JSONWriter struct {
	writer io.Writer
	closer io.Closer
}

// NewJSONWriter creates a new JSON writer for line-delimited JSON output
func NewJSONWriter(w io.WriteCloser) *JSONWriter {
	return &JSONWriter{
		writer: w,
		closer: w,
	}
}

// Write implements the DataSink interface
func (j *JSONWriter) Write(ctx context.Context, record goetl.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record to JSON: %w", err)
	}

	// Write JSON line
	if _, err := j.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write JSON data: %w", err)
	}

	// Write newline
	if _, err := j.writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// Flush implements the DataSink interface
func (j *JSONWriter) Flush() error {
	if flusher, ok := j.writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

// Close implements the DataSink interface
func (j *JSONWriter) Close() error {
	if j.closer != nil {
		return j.closer.Close()
	}
	return nil
}
