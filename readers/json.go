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
	"bufio"
	"context"
	"encoding/json"
	"io"

	"goetl"
)

// JSONReader implements DataSource for JSON lines files
type JSONReader struct {
	scanner *bufio.Scanner
	closer  io.Closer
}

// NewJSONReader creates a new JSON reader for line-delimited JSON
func NewJSONReader(r io.ReadCloser) *JSONReader {
	scanner := bufio.NewScanner(r)
	return &JSONReader{
		scanner: scanner,
		closer:  r,
	}
}

// Read implements the DataSource interface
func (j *JSONReader) Read(ctx context.Context) (goetl.Record, error) {
	if !j.scanner.Scan() {
		if err := j.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	line := j.scanner.Text()
	var record goetl.Record

	if err := json.Unmarshal([]byte(line), &record); err != nil {
		return nil, err
	}

	return record, nil
}

// Close implements the DataSource interface
func (j *JSONReader) Close() error {
	if j.closer != nil {
		return j.closer.Close()
	}
	return nil
}
