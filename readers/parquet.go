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
	"reflect"

	local "github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"goetl"
)

// ParquetReader implements DataSource for Parquet files
// It streams records as map[string]interface{} for compatibility with the pipeline
// Schema is inferred from the file, or can be provided via options (future)
type ParquetReader struct {
	pr           *reader.ParquetReader
	rc           source.ParquetFile
	rowIdx       int64
	schemaStruct interface{} // keep schema struct for reading
}

type ParquetReaderOptions struct {
	SchemaStruct interface{} // Optional: user-supplied struct for schema
}

// NewParquetReader creates a new Parquet reader
func NewParquetReader(filename string, opts *ParquetReaderOptions) (*ParquetReader, error) {
	rc, err := local.NewLocalFileReader(filename)
	if err != nil {
		return nil, err
	}
	var schema interface{} = nil
	var schemaStruct interface{} = nil
	if opts != nil && opts.SchemaStruct != nil {
		schema = opts.SchemaStruct
		schemaStruct = opts.SchemaStruct
	}
	pr, err := reader.NewParquetReader(rc, schema, 1)
	if err != nil {
		rc.Close()
		return nil, err
	}
	return &ParquetReader{pr: pr, rc: rc, schemaStruct: schemaStruct}, nil
}

// Read implements the DataSource interface
func (p *ParquetReader) Read(ctx context.Context) (goetl.Record, error) {
	if p.rowIdx >= p.pr.GetNumRows() {
		return nil, io.EOF
	}
	if p.schemaStruct != nil {
		// Read into a slice of the struct type (not pointer)
		structType := reflect.TypeOf(p.schemaStruct)
		if structType.Kind() == reflect.Ptr {
			structType = structType.Elem()
		}
		sliceType := reflect.SliceOf(structType)
		slicePtr := reflect.New(sliceType)
		slice := slicePtr.Interface()
		err := p.pr.Read(slice)
		if err != nil {
			fmt.Printf("[DEBUG] ParquetReader: read error: %v\n", err)
			return nil, err
		}
		results := reflect.Indirect(slicePtr)
		if results.Len() == 0 {
			fmt.Printf("[DEBUG] ParquetReader: no records read, returning EOF\n")
			return nil, io.EOF
		}
		p.rowIdx++
		// Convert struct to map[string]interface{}
		rec := structToMap(results.Index(0).Interface())
		fmt.Printf("[DEBUG] ParquetReader: read record: %+v\n", rec)
		return rec, nil
	}
	// Fallback: read as map[string]interface{}
	var recs []map[string]interface{}
	err := p.pr.Read(&recs)
	if err != nil {
		fmt.Printf("[DEBUG] ParquetReader: read error: %v\n", err)
		return nil, err
	}
	if len(recs) == 0 {
		fmt.Printf("[DEBUG] ParquetReader: no records read, returning EOF\n")
		return nil, io.EOF
	}
	p.rowIdx++
	fmt.Printf("[DEBUG] ParquetReader: read record: %+v\n", recs[0])
	return recs[0], nil
}

// structToMap converts a struct to map[string]interface{}
func structToMap(s interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	v := reflect.ValueOf(s)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		name := field.Name
		if tag, ok := field.Tag.Lookup("parquet"); ok {
			for _, part := range splitParquetTag(tag) {
				if len(part) > 5 && part[:5] == "name=" {
					name = part[5:]
				}
			}
		}
		m[name] = v.Field(i).Interface()
	}
	return m
}

// splitParquetTag splits a parquet struct tag into parts
func splitParquetTag(tag string) []string {
	var parts []string
	current := ""
	for _, c := range tag {
		if c == ',' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

// Close implements the DataSource interface
func (p *ParquetReader) Close() error {
	p.pr.ReadStop()
	return p.rc.Close()
}

// PrSchema returns the Parquet schema handler for debugging
func (p *ParquetReader) PrSchema() interface{} {
	return p.pr.SchemaHandler
}
