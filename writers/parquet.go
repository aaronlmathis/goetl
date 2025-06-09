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
	"errors"
	"fmt"
	"goetl"
	"reflect"

	local "github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetWriter implements DataSink for Parquet files
// Only supports writing to a file path (not generic io.Writer)
type ParquetWriter struct {
	pw           *writer.ParquetWriter
	wc           source.ParquetFile
	schemaStruct interface{} // keep a reference to the schema struct if provided
}

type ParquetWriterOptions struct {
	SchemaStruct    interface{} // Optional: user-supplied struct for schema
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec
}

// NewParquetWriter creates a new Parquet writer for a file
func NewParquetWriter(filename string, opts *ParquetWriterOptions) (*ParquetWriter, error) {
	wc, err := local.NewLocalFileWriter(filename)
	if err != nil {
		return nil, err
	}
	var schema interface{}
	var schemaStruct interface{}
	if opts != nil && opts.SchemaStruct != nil {
		schema = opts.SchemaStruct
		schemaStruct = opts.SchemaStruct
	} else {
		schema = make(map[string]interface{}) // fallback: generic map (flat only)
		schemaStruct = nil
	}
	pw, err := writer.NewParquetWriter(wc, schema, 1)
	if err != nil {
		wc.Close()
		return nil, err
	}
	if opts != nil {
		if opts.RowGroupSize > 0 {
			pw.RowGroupSize = opts.RowGroupSize
		}
		if opts.CompressionType != 0 {
			pw.CompressionType = opts.CompressionType
		}
	}
	return &ParquetWriter{pw: pw, wc: wc, schemaStruct: schemaStruct}, nil
}

// Write implements the DataSink interface
func (p *ParquetWriter) Write(ctx context.Context, rec goetl.Record) error {
	if p.schemaStruct != nil {
		// Convert map to struct
		structVal, err := mapToStruct(rec, p.schemaStruct)
		if err != nil {
			fmt.Printf("[DEBUG] ParquetWriter: failed to convert record: %+v, err: %v\n", rec, err)
			return fmt.Errorf("failed to convert record to struct: %w", err)
		}
		fmt.Printf("[DEBUG] ParquetWriter: writing struct: %+v\n", structVal)
		return p.pw.Write(structVal)
	}
	fmt.Printf("[DEBUG] ParquetWriter: writing map: %+v\n", rec)
	return p.pw.Write(rec)
}

// mapToStruct converts a map[string]interface{} to a struct of the same type as schemaPtr
func mapToStruct(m map[string]interface{}, schemaPtr interface{}) (interface{}, error) {
	// schemaPtr is a pointer to a struct (e.g., &Person{})
	typ := reflect.TypeOf(schemaPtr)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return nil, errors.New("schemaPtr must be pointer to struct")
	}
	val := reflect.New(typ.Elem()).Elem()
	for i := 0; i < typ.Elem().NumField(); i++ {
		field := typ.Elem().Field(i)
		name := field.Name
		// Use struct tag if present
		if tag, ok := field.Tag.Lookup("parquet"); ok {
			// parse tag for name=...
			for _, part := range splitParquetTag(tag) {
				if len(part) > 5 && part[:5] == "name=" {
					name = part[5:]
				}
			}
		}
		if v, ok := m[name]; ok && v != nil {
			fv := reflect.ValueOf(v)
			// Try to set the value, with type conversion if needed
			if fv.Type().AssignableTo(field.Type) {
				val.Field(i).Set(fv)
			} else if fv.Type().ConvertibleTo(field.Type) {
				val.Field(i).Set(fv.Convert(field.Type))
			} else {
				// Try string to int/float, etc.
				switch field.Type.Kind() {
				case reflect.Int, reflect.Int32, reflect.Int64:
					var ival int64
					switch vt := v.(type) {
					case int:
						ival = int64(vt)
					case int32:
						ival = int64(vt)
					case int64:
						ival = vt
					case float64:
						ival = int64(vt)
					case string:
						fmt.Sscanf(vt, "%d", &ival)
					}
					val.Field(i).SetInt(ival)
				case reflect.Float32, reflect.Float64:
					var fval float64
					switch vt := v.(type) {
					case float64:
						fval = vt
					case float32:
						fval = float64(vt)
					case int:
						fval = float64(vt)
					case int32:
						fval = float64(vt)
					case string:
						fmt.Sscanf(vt, "%f", &fval)
					}
					val.Field(i).SetFloat(fval)
				case reflect.String:
					val.Field(i).SetString(fmt.Sprintf("%v", v))
				}
			}
		}
	}
	return val.Addr().Interface(), nil
}

// splitParquetTag splits a parquet struct tag into parts
func splitParquetTag(tag string) []string {
	var parts []string
	for _, part := range []rune(tag) {
		if part == ',' {
			parts = append(parts, "")
		} else {
			if len(parts) == 0 {
				parts = append(parts, "")
			}
			parts[len(parts)-1] += string(part)
		}
	}
	return parts
}

// Flush implements the DataSink interface
func (p *ParquetWriter) Flush() error {
	return nil // ParquetWriter does not require explicit flush
}

// Close implements the DataSink interface
func (p *ParquetWriter) Close() error {
	err1 := p.pw.WriteStop()
	err2 := p.wc.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
