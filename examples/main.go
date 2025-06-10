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

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/aaronlmathis/goetl/filter"
	"github.com/aaronlmathis/goetl/readers"
	"github.com/aaronlmathis/goetl/transform"
	"github.com/aaronlmathis/goetl/writers"

	"github.com/aaronlmathis/goetl"
)

// User represents a simple data structure for our examples
type User struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Email string  `json:"email"`
	Age   int     `json:"age"`
	Score float64 `json:"score"`
}

func main() {
	fmt.Println("Go ETL Library - Example Usage")

	// Example 1: Basic CSV processing
	fmt.Println("\n=== Example 1: CSV Data Cleaning ===")
	if err := csvDataCleaningExample(); err != nil {
		log.Printf("CSV example failed: %v", err)
	}

	// Example 2: JSON processing
	fmt.Println("\n=== Example 2: JSON Data Transformation ===")
	if err := jsonTransformationExample(); err != nil {
		log.Printf("JSON example failed: %v", err)
	}

	// Example 3: Complex pipeline
	fmt.Println("\n=== Example 3: Complex Data Pipeline ===")
	if err := complexPipelineExample(); err != nil {
		log.Printf("Complex pipeline example failed: %v", err)
	}

	// Example 4: CSV -> Transform -> Parquet Roundtrip
	fmt.Println("\n=== Example 4: CSV -> Transform -> Parquet Roundtrip ===")
	if err := csvToParquetRoundtripExample(); err != nil {
		log.Printf("Parquet pipeline example failed: %v", err)
	}

	// Example 5: CSV to Parquet roundtrip
	fmt.Println("\n=== Example 5: CSV to Parquet Roundtrip ===")
	if err := csvToParquetRoundtripExample(); err != nil {
		log.Printf("CSV to Parquet roundtrip example failed: %v", err)
	}
}

func csvDataCleaningExample() error {
	// Sample CSV data
	csvData := `name,age,email,salary
" John Doe ",25,john@example.com,50000
" jane smith ",30,JANE@COMPANY.COM,75000
"Bob Johnson",35,bob@test.org,60000
" Alice Brown ",28,,45000`

	// Create input reader
	reader := strings.NewReader(csvData)
	csvReader, err := readers.NewCSVReader(nopCloser{reader}, nil)
	if err != nil {
		return err
	}

	// Create output writer
	var output strings.Builder
	jsonWriter := writers.NewJSONWriter(nopWriteCloser{&output})

	// Build pipeline
	pipeline, err := goetl.NewPipeline().
		From(csvReader).
		// Clean up data
		Transform(transform.TrimSpace("name", "email")).
		Transform(transform.ToLower("email")).
		// Filter out records without email
		Filter(filter.NotNull("email")).
		// Add processed timestamp
		Transform(transform.AddField("processed_at", func(r goetl.Record) interface{} {
			return "2024-01-01T00:00:00Z"
		})).
		To(jsonWriter).
		Build()

	if err != nil {
		return err
	}

	// Execute pipeline
	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		return err
	}

	fmt.Println("Cleaned data:")
	fmt.Println(output.String())
	return nil
}

func jsonTransformationExample() error {
	// Sample JSON lines data
	jsonData := `{"product": "Laptop", "price": "999.99", "category": "electronics"}
{"product": "Book", "price": "19.99", "category": "books"}
{"product": "Mouse", "price": "25.50", "category": "electronics"}`

	// Create input reader
	reader := strings.NewReader(jsonData)
	jsonReader := readers.NewJSONReader(nopCloser{reader})
	// Create output writer
	var output strings.Builder
	csvWriter := writers.NewCSVWriter(
		nopWriteCloser{&output},
		&writers.CSVWriterOptions{
			Comma:       ',',
			WriteHeader: true,
			Headers:     []string{"product", "price_usd", "category", "is_electronics"},
		},
	)

	// Build pipeline
	pipeline, err := goetl.NewPipeline().
		From(jsonReader).
		// Convert price to float
		Transform(transform.ToFloat("price")).
		// Rename price field
		Transform(transform.Rename(map[string]string{"price": "price_usd"})).
		// Add computed field
		Transform(transform.AddField("is_electronics", func(r goetl.Record) interface{} {
			return r["category"] == "electronics"
		})).
		To(csvWriter).
		Build()

	if err != nil {
		return err
	}

	// Execute pipeline
	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		return err
	}

	fmt.Println("Transformed data:")
	fmt.Println(output.String())
	return nil
}

func complexPipelineExample() error {
	// Sample sales data
	salesData := `customer,product,quantity,unit_price,region
Alice,Laptop,1,999.99,North
Bob,Mouse,2,25.50,South
Alice,Keyboard,1,79.99,North
Charlie,Laptop,1,999.99,East
Bob,Monitor,1,299.99,South
Diana,Mouse,3,25.50,West`

	// Create input reader
	reader := strings.NewReader(salesData)
	csvReader, err := readers.NewCSVReader(nopCloser{reader}, nil)
	if err != nil {
		return err
	}

	// Create output writer
	var output strings.Builder
	jsonWriter := writers.NewJSONWriter(nopWriteCloser{&output})

	// Build complex pipeline
	pipeline, err := goetl.NewPipeline().
		From(csvReader).
		// Convert numeric fields to proper types
		Transform(transform.ToInt("quantity")).
		Transform(transform.ToFloat("unit_price")).
		// Calculate total price for each order
		Transform(transform.AddField("total_price", func(r goetl.Record) interface{} {
			qty := r["quantity"].(int)
			price := r["unit_price"].(float64)
			return float64(qty) * price
		})).
		Transform(transform.AddField("debug_total_price", func(r goetl.Record) interface{} {
			fmt.Printf("Filtering: total_price=%v (%T)\n", r["total_price"], r["total_price"])
			return nil
		})).
		// Filter for high-value orders only (> $100)
		Filter(filter.GreaterThan("total_price", float64(100.0))).
		// Normalize customer names to uppercase
		Transform(transform.ToUpper("customer")).
		// Debugging: print types of quantity and unit_price
		Transform(transform.AddField("debug_types", func(r goetl.Record) interface{} {
			fmt.Printf("quantity: %T, unit_price: %T\n", r["quantity"], r["unit_price"])
			return nil
		})).
		// Add order category based on total value
		Transform(transform.AddField("order_category", func(r goetl.Record) interface{} {
			total := r["total_price"].(float64)
			if total > 500 {
				return "high_value"
			} else if total > 200 {
				return "medium_value"
			}
			return "low_value"
		})).
		// Select only the fields we want in output
		Transform(transform.Select("customer", "product", "total_price", "region", "order_category")).
		To(jsonWriter).
		WithErrorStrategy(goetl.SkipErrors).
		Build()

	if err != nil {
		return err
	}

	// Execute pipeline
	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		return err
	}

	fmt.Println("Processed sales data:")
	fmt.Println(output.String())
	return nil
}

func csvToParquetRoundtripExample() error {
	// Sample CSV data with users
	csvData := `name,age,score
Alice,30,95.5
Bob,25,88.0
Charlie,40,91.2
Diana,22,72.0`

	// Create input CSV reader
	reader := strings.NewReader(csvData)
	csvReader, err := readers.NewCSVReader(nopCloser{reader}, nil)
	if err != nil {
		return fmt.Errorf("failed to create CSV reader: %w", err)
	}

	// Create Parquet writer to a temporary file
	parquetFile := "example_roundtrip.parquet"
	parquetWriter, err := writers.NewParquetWriter(parquetFile, &writers.ParquetWriterOptions{
		BatchSize:  10,
		FieldOrder: []string{"name", "age", "score"},
	})
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Build pipeline to write CSV -> Parquet with transformations
	pipeline, err := goetl.NewPipeline().
		From(csvReader).
		// Transform name to uppercase
		Transform(transform.ToUpper("name")).
		// Filter for ages >= 30
		Filter(filter.GreaterThan("age", 29)).
		To(parquetWriter).
		WithErrorStrategy(goetl.SkipErrors).
		Build()

	if err != nil {
		return fmt.Errorf("failed to build pipeline: %w", err)
	}

	// Execute pipeline to write Parquet file
	ctx := context.Background()
	if err := pipeline.Execute(ctx); err != nil {
		return fmt.Errorf("failed to execute write pipeline: %w", err)
	}

	fmt.Printf("Wrote filtered/transformed records to Parquet: %s\n", parquetFile)

	// Now read back from the Parquet file
	parquetReader, err := readers.NewParquetReader(parquetFile, &readers.ParquetReaderOptions{
		BatchSize: 10,
	})
	if err != nil {
		return fmt.Errorf("failed to open Parquet reader: %w", err)
	}

	// Read all records and print them
	fmt.Println("Reading back from Parquet file:")
	for {
		record, err := parquetReader.Read(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}
		fmt.Printf("  %+v\n", record)
	}

	// Close the reader
	if err := parquetReader.Close(); err != nil {
		fmt.Printf("Warning: failed to close Parquet reader: %v\n", err)
	}

	fmt.Println("Parquet roundtrip completed successfully!")
	return nil
}

// Helper types for examples
type nopCloser struct {
	*strings.Reader
}

func (nopCloser) Close() error { return nil }

type nopWriteCloser struct {
	*strings.Builder
}

func (nopWriteCloser) Close() error { return nil }
