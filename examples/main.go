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
	"log"
	"os"
	"strings"
	"time"

	"github.com/aaronlmathis/goetl/filter"
	"github.com/aaronlmathis/goetl/readers"
	"github.com/aaronlmathis/goetl/transform"
	"github.com/aaronlmathis/goetl/writers"
	"github.com/apache/arrow/go/v12/parquet/compress"

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

	// Example 4: JSON -> Transform -> Parquet Roundtrip
	fmt.Println("\n=== Example 4: JSON to Parquet Conversion ===")
	if err := jsonToParquetExample(); err != nil {
		log.Printf("JSON to Parquet example failed: %v", err)
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
	csvReader, err := readers.NewCSVReader(&nopCloser{reader})
	if err != nil {
		return err
	}

	// Create output writer
	var output strings.Builder
	jsonWriter := writers.NewJSONWriter(&nopWriteCloser{&output})

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
	jsonReader := readers.NewJSONReader(&nopCloser{reader})

	// Create output writer
	var output strings.Builder
	csvWriter, err := writers.NewCSVWriter(
		&nopWriteCloser{&output},
		writers.WithCSVHeaders([]string{"product", "price_usd", "category", "is_electronics"}),
		writers.WithCSVDelimiter(','),
		writers.WithCSVWriteHeader(true),
	)
	if err != nil {
		return fmt.Errorf("failed to create CSV writer: %w", err)
	}

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
	csvReader, err := readers.NewCSVReader(&nopCloser{reader})
	if err != nil {
		return err
	}

	// Create output writer
	var output strings.Builder
	jsonWriter := writers.NewJSONWriter(&nopWriteCloser{&output})

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
		// Filter for high-value orders only (> $100)
		Filter(filter.GreaterThan("total_price", float64(100.0))).
		// Normalize customer names to uppercase
		Transform(transform.ToUpper("customer")).
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

func jsonToParquetExample() error {
	fmt.Println("Reading Titanic dataset and converting to Parquet...")

	// Open the JSON file
	file, err := os.Open("example_datasets/json/titanic.json")
	if err != nil {
		return fmt.Errorf("failed to open titanic.json: %w", err)
	}
	defer file.Close()

	// Create JSON reader
	jsonReader := readers.NewJSONReader(file)

	// Create Parquet writer with optimized settings for the Titanic dataset
	parquetWriter, err := writers.NewParquetWriter(
		"output/titanic.parquet",
		writers.WithBatchSize(100),                      // Smaller batches for better memory usage
		writers.WithCompression(compress.Codecs.Snappy), // Fast compression
		writers.WithFieldOrder([]string{ // Explicit field ordering for consistency
			"PassengerId", "Survived", "Pclass", "Name", "Sex",
			"Age", "SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked",
		}),
		writers.WithRowGroupSize(1000), // Optimize for file size
		writers.WithMetadata(map[string]string{
			"dataset":    "titanic",
			"source":     "titanic.json",
			"created_by": "goetl_example",
			"version":    "1.0",
		}),
		writers.WithSchemaValidation(false), // Allow flexible schema for missing fields
	)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Build pipeline with data cleaning and type conversion
	pipeline, err := goetl.NewPipeline().
		From(jsonReader).
		// Clean and normalize data
		Transform(transform.TrimSpace("Name", "Sex", "Ticket", "Cabin", "Embarked")).
		// Convert numeric fields to proper types
		Transform(transform.ToInt("PassengerId")).
		Transform(transform.ToInt("Survived")).
		Transform(transform.ToInt("Pclass")).
		Transform(transform.ToFloat("Age")). // Age might be missing, handle gracefully
		Transform(transform.ToInt("SibSp")).
		Transform(transform.ToInt("Parch")).
		Transform(transform.ToFloat("Fare")). // Fare might have decimals
		// Normalize categorical fields
		Transform(transform.ToUpper("Sex")).
		Transform(transform.ToUpper("Embarked")).
		// Add computed fields for analysis
		Transform(transform.AddField("has_cabin", func(r goetl.Record) interface{} {
			cabin := r["Cabin"]
			return cabin != nil && cabin != ""
		})).
		Transform(transform.AddField("family_size", func(r goetl.Record) interface{} {
			sibsp := getIntValue(r["SibSp"])
			parch := getIntValue(r["Parch"])
			return sibsp + parch + 1 // +1 for the passenger themselves
		})).
		Transform(transform.AddField("age_group", func(r goetl.Record) interface{} {
			age := getFloatValue(r["Age"])
			if age == 0 { // Missing age
				return "Unknown"
			} else if age < 18 {
				return "Child"
			} else if age < 65 {
				return "Adult"
			} else {
				return "Senior"
			}
		})).
		To(parquetWriter).
		WithErrorStrategy(goetl.SkipErrors). // Skip malformed records
		WithErrorHandler(goetl.ErrorHandlerFunc(func(ctx context.Context, record goetl.Record, err error) error {
			fmt.Printf("Warning: Skipping record due to error: %v\n", err)
			return nil // Continue processing
		})).
		Build()

	if err != nil {
		return fmt.Errorf("failed to build pipeline: %w", err)
	}

	// Execute the pipeline
	ctx := context.Background()
	start := time.Now()

	if err := pipeline.Execute(ctx); err != nil {
		return fmt.Errorf("pipeline execution failed: %w", err)
	}

	duration := time.Since(start)

	// Get statistics from the writer
	stats := parquetWriter.Stats()

	fmt.Printf("Conversion completed successfully!\n")
	fmt.Printf("Records processed: %d\n", stats.RecordsWritten)
	fmt.Printf("Batches written: %d\n", stats.BatchesWritten)
	fmt.Printf("Processing time: %v\n", duration)
	fmt.Printf("Flush duration: %v\n", stats.FlushDuration)

	// Show null value counts for data quality insights
	if len(stats.NullValueCounts) > 0 {
		fmt.Println("Null value counts by field:")
		for field, count := range stats.NullValueCounts {
			if count > 0 {
				fmt.Printf("  %s: %d\n", field, count)
			}
		}
	}

	// Verify the output file
	if fileInfo, err := os.Stat("output/titanic.parquet"); err == nil {
		fmt.Printf("Output file size: %d bytes\n", fileInfo.Size())
	}

	return nil
}

// Helper types for examples - updated to implement io.ReadCloser and io.WriteCloser
type nopCloser struct {
	*strings.Reader
}

func (nopCloser) Close() error { return nil }

type nopWriteCloser struct {
	*strings.Builder
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	return n.Builder.Write(p)
}

func (nopWriteCloser) Close() error { return nil }

// Helper functions for safe type conversion following your type-safe principles
func getIntValue(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

func getFloatValue(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}
