# Go ETL Library

A comprehensive, high-performance Go library for data transformation pipelines. This library focuses on the "Transform" phase of ETL (Extract, Transform, Load) operations, providing a fluent API for building complex data processing workflows.

## Features

### Core Capabilities
- **Streaming Processing**: Efficient record-by-record processing for large datasets
- **Fluent API**: Chainable operations for building intuitive pipelines
- **Type Safety**: Robust type conversion with configurable error handling
- **Extensible Design**: Interface-driven architecture for custom implementations
- **Concurrent Processing**: Thread-safe operations with proper synchronization
- **Comprehensive Error Handling**: Multiple error strategies (fail-fast, skip, collect)

### Transformation Operations
- **Mapping/Projection**: Select, rename, and reorder fields
- **Filtering**: Conditional record filtering with multiple criteria
- **Type Conversion**: String, numeric, boolean, and date/time conversions
- **Data Cleaning**: Trimming, case conversion, normalization
- **Field Enhancement**: Add computed fields and derived values
- **Aggregation**: Group by operations with sum, count, avg, min, max

### Supported Formats
- **CSV**: Configurable delimiters, headers, quoting, batching
- **JSON**: Line-delimited JSON (JSONL) format with batching
- **Parquet**: High-performance columnar format with compression
- **Extensible**: Easy to add custom readers and writers

## Quick Start

```go
package main

import (
    "context"
    "log"
    "os"
    
    "github.com/aaronlmathis/goetl"
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
    "github.com/aaronlmathis/goetl/transform"
    "github.com/aaronlmathis/goetl/filter"
)

func main() {
    // Open input CSV file
    inputFile, err := os.Open("input.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer inputFile.Close()

    // Create CSV reader
    csvReader, err := readers.NewCSVReader(inputFile)
    if err != nil {
        log.Fatal(err)
    }

    // Open output JSON file
    outputFile, err := os.Create("output.json")
    if err != nil {
        log.Fatal(err)
    }
    defer outputFile.Close()

    // Create JSON writer with batching
    jsonWriter := writers.NewJSONWriter(outputFile, 
        writers.WithJSONBatchSize(100),
        writers.WithFlushOnWrite(false),
    )

    // Build and execute pipeline
    pipeline, err := goetl.NewPipeline().
        From(csvReader).
        Filter(filter.NotNull("email")).
        Transform(transform.TrimSpace("name", "email")).
        Transform(transform.ToLower("email")).
        Transform(transform.AddField("processed_at", func(r goetl.Record) interface{} {
            return time.Now().Format(time.RFC3339)
        })).
        To(jsonWriter).
        WithErrorStrategy(goetl.SkipErrors).
        Build()

    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := pipeline.Execute(ctx); err != nil {
        log.Fatal(err)
    }

    log.Println("Pipeline executed successfully!")
}
```

## Transformations

```go
import "github.com/aaronlmathis/goetl/transform"

// Field selection and renaming
transform.Select("field1", "field2", "field3")
transform.Rename(map[string]string{"old_name": "new_name"})

// Type conversions
transform.ToString("field")
transform.ToInt("field")
transform.ToFloat("field")

// String operations
transform.TrimSpace("field1", "field2")
transform.ToUpper("field1", "field2")
transform.ToLower("field1", "field2")

// Add computed fields
transform.AddField("new_field", func(record goetl.Record) interface{} {
    // Compute value based on other fields
    return record["field1"].(string) + "_processed"
})

// Date/time parsing
transform.ParseTime("date_field", "2006-01-02")
```

## Filters

```go
import "github.com/aaronlmathis/goetl/filter"

// Null/empty checks
filter.NotNull("field")

// Value comparisons
filter.Equals("status", "active")
filter.GreaterThan("age", 18)
filter.LessThan("score", 100)
filter.Between("salary", 50000, 100000)

// String operations
filter.Contains("email", "@example.com")
filter.StartsWith("name", "John")
filter.EndsWith("file", ".pdf")
filter.MatchesRegex("phone", `^\d{3}-\d{3}-\d{4}$`)

// Set operations
filter.In("category", "books", "music", "movies")

// Logical operations
filter.And(filter1, filter2, filter3)
filter.Or(filter1, filter2)
filter.Not(filter1)
```

## Readers and Writers

### CSV Reader/Writer

```go
import (
    "github.com/aaronlmathis/goetl/readers"
    "github.com/aaronlmathis/goetl/writers"
)

// CSV Reader
csvReader, err := readers.NewCSVReader(file)

// CSV Writer with functional options
csvWriter, err := writers.NewCSVWriter(file,
    writers.WithHeaders([]string{"id", "name", "email"}),
    writers.WithComma(','),
    writers.WithWriteHeader(true),
    writers.WithCSVBatchSize(100),
    writers.WithUseCRLF(false),
)
```

### JSON Reader/Writer

```go
// JSON Reader (line-delimited)
jsonReader := readers.NewJSONReader(file)

// JSON Writer with batching
jsonWriter := writers.NewJSONWriter(file,
    writers.WithJSONBatchSize(100),
    writers.WithFlushOnWrite(false),
)
```

### Parquet Writer

```go
import "github.com/apache/arrow/go/v12/parquet/compress"

// Parquet Writer with compression and metadata
parquetWriter, err := writers.NewParquetWriter(
    "output.parquet",
    writers.WithBatchSize(1000),
    writers.WithCompression(compress.Codecs.Snappy),
    writers.WithFieldOrder([]string{"id", "name", "email", "age"}),
    writers.WithMetadata(map[string]string{
        "dataset": "users",
        "version": "1.0",
    }),
    writers.WithRowGroupSize(10000),
    writers.WithSchemaValidation(true),
)
```

## Error Handling

The library provides comprehensive error handling with granular error types:

### Error Strategies

1. **FailFast** (default): Stop processing on the first error
2. **SkipErrors**: Continue processing, skip problematic records
3. **CollectErrors**: Continue processing, collect all errors for review

```go
pipeline.WithErrorStrategy(goetl.SkipErrors)

// Custom error handler
pipeline.WithErrorHandler(goetl.ErrorHandlerFunc(func(ctx context.Context, record goetl.Record, err error) error {
    log.Printf("Error processing record %v: %v", record, err)
    return nil // Continue processing
}))
```

### Error Types

Each component provides specific error types for better error handling:

```go
// CSV Writer errors
&writers.CSVWriterError{Op: "write_row", Err: ...}
&writers.CSVWriterError{Op: "csv_flush", Err: ...}

// JSON Writer errors  
&writers.JSONWriterError{Op: "marshal_record", Err: ...}
&writers.JSONWriterError{Op: "write_json", Err: ...}

// Parquet Writer errors
&writers.ParquetWriterError{Op: "schema_inference", Err: ...}
&writers.ParquetWriterError{Op: "write_batch", Err: ...}
```

## Performance and Statistics

All writers provide detailed performance statistics:

```go
// Get writer statistics
stats := writer.Stats()
fmt.Printf("Records written: %d\n", stats.RecordsWritten)
fmt.Printf("Batches written: %d\n", stats.BatchesWritten)
fmt.Printf("Flush count: %d\n", stats.FlushCount)
fmt.Printf("Processing time: %v\n", stats.FlushDuration)

// Null value tracking for data quality
for field, count := range stats.NullValueCounts {
    fmt.Printf("Field %s has %d null values\n", field, count)
}
```

## Examples

Check the `examples/` directory for complete working examples:

- **Basic CSV Processing**: Data cleaning and validation
- **JSON Transformation**: Format conversion and field manipulation  
- **Complex Pipeline**: Multi-step transformation with filtering and aggregation
- **JSON to Parquet**: High-performance format conversion with compression

### Example: CSV Data Cleaning

```go
func csvDataCleaningExample() error {
    csvData := `name,age,email,salary
" John Doe ",25,john@example.com,50000
" jane smith ",30,JANE@COMPANY.COM,75000
"Bob Johnson",35,bob@test.org,60000`

    reader := strings.NewReader(csvData)
    csvReader, err := readers.NewCSVReader(&nopCloser{reader})
    if err != nil {
        return err
    }

    var output strings.Builder
    jsonWriter := writers.NewJSONWriter(&nopWriteCloser{&output})

    pipeline, err := goetl.NewPipeline().
        From(csvReader).
        Transform(transform.TrimSpace("name", "email")).
        Transform(transform.ToLower("email")).
        Filter(filter.NotNull("email")).
        Transform(transform.AddField("processed_at", func(r goetl.Record) interface{} {
            return time.Now().Format(time.RFC3339)
        })).
        To(jsonWriter).
        Build()

    if err != nil {
        return err
    }

    return pipeline.Execute(context.Background())
}
```

### Example: JSON to Parquet with Data Quality

```go
func jsonToParquetExample() error {
    file, err := os.Open("input.json")
    if err != nil {
        return err
    }
    defer file.Close()

    jsonReader := readers.NewJSONReader(file)

    parquetWriter, err := writers.NewParquetWriter(
        "output.parquet",
        writers.WithBatchSize(1000),
        writers.WithCompression(compress.Codecs.Snappy),
        writers.WithFieldOrder([]string{"id", "name", "email", "age"}),
        writers.WithMetadata(map[string]string{
            "dataset": "users",
            "version": "1.0",
        }),
    )
    if err != nil {
        return err
    }

    pipeline, err := goetl.NewPipeline().
        From(jsonReader).
        Transform(transform.TrimSpace("name", "email")).
        Transform(transform.ToInt("id")).
        Transform(transform.ToFloat("age")).
        Transform(transform.ToUpper("email")).
        Transform(transform.AddField("has_email", func(r goetl.Record) interface{} {
            email := r["email"]
            return email != nil && email != ""
        })).
        To(parquetWriter).
        WithErrorStrategy(goetl.SkipErrors).
        Build()

    if err != nil {
        return err
    }

    ctx := context.Background()
    if err := pipeline.Execute(ctx); err != nil {
        return err
    }

    // Print statistics
    stats := parquetWriter.Stats()
    fmt.Printf("Processed %d records in %d batches\n", 
        stats.RecordsWritten, stats.BatchesWritten)
    
    return nil
}
```

## Performance Considerations

- **Streaming**: Records are processed one at a time to minimize memory usage
- **Batching**: Configure optimal batch sizes for your workload
- **Type Assertions**: Use type assertions carefully in custom transformations
- **Error Handling**: Choose appropriate error strategy based on your use case
- **Concurrent Safety**: All writers are thread-safe with proper synchronization

### Optimal Batch Sizes

- **CSV**: 100-1000 records per batch
- **JSON**: 100-500 records per batch  
- **Parquet**: 1000-10000 records per batch (depends on schema complexity)

## Extending the Library

### Custom Transformers

```go
type CustomTransformer struct {
    // configuration fields
}

func (ct *CustomTransformer) Transform(ctx context.Context, record goetl.Record) (goetl.Record, error) {
    // Implementation
    return record, nil
}
```

### Custom Filters

```go
type CustomFilter struct {
    // configuration fields
}

func (cf *CustomFilter) ShouldInclude(ctx context.Context, record goetl.Record) (bool, error) {
    // Implementation
    return true, nil
}
```

### Custom Data Sources

```go
type CustomDataSource struct {
    // implementation fields
}

func (cds *CustomDataSource) Read(ctx context.Context) (goetl.Record, error) {
    // Implementation
    return record, nil
}

func (cds *CustomDataSource) Close() error {
    // Cleanup
    return nil
}
```

## Testing

The library includes comprehensive test coverage:

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. ./...

# Run specific package tests
go test ./writers
go test ./readers  
go test ./transform
go test ./filter
```

## Error Recovery and Data Quality

The library provides tools for monitoring data quality:

```go
// Track null values across fields
stats := writer.Stats()
for field, count := range stats.NullValueCounts {
    if count > 0 {
        log.Printf("Warning: Field '%s' has %d null values", field, count)
    }
}

// Custom validation
pipeline.Transform(transform.AddField("validation_errors", func(r goetl.Record) interface{} {
    var errors []string
    if r["email"] == nil || r["email"] == "" {
        errors = append(errors, "missing_email")
    }
    if age, ok := r["age"].(int); !ok || age < 0 || age > 150 {
        errors = append(errors, "invalid_age")
    }
    return errors
}))
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the GPL-3.0-or-later License - see the LICENSE file for details.

## Roadmap

- [ ] Advanced aggregation operations with grouping
- [ ] Database readers/writers (SQL, NoSQL)
- [ ] Message queue integration (Kafka, RabbitMQ)
- [ ] Schema validation and inference improvements
- [ ] Metrics and observability hooks
- [ ] Additional compression formats
- [ ] Performance optimizations for large datasets
- [ ] Documentation improvements and tutorials
