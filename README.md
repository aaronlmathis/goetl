# Go ETL Library

A comprehensive, high-performance Go library for data transformation pipelines. This library focuses on the "Transform" phase of ETL (Extract, Transform, Load) operations, providing a fluent API for building complex data processing workflows.

## Features

### Core Capabilities
- **Streaming Processing**: Efficient record-by-record processing for large datasets
- **Fluent API**: Chainable operations for building intuitive pipelines
- **Type Safety**: Robust type conversion with configurable error handling
- **Extensible Design**: Interface-driven architecture for custom implementations
- **Concurrent Processing**: Go routine support for parallel operations
- **Comprehensive Error Handling**: Multiple error strategies (fail-fast, skip, collect)

### Transformation Operations
- **Mapping/Projection**: Select, rename, and reorder fields
- **Filtering**: Conditional record filtering with multiple criteria
- **Type Conversion**: String, numeric, boolean, and date/time conversions
- **Data Cleaning**: Trimming, case conversion, normalization
- **Field Enhancement**: Add computed fields and derived values
- **Aggregation**: Group by operations with sum, count, avg, min, max

### Supported Formats
- **CSV**: Configurable delimiters, headers, quoting
- **JSON**: Line-delimited JSON (JSONL) format
- **Extensible**: Easy to add custom readers and writers

## Quick Start

```go
package main

import (
    "context"
    "log"
    "os"
    
    "goetl"
    "goetl/readers"
    "goetl/writers"
    "goetl/transform"
    "goetl/filter"
)

func main() {
    // Open input CSV file
    inputFile, err := os.Open("input.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer inputFile.Close()

    // Create CSV reader
    csvReader, err := readers.NewCSVReader(inputFile, nil)
    if err != nil {
        log.Fatal(err)
    }

    // Open output JSON file
    outputFile, err := os.Create("output.json")
    if err != nil {
        log.Fatal(err)
    }
    defer outputFile.Close()

    // Create JSON writer
    jsonWriter := writers.NewJSONWriter(outputFile)

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

## API Reference

### Pipeline Builder

```go
// Create a new pipeline
pipeline := goetl.NewPipeline()

// Set data source
pipeline.From(dataSource)

// Add transformations
pipeline.Transform(transformer)
pipeline.Map(func(ctx context.Context, record goetl.Record) (goetl.Record, error) {
    // Custom transformation logic
    return record, nil
})

// Add filters
pipeline.Filter(filter)
pipeline.Where(func(ctx context.Context, record goetl.Record) (bool, error) {
    // Custom filter logic
    return true, nil
})

// Set data sink
pipeline.To(dataSink)

// Configure error handling
pipeline.WithErrorStrategy(goetl.SkipErrors)
pipeline.WithErrorHandler(errorHandler)

// Build and execute
builtPipeline, err := pipeline.Build()
if err != nil {
    // handle error
}

err = builtPipeline.Execute(context.Background())
```

### Transformations

```go
import "goetl/transform"

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

### Filters

```go
import "goetl/filter"

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

### Readers and Writers

```go
import (
    "goetl/readers"
    "goetl/writers"
)

// CSV Reader
csvReader, err := readers.NewCSVReader(file, &readers.CSVReaderOptions{
    Comma:            ',',
    HasHeaders:       true,
    TrimLeadingSpace: true,
})

// JSON Reader (line-delimited)
jsonReader := readers.NewJSONReader(file)

// CSV Writer
csvWriter := writers.NewCSVWriter(file, &writers.CSVWriterOptions{
    Comma:       ',',
    WriteHeader: true,
    Headers:     []string{"col1", "col2", "col3"},
})

// JSON Writer (line-delimited)
jsonWriter := writers.NewJSONWriter(file)
```

## Error Handling

The library provides three error handling strategies:

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

## Examples

Check the `examples/` directory for complete working examples:

- **Basic CSV Processing**: Data cleaning and validation
- **JSON Transformation**: Format conversion and field manipulation
- **Complex Pipeline**: Multi-step transformation with aggregation
- **Custom Components**: Implementing custom transformers and filters

## Performance Considerations

- **Streaming**: Records are processed one at a time to minimize memory usage
- **Type Assertions**: Use type assertions carefully in custom transformations
- **Error Handling**: Choose appropriate error strategy based on your use case
- **Concurrent Processing**: Leverage Go routines for CPU-intensive transformations

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

Run the test suite:

```bash
go test ./...
```

Run with coverage:

```bash
go test -cover ./...
```

Run benchmarks:

```bash
go test -bench=. ./...
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Roadmap

- [ ] Database readers/writers (SQL, NoSQL)
- [ ] Message queue integration (Kafka, RabbitMQ)
- [ ] Advanced aggregation operations
- [ ] Schema validation and inference
- [ ] Metrics and observability hooks
- [ ] Performance optimizations
- [ ] Documentation improvements
# GoETL
