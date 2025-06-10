package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

func main() {
	fmt.Println("Debugging Parquet file...")

	// Open the parquet file
	reader, err := file.OpenParquetFile("../examples/example_roundtrip.parquet", false)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}

	fmt.Printf("File has %d rows\n", reader.NumRows())
	fmt.Printf("File has %d row groups\n", reader.NumRowGroups())

	// Print schema info
	schema := reader.Schema()
	fmt.Printf("Schema has %d columns:\n", schema.NumColumns())
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		fmt.Printf("  Column %d: %s (%s)\n", i, col.Name(), col.PhysicalType())
	}

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		log.Fatalf("Failed to create arrow reader: %v", err)
	}

	// Get Arrow schema
	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		log.Fatalf("Failed to get arrow schema: %v", err)
	}

	fmt.Printf("Arrow schema has %d fields:\n", arrowSchema.NumFields())
	for i := 0; i < arrowSchema.NumFields(); i++ {
		field := arrowSchema.Field(i)
		fmt.Printf("  Field %d: %s (%s)\n", i, field.Name, field.Type)
	}

	// Try to read all row groups
	for i := 0; i < reader.NumRowGroups(); i++ {
		fmt.Printf("Row group %d has %d rows\n", i, reader.RowGroup(i).NumRows())
	}
}
