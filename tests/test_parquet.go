package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aaronlmathis/goetl/readers"
)

func main() {
	fmt.Println("Testing Parquet reader...")

	reader, err := readers.NewParquetReader("../examples/example_roundtrip.parquet", nil)
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ { // Limit to avoid infinite loop
		record, err := reader.Read(ctx)
		if err == io.EOF {
			fmt.Println("Reached end of file")
			break
		}
		if err != nil {
			log.Fatalf("Failed to read record: %v", err)
		}
		fmt.Printf("Record %d: %+v\n", i+1, record)
	}
}
