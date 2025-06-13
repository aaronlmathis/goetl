// example_dag.go - Example DAG usage
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/dag"
	"github.com/aaronlmathis/goetl/dag/tasks"
	"github.com/aaronlmathis/goetl/filter"
	"github.com/aaronlmathis/goetl/readers"
	"github.com/aaronlmathis/goetl/transform"
	"github.com/aaronlmathis/goetl/validators"
	"github.com/aaronlmathis/goetl/writers"
)

// Add missing variables and complete the examples

// Add these variables at package level or in respective functions
var (
	insertSink      core.DataSink
	dimensionWriter core.DataSink
)

func simpleDAGExample() error {

	jsonFile, err := os.Open("../example_datasets/json/iris.json")
	if err != nil {
		return fmt.Errorf("failed to open JSON file: %w", err)
	}
	defer jsonFile.Close()

	jsonReader := readers.NewJSONReader(jsonFile)

	csvFile, err := os.Create("../output/iris_out.csv") // Create, not Open
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer csvFile.Close()

	csvWriter, err := writers.NewCSVWriter(csvFile)
	if err != nil {
		return fmt.Errorf("failed to create CSV writer: %w", err)
	}

	// Helper function to debug record structure
	getRecordKeys := func(r core.Record) []string {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		return keys
	}

	// Transform function - Enhanced with debugging
	sepalTransform := transform.AddField("sepal.square", func(r core.Record) interface{} {
		// Debug: Print the record structure for the first few records
		fmt.Printf("DEBUG: Record keys: %v\n", getRecordKeys(r))

		sepalLength, ok := r["sepal.length"].(float64)
		if !ok {
			// Try alternative type assertions
			if val, ok := r["sepal.length"].(string); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil {
					sepalLength = parsed
				} else {
					fmt.Printf("DEBUG: sepal.length parse error: %v, value: %v\n", err, val)
					return nil
				}
			} else if val, ok := r["sepal.length"].(int); ok {
				sepalLength = float64(val)
			} else {
				fmt.Printf("DEBUG: sepal_length type assertion failed, type: %T, value: %v\n", r["sepal.length"], r["sepal.length"])
				return nil
			}
		}

		sepalWidth, ok := r["sepal.width"].(float64)
		if !ok {
			// Try alternative type assertions
			if val, ok := r["sepal_width"].(string); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil {
					sepalWidth = parsed
				} else {
					fmt.Printf("DEBUG: sepal_width parse error: %v, value: %v\n", err, val)
					return nil
				}
			} else if val, ok := r["sepal.width"].(int); ok {
				sepalWidth = float64(val)
			} else {
				fmt.Printf("DEBUG: sepal.width type assertion failed, type: %T, value: %v\n", r["sepal.width"], r["sepal.width"])
				return nil
			}
		}

		result := sepalLength * sepalWidth
		fmt.Printf("DEBUG: sepal.length=%v, sepal.width=%v, result=%v\n", sepalLength, sepalWidth, result)
		return result
	})

	// Build DAG with retry configuration
	retryConfig := &tasks.RetryConfig{
		MaxRetries: 3,
		Strategy:   &tasks.ExponentialBackoff{BaseDelay: time.Second, MaxDelay: time.Minute},
	}

	dagInstance, err := dag.NewDAG("example_simple_dag", "JSON to CSV Pipeline").
		AddSourceTask("extract_iris", jsonReader,
			tasks.WithRetryConfig(retryConfig),
			tasks.WithTimeout(5*time.Minute),
			tasks.WithDescription("Extract iris data from JSON file"),
			tasks.WithTags("source", "json", "iris")).
		AddTransformTask("square_length_width", sepalTransform, []string{"extract_iris"},
			tasks.WithRetryConfig(retryConfig),
			tasks.WithDescription("Calculate sepal area (length × width)"),
			tasks.WithTags("transform", "calculation")).
		AddSinkTask("write_csv", csvWriter, []string{"square_length_width"},
			tasks.WithTimeout(2*time.Minute),
			tasks.WithDescription("Write results to CSV file"),
			tasks.WithTags("sink", "csv")).
		WithMaxParallelism(4).
		WithDefaultTimeout(10 * time.Minute).
		Build()

	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}

	// ========== DEBUGGING DEMONSTRATIONS ==========

	fmt.Println("=== DAG Structure Analysis ===")
	dagInstance.PrintDAGStructure()

	fmt.Println("\n=== DAG Validation ===")
	if validationErrors := dagInstance.ValidateDAGStructure(); len(validationErrors) > 0 {
		fmt.Println("Validation Errors Found:")
		for _, err := range validationErrors {
			fmt.Printf("  %v\n", err)
		}
		return fmt.Errorf("DAG validation failed")
	} else {
		fmt.Println("DAG validation passed")
	}

	fmt.Println("\n=== DAG Metrics ===")
	metrics := dagInstance.GetDAGMetrics()
	for key, value := range metrics {
		fmt.Printf("  %s: %v\n", key, value)
	}

	fmt.Println("\n=== Execution Order ===")
	if order, err := dagInstance.GetExecutionOrder(); err == nil {
		fmt.Printf("Planned execution order: %v\n", order)
	} else {
		fmt.Printf("Cannot determine execution order: %v\n", err)
	}

	fmt.Println("\n=== Task Dependencies ===")
	for id, task := range dagInstance.GetTasks() {
		deps := dagInstance.GetDependencies(id)
		downstream := dagInstance.GetDownstreamTasks(id)
		metadata := task.Metadata()

		fmt.Printf(" Task: %s [%s]\n", id, metadata.TaskType)
		if len(deps) > 0 {
			fmt.Printf("   ← Dependencies: %v\n", deps)
		}
		if len(downstream) > 0 {
			fmt.Printf("   → Downstream: %v\n", downstream)
		}
		if len(metadata.Tags) > 0 {
			fmt.Printf("     Tags: %v\n", metadata.Tags)
		}
	}

	// Execute DAG
	fmt.Println("\n=== DAG Execution ===")
	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4))
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("DAG execution failed: %w", err)
	}

	// Print results
	fmt.Printf("\nDAG execution completed successfully in %v\n", result.EndTime.Sub(result.StartTime))
	for taskID, taskResult := range result.TaskResults {
		fmt.Printf("Task %s: %d records in, %d records out, took %v\n",
			taskID, taskResult.RecordsIn, taskResult.RecordsOut,
			taskResult.EndTime.Sub(taskResult.StartTime))
	}

	return nil
}

func dagExample() error {
	// Create PostgreSQL reader
	pgReader, err := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila?sslmode=disable"),
		readers.WithPostgresQuery("SELECT actor_id, first_name, last_name FROM actor"),
	)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	// Create JSON writer
	jsonWriter := writers.NewJSONWriter(os.Stdout)

	// Create transformers
	addFullName := transform.AddField("full_name", func(r core.Record) interface{} {
		firstName, _ := r["first_name"].(string)
		lastName, _ := r["last_name"].(string)
		return fmt.Sprintf("%s %s", firstName, lastName)
	})

	toLowerCase := transform.ToLower("first_name")

	// Create filter
	evenActorFilter := filter.Custom(func(r core.Record) bool {
		actorID, _ := r["actor_id"].(int)
		return actorID%2 == 0
	})

	// Build DAG with retry configuration
	retryConfig := &tasks.RetryConfig{
		MaxRetries: 3,
		Strategy:   &tasks.ExponentialBackoff{BaseDelay: time.Second, MaxDelay: time.Minute},
	}

	dagInstance, err := dag.NewDAG("example_dag", "PostgreSQL to JSON Pipeline").
		AddSourceTask("extract_actors", pgReader,
			tasks.WithRetryConfig(retryConfig),
			tasks.WithTimeout(5*time.Minute),
			tasks.WithDescription("Extract actor data from PostgreSQL pagila database"),
			tasks.WithTags("source", "postgresql", "actors"),
			tasks.WithOwner("data-team")).
		AddTransformTask("add_full_name", addFullName, []string{"extract_actors"},
			tasks.WithRetryConfig(retryConfig),
			tasks.WithDescription("Combine first_name and last_name fields"),
			tasks.WithTags("transform", "string-ops")).
		AddTransformTask("lowercase_names", toLowerCase, []string{"add_full_name"},
			tasks.WithDescription("Convert first_name to lowercase"),
			tasks.WithTags("transform", "normalization")).
		AddFilterTask("filter_even_actors", evenActorFilter, []string{"lowercase_names"},
			tasks.WithDescription("Filter actors with even actor_id"),
			tasks.WithTags("filter", "business-logic")).
		AddSinkTask("write_json", jsonWriter, []string{"filter_even_actors"},
			tasks.WithTimeout(2*time.Minute),
			tasks.WithDescription("Write filtered results to JSON output"),
			tasks.WithTags("sink", "json")).
		WithMaxParallelism(4).
		WithDefaultTimeout(10 * time.Minute).
		Build()

	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}

	// ========== COMPREHENSIVE DEBUGGING ==========

	fmt.Println("=== DAG Information ===")
	fmt.Printf("ID: %s\n", dagInstance.GetID())
	fmt.Printf("Name: %s\n", dagInstance.GetName())
	fmt.Printf("Description: %s\n", dagInstance.GetDescription())
	fmt.Printf("Max Parallelism: %d\n", dagInstance.GetMaxParallelism())
	fmt.Printf("Default Timeout: %v\n", dagInstance.GetDefaultTimeout())
	fmt.Printf("Total Tasks: %d\n", dagInstance.GetTaskCount())

	fmt.Println("\n=== Task Type Breakdown ===")
	taskTypes := []tasks.TaskType{
		tasks.TaskTypeSource, tasks.TaskTypeTransform,
		tasks.TaskTypeFilter, tasks.TaskTypeSink,
	}

	for _, taskType := range taskTypes {
		tasksByType := dagInstance.GetTasksByType(taskType)
		if len(tasksByType) > 0 {
			fmt.Printf("  %s: %d tasks\n", taskType, len(tasksByType))
			for id := range tasksByType {
				fmt.Printf("    - %s\n", id)
			}
		}
	}

	fmt.Println("\n=== Detailed Structure ===")
	dagInstance.PrintDAGStructure()

	fmt.Println("\n=== Pre-execution Validation ===")
	validationErrors := dagInstance.ValidateDAGStructure()
	if len(validationErrors) > 0 {
		fmt.Println("⚠️  Validation Issues:")
		for _, err := range validationErrors {
			fmt.Printf("  - %v\n", err)
		}
	} else {
		fmt.Println("✅ All validations passed")
	}

	fmt.Println("\n=== DAG Metrics ===")
	metrics := dagInstance.GetDAGMetrics()
	fmt.Printf("📈 Metrics: %+v\n", metrics)

	// Execute DAG
	fmt.Println("\n=== Executing DAG ===")
	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4))
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("DAG execution failed: %w", err)
	}

	// Print results with enhanced formatting
	fmt.Printf("\n🎉 DAG execution completed successfully in %v\n", result.EndTime.Sub(result.StartTime))
	fmt.Println("\n📊 Task Execution Summary:")
	for taskID, taskResult := range result.TaskResults {
		status := "✅"
		if !taskResult.Success {
			status = "❌"
		}
		fmt.Printf("  %s %s: %d→%d records, %v\n",
			status, taskID, taskResult.RecordsIn, taskResult.RecordsOut,
			taskResult.EndTime.Sub(taskResult.StartTime))
	}

	return nil
}

// Multiple sources feeding into a join/merge operation
func multiSourceFanInExample() error {
	// Create multiple data sources
	pgActors, _ := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila"),
		readers.WithPostgresQuery("SELECT actor_id, first_name, last_name FROM actor"),
	)

	csvRatings, _ := readers.NewCSVReader(os.Stdin)
	apiData, _ := readers.NewHTTPReader("https://api.example.com/metadata")
	jsonWriter := writers.NewJSONWriter(os.Stdout)

	// Build DAG with multiple sources
	dagInstance, err := dag.NewDAG("multi_source_dag", "Multi-Source ETL Pipeline").
		// Independent source extraction
		AddSourceTask("extract_actors", pgActors).
		AddSourceTask("extract_ratings", csvRatings).
		AddSourceTask("extract_metadata", apiData).

		// Join/merge task that depends on multiple sources
		AddJoinTask("join_actor_ratings",
			tasks.JoinConfig{ // Changed from joinConfig
				JoinType:    "inner",
				LeftKeys:    []string{"actor_id"}, // Changed from LeftKey to LeftKeys
				RightKeys:   []string{"actor_id"}, // Changed from RightKey to RightKeys
				FieldPrefix: map[string]string{},  // Add required field
				Strategy:    "hash",               // Add strategy field
			},
			[]string{"extract_actors", "extract_ratings"}). // Multiple dependencies

		// Another join adding metadata -
		AddJoinTask("add_metadata",
			tasks.JoinConfig{ // Changed from joinConfig
				JoinType:    "left",
				LeftKeys:    []string{"movie_id"}, // Changed from LeftKey to LeftKeys
				RightKeys:   []string{"id"},       // Changed from RightKey to RightKeys
				FieldPrefix: map[string]string{},  // Add required field
				Strategy:    "hash",               // Add strategy field
			},
			[]string{"join_actor_ratings", "extract_metadata"}).
		AddSinkTask("write_combined", jsonWriter, []string{"add_metadata"}).
		Build()

	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4))
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("DAG execution failed: %w", err)
	}

	// Print results
	fmt.Printf("DAG execution completed successfully in %v\n", result.EndTime.Sub(result.StartTime))
	for taskID, taskResult := range result.TaskResults {
		fmt.Printf("Task %s: %d records in, %d records out, took %v\n",
			taskID, taskResult.RecordsIn, taskResult.RecordsOut,
			taskResult.EndTime.Sub(taskResult.StartTime))
	}

	return err // Add proper error handling
}

// Single source feeding multiple parallel transformations
func multiDestinationFanOutExample() error {
	pgReader, _ := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila?sslmode=disable"),
		readers.WithPostgresQuery("SELECT actor_id, first_name, last_name FROM actor"),
	)

	// Create writers for different destinations
	analyticsWriter := writers.NewJSONWriter(os.Stdout)
	reportWriter, _ := writers.NewCSVWriter(os.Stdout)
	mlWriter, _ := writers.NewParquetWriter("ml_features.parquet")

	// Create transformers for different processing needs
	analyticsTransformer := transform.AddField("full_name", func(r core.Record) interface{} {
		firstName, _ := r["first_name"].(string)
		lastName, _ := r["last_name"].(string)
		return fmt.Sprintf("%s %s", firstName, lastName)
	})

	reportingTransformer := transform.ToLower("first_name")

	mlTransformer := transform.AddField("actor_id_str", func(r core.Record) interface{} {
		actorID, _ := r["actor_id"].(int)
		return fmt.Sprintf("actor_%d", actorID)
	})

	// Build DAG for fan-out processing
	dagInstance, err := dag.NewDAG("fan_out_dag", "Fan-Out Processing").
		AddSourceTask("extract_data", pgReader).

		// Multiple parallel transformation branches
		AddTransformTask("transform_analytics", analyticsTransformer,
			[]string{"extract_data"}).
		AddTransformTask("transform_reporting", reportingTransformer,
			[]string{"extract_data"}).
		AddTransformTask("transform_ml", mlTransformer,
			[]string{"extract_data"}).

		// Multiple sinks
		AddSinkTask("write_analytics", analyticsWriter, []string{"transform_analytics"}).
		AddSinkTask("write_reports", reportWriter, []string{"transform_reporting"}).
		AddSinkTask("write_ml_features", mlWriter, []string{"transform_ml"}).
		Build()

	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4)) // Use functional options
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("DAG execution failed: %w", err)
	}

	// Print results
	fmt.Printf("DAG execution completed successfully in %v\n", result.EndTime.Sub(result.StartTime))
	for taskID, taskResult := range result.TaskResults {
		fmt.Printf("Task %s: %d records in, %d records out, took %v\n",
			taskID, taskResult.RecordsIn, taskResult.RecordsOut,
			taskResult.EndTime.Sub(taskResult.StartTime))
	}
	return err
}

// Professional ETL pattern with conditional branching
func complexMultiSourceExample() error {
	// Multiple heterogeneous sources
	pgCustomers, _ := readers.NewPostgresReader(
		readers.WithPostgresDSN("postgres://test:test@localhost/pagila"),
		readers.WithPostgresQuery("SELECT customer_id, name, email FROM customers"),
	)

	mongoOrders, _ := readers.NewMongoReader(
		readers.WithMongoURI("mongodb://localhost:27017"),
		readers.WithMongoDB("order_db"),
		readers.WithMongoCollection("orders"),
	)

	s3Files, _ := readers.NewS3Reader(
		readers.WithS3Bucket("my-bucket"),
		readers.WithS3Prefix("data/files/"),
	)

	// Create writers for different destinations
	warehouseWriter, _ := writers.NewParquetWriter("warehouse_data.parquet")
	analyticsWriter := writers.NewJSONWriter(os.Stdout)

	// Data quality validators
	customerValidator := &validators.DataQualityValidator{
		MinRecords:     1000,
		RequiredFields: []string{"customer_id", "name"},
	}

	orderValidator := &validators.DataQualityValidator{
		MinRecords:     500,
		RequiredFields: []string{"order_id", "customer_id"},
	}

	// Build complex DAG
	dagInstance, err := dag.NewDAG("complex_etl", "Complex Multi-Source ETL").
		// Parallel source extraction
		AddSourceTask("extract_customers", pgCustomers).
		AddSourceTask("extract_orders", mongoOrders).
		AddSourceTask("extract_files", s3Files).

		// Data quality checks using conditional tasks
		AddConditionalTask("validate_customers", customerValidator, []string{"extract_customers"}).
		AddConditionalTask("validate_orders", orderValidator, []string{"extract_orders"}).

		// Join based on validation results
		AddJoinTask("join_customer_orders",
			tasks.JoinConfig{
				JoinType:    "inner",
				LeftKeys:    []string{"customer_id"},
				RightKeys:   []string{"customer_id"},
				FieldPrefix: map[string]string{"left": "cust_", "right": "order_"},
				Strategy:    "hash",
			},
			[]string{"validate_customers", "validate_orders"},
			tasks.WithTriggerRule(tasks.TriggerRuleAllSuccess)).

		// Enrichment from file data
		AddJoinTask("enrich_with_files",
			tasks.JoinConfig{
				JoinType:    "left",
				LeftKeys:    []string{"file_id"},
				RightKeys:   []string{"id"},
				FieldPrefix: map[string]string{"right": "file_"},
				Strategy:    "hash",
			},
			[]string{"join_customer_orders", "extract_files"}).

		// Multiple output formats
		AddSinkTask("write_warehouse", warehouseWriter,
			[]string{"enrich_with_files"}).
		AddSinkTask("write_analytics", analyticsWriter,
			[]string{"enrich_with_files"}).
		Build()

	if err != nil {
		return fmt.Errorf("failed to build complex DAG: %w", err)
	}

	// Execute DAG
	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4)) // Use functional options
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("complex DAG execution failed: %w", err)
	}

	fmt.Printf("Complex DAG execution completed in %v\n", result.EndTime.Sub(result.StartTime))
	return nil
}

func cdcPipelineExample() error {
	// Source system snapshots
	currentSnapshot, _ := readers.NewPostgresReader( /*current state*/ )
	previousSnapshot, _ := readers.NewPostgresReader( /*previous state*/ )

	dagInstance, err := dag.NewDAG("cdc_pipeline", "Change Data Capture").
		AddSourceTask("extract_current", currentSnapshot).
		AddSourceTask("extract_previous", previousSnapshot).

		// CDC comparison task - Fix: use core.CDCConfig instead of cdcConfig
		AddCDCTask("detect_changes",
			tasks.CDCConfig{ // Changed from cdcConfig
				KeyFields:     []string{"id"},
				CompareFields: []string{"name", "email", "updated_at"},
				ChangeTypes:   []string{"INSERT", "UPDATE", "DELETE"},
			},
			[]string{"extract_current", "extract_previous"}).

		// Route changes to different destinations
		AddFilterTask("filter_inserts",
			filter.Custom(func(r core.Record) bool { // Fix: use core.Record instead of Record
				return r["change_type"] == "INSERT"
			}),
			[]string{"detect_changes"}).
		AddSinkTask("write_inserts", insertSink, []string{"filter_inserts"}).
		Build()

	// DEBUG: Print DAG structure before execution
	fmt.Println("DAG Tasks:")
	for id, _ := range dagInstance.GetTasks() {
		deps := dagInstance.GetDependencies(id)
		fmt.Printf("  %s -> dependencies: %v\n", id, deps)
	}

	executor := dag.NewDAGExecutor(dag.WithMaxWorkers(4)) // Use functional options
	result, err := executor.Execute(context.Background(), dagInstance)
	if err != nil {
		return fmt.Errorf("DAG execution failed: %w", err)
	}

	// Print results
	fmt.Printf("DAG execution completed successfully in %v\n", result.EndTime.Sub(result.StartTime))
	for taskID, taskResult := range result.TaskResults {
		fmt.Printf("Task %s: %d records in, %d records out, took %v\n",
			taskID, taskResult.RecordsIn, taskResult.RecordsOut,
			taskResult.EndTime.Sub(taskResult.StartTime))
	}
	return err // Add proper error handling
}

func scdPipelineExample() error {
	sourceData, _ := readers.NewPostgresReader( /*source*/ )
	dimensionTable, _ := readers.NewPostgresReader( /*existing dimension*/ )

	_, err := dag.NewDAG("scd_pipeline", "Slowly Changing Dimensions").
		AddSourceTask("extract_source", sourceData).
		AddSourceTask("extract_dimension", dimensionTable).

		// SCD Type 2 processing - Fix: use core.SCDConfig instead of core.scdConfig
		AddSCDTask("process_scd2",
			tasks.SCDConfig{ // Changed from core.scdConfig
				Type:               "SCD2",
				KeyFields:          []string{"customer_id"},
				TrackingFields:     []string{"name", "address", "phone"},
				EffectiveFromField: "effective_from",
				EffectiveToField:   "effective_to",
				CurrentFlag:        "is_current",
			},
			[]string{"extract_source", "extract_dimension"}).
		AddSinkTask("write_dimension", dimensionWriter, []string{"process_scd2"}).
		Build()

	return err // Add proper error handling
}

func main() {
	if err := simpleDAGExample(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
