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

// dag_executor.go - DAG execution engine with topological sort
package dag

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/dag/tasks"
)

// DAGExecutor executes DAGs with topological sorting and parallelism
type DAGExecutor struct {
	maxWorkers   int
	retryBackoff tasks.BackoffStrategy
}

// DAGExecutorOption configures a DAGExecutor
type DAGExecutorOption func(*DAGExecutor)

// WithMaxWorkers sets the maximum number of concurrent workers
func WithMaxWorkers(workers int) DAGExecutorOption {
	return func(de *DAGExecutor) {
		if workers > 0 {
			de.maxWorkers = workers
		}
	}
}

// WithBackoffStrategy sets a custom backoff strategy
func WithBackoffStrategy(strategy tasks.BackoffStrategy) DAGExecutorOption {
	return func(de *DAGExecutor) {
		de.retryBackoff = strategy
	}
}

// NewDAGExecutor creates a new DAG executor with options
func NewDAGExecutor(opts ...DAGExecutorOption) *DAGExecutor {
	de := &DAGExecutor{
		maxWorkers: runtime.NumCPU(), // Default
		retryBackoff: &tasks.ExponentialBackoff{
			BaseDelay: time.Second,
			MaxDelay:  time.Minute,
		},
	}

	for _, opt := range opts {
		opt(de)
	}

	return de
}

// Keep backward compatibility with old signature
func NewDAGExecutorWithWorkers(maxWorkers int) *DAGExecutor {
	return NewDAGExecutor(WithMaxWorkers(maxWorkers))
}

// Execute runs the DAG using topological sort for dependency resolution
func (de *DAGExecutor) Execute(ctx context.Context, dag *DAG) (*DAGResult, error) {
	// Topologically sort tasks
	sortedTasks, err := dag.topologicalSort()
	if err != nil {
		return nil, fmt.Errorf("topological sort failed: %w", err)
	}

	// Create execution context
	execCtx := &executionContext{
		dag:           dag,
		taskOutputs:   make(map[string]tasks.TaskOutput),
		taskResults:   make(map[string]tasks.TaskResultMetadata),
		globalContext: make(map[string]interface{}),
		mu:            sync.RWMutex{},
	}

	// Copy global context
	for k, v := range dag.metadata.GlobalContext {
		execCtx.globalContext[k] = v
	}

	// Execute tasks in topological order with parallelism
	result, err := de.executeTasksParallel(ctx, execCtx, sortedTasks)
	if err != nil {
		return nil, fmt.Errorf("DAG execution failed: %w", err)
	}

	return result, nil
}

// executeTasksParallel executes tasks with controlled parallelism
func (de *DAGExecutor) executeTasksParallel(ctx context.Context, execCtx *executionContext, sortedTasks []string) (*DAGResult, error) {
	start := time.Now()

	// Group tasks by level for parallel execution
	taskLevels := de.groupTasksByLevel(execCtx.dag, sortedTasks)

	for levelIdx, level := range taskLevels {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Execute all tasks in this level in parallel
		if err := de.executeLevel(ctx, execCtx, level); err != nil {
			return &DAGResult{
				Success:     false,
				StartTime:   start,
				EndTime:     time.Now(),
				TaskResults: execCtx.taskResults,
				Error:       err,
			}, err
		}

		fmt.Printf("Completed level %d with %d tasks\n", levelIdx, len(level))
	}

	return &DAGResult{
		Success:     true,
		StartTime:   start,
		EndTime:     time.Now(),
		TaskResults: execCtx.taskResults,
	}, nil
}

// groupTasksByLevel groups tasks by their dependency level for parallel execution
func (de *DAGExecutor) groupTasksByLevel(dag *DAG, sortedTasks []string) [][]string {
	taskLevel := make(map[string]int)

	// Calculate level for each task
	for _, taskID := range sortedTasks {
		maxDepLevel := -1
		for _, dep := range dag.dependencies[taskID] {
			if depLevel, exists := taskLevel[dep]; exists {
				if depLevel > maxDepLevel {
					maxDepLevel = depLevel
				}
			}
		}
		taskLevel[taskID] = maxDepLevel + 1
	}

	// Group tasks by level
	levels := make(map[int][]string)
	maxLevel := 0
	for taskID, level := range taskLevel {
		levels[level] = append(levels[level], taskID)
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Convert to slice
	result := make([][]string, maxLevel+1)
	for level := 0; level <= maxLevel; level++ {
		result[level] = levels[level]
	}

	return result
}

// executeLevel executes all tasks in a level concurrently
func (de *DAGExecutor) executeLevel(ctx context.Context, execCtx *executionContext, taskIDs []string) error {
	if len(taskIDs) == 0 {
		return nil
	}

	// Use worker pool for controlled concurrency
	maxWorkers := de.maxWorkers
	if len(taskIDs) < maxWorkers {
		maxWorkers = len(taskIDs)
	}

	taskChan := make(chan string, len(taskIDs))
	errChan := make(chan error, len(taskIDs))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for taskID := range taskChan {
				if err := de.executeTaskWithRetry(ctx, execCtx, taskID); err != nil {
					errChan <- fmt.Errorf("task %s failed: %w", taskID, err)
					return
				}
			}
		}()
	}

	// Send tasks to workers
	for _, taskID := range taskIDs {
		taskChan <- taskID
	}
	close(taskChan)

	// Wait for completion
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		return err
	}

	return nil
}

// executeTaskWithRetry executes a single task with retry logic
func (de *DAGExecutor) executeTaskWithRetry(ctx context.Context, execCtx *executionContext, taskID string) error {
	task := execCtx.dag.tasks[taskID]
	metadata := task.Metadata()

	maxRetries := 0
	if metadata.RetryConfig != nil {
		maxRetries = metadata.RetryConfig.MaxRetries
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Create task context with timeout
		taskCtx := ctx
		if metadata.Timeout > 0 {
			var cancel context.CancelFunc
			taskCtx, cancel = context.WithTimeout(ctx, metadata.Timeout)
			defer cancel()
		}

		// Check trigger rule
		if !de.shouldExecuteTask(execCtx, task) {
			return fmt.Errorf("task %s trigger rule not satisfied", taskID)
		}

		// Prepare input
		input := de.prepareTaskInput(execCtx, task)

		// Execute task
		output, err := task.Execute(taskCtx, input)
		if err == nil {
			// Success - store output
			execCtx.mu.Lock()
			execCtx.taskOutputs[taskID] = output
			execCtx.taskResults[taskID] = output.Metadata
			// Update global context
			for k, v := range output.Context {
				execCtx.globalContext[k] = v
			}
			execCtx.mu.Unlock()
			return nil
		}

		lastErr = err

		// Check if we should retry this error
		if metadata.RetryConfig != nil && de.shouldRetryError(err, metadata.RetryConfig) {
			if attempt < maxRetries {
				delay := de.retryBackoff.Delay(attempt)
				fmt.Printf("Task %s attempt %d failed, retrying in %v: %v\n", taskID, attempt+1, delay, err)

				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		break
	}

	// Store failure result
	execCtx.mu.Lock()
	execCtx.taskResults[taskID] = tasks.TaskResultMetadata{
		Success:      false,
		Error:        lastErr,
		AttemptCount: maxRetries + 1,
	}
	execCtx.mu.Unlock()

	return lastErr
}

// shouldExecuteTask checks if a task should execute based on its trigger rule
func (de *DAGExecutor) shouldExecuteTask(execCtx *executionContext, task tasks.Task) bool {
	metadata := task.Metadata()
	dependencies := task.Dependencies()

	if len(dependencies) == 0 {
		return true // No dependencies, always execute
	}

	execCtx.mu.RLock()
	defer execCtx.mu.RUnlock()

	successCount := 0
	failureCount := 0
	completeCount := 0

	for _, depID := range dependencies {
		if result, exists := execCtx.taskResults[depID]; exists {
			completeCount++
			if result.Success {
				successCount++
			} else {
				failureCount++
			}
		}
	}

	switch metadata.TriggerRule {
	case TriggerAllSuccess:
		return successCount == len(dependencies)
	case TriggerAllComplete:
		return completeCount == len(dependencies)
	case TriggerOneFailed:
		return failureCount > 0
	case TriggerOneSuccess:
		return successCount > 0
	case TriggerNoneFailed:
		return failureCount == 0 && completeCount == len(dependencies)
	default:
		return successCount == len(dependencies) // Default to all success
	}
}

// Enhanced prepareTaskInput in dag_executor.go
func (de *DAGExecutor) prepareTaskInput(execCtx *executionContext, task tasks.Task) tasks.TaskInput {
	execCtx.mu.RLock()
	defer execCtx.mu.RUnlock()

	var allRecords []core.Record
	sourceMap := make(map[string][]core.Record)
	metadataMap := make(map[string]tasks.TaskResultMetadata)
	dependencies := task.Dependencies()

	// Gather records from all dependencies with source tracking
	for _, depID := range dependencies {
		if output, exists := execCtx.taskOutputs[depID]; exists {
			allRecords = append(allRecords, output.Records...)
			sourceMap[depID] = output.Records
			metadataMap[depID] = output.Metadata
		}
	}

	return tasks.TaskInput{
		Records:   allRecords,
		Context:   execCtx.globalContext,
		SourceMap: sourceMap,
		Metadata:  metadataMap,
	}
}

// shouldRetryError determines if an error should trigger a retry
func (de *DAGExecutor) shouldRetryError(err error, config *tasks.RetryConfig) bool {
	if len(config.RetryOn) == 0 {
		return true // Retry all errors if none specified
	}

	for _, retryErr := range config.RetryOn {
		if err.Error() == retryErr.Error() {
			return true
		}
	}

	return false
}

// executionContext holds state during DAG execution
type executionContext struct {
	dag           *DAG
	taskOutputs   map[string]tasks.TaskOutput
	taskResults   map[string]tasks.TaskResultMetadata
	globalContext map[string]interface{}
	mu            sync.RWMutex
}

// DAGResult contains the results of DAG execution
type DAGResult struct {
	Success     bool
	StartTime   time.Time
	EndTime     time.Time
	TaskResults map[string]tasks.TaskResultMetadata
	Error       error
}

// ExponentialBackoff implements exponential backoff strategy
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (eb *ExponentialBackoff) Delay(attempt int) time.Duration {
	delay := eb.BaseDelay * time.Duration(1<<uint(attempt))
	if delay > eb.MaxDelay {
		delay = eb.MaxDelay
	}
	return delay
}
