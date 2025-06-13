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

package dag

import (
	"fmt"
	"time"

	"github.com/aaronlmathis/goetl/dag/tasks"
)

// GetTasks returns all tasks in the DAG
func (d *DAG) GetTasks() map[string]tasks.Task {
	return d.tasks
}

// GetDependencies returns the dependencies for a specific task
func (d *DAG) GetDependencies(taskID string) []string {
	if deps, exists := d.dependencies[taskID]; exists {
		return deps
	}
	return []string{}
}

// GetAllDependencies returns the complete dependency map
func (d *DAG) GetAllDependencies() map[string][]string {
	return d.dependencies
}

// GetTasksByType returns tasks filtered by type
func (d *DAG) GetTasksByType(taskType tasks.TaskType) map[string]tasks.Task {
	result := make(map[string]tasks.Task)
	for id, task := range d.tasks {
		if task.Metadata().TaskType == taskType {
			result[id] = task
		}
	}
	return result
}

// GetTaskCount returns the total number of tasks
func (d *DAG) GetTaskCount() int {
	return len(d.tasks)
}

// HasTask checks if a task exists in the DAG
func (d *DAG) HasTask(taskID string) bool {
	_, exists := d.tasks[taskID]
	return exists
}

// GetUpstreamTasks returns all tasks that this task depends on
func (d *DAG) GetUpstreamTasks(taskID string) []string {
	return d.GetDependencies(taskID)
}

// GetDownstreamTasks returns all tasks that depend on this task
func (d *DAG) GetDownstreamTasks(taskID string) []string {
	var downstream []string
	for id, deps := range d.dependencies {
		for _, dep := range deps {
			if dep == taskID {
				downstream = append(downstream, id)
				break
			}
		}
	}
	return downstream
}

// GetMetadata returns the DAG's metadata
func (d *DAG) GetMetadata() DAGMetadata {
	return d.metadata
}

// GetID returns the DAG's unique identifier
func (d *DAG) GetID() string {
	return d.id
}

// GetName returns the DAG's name
func (d *DAG) GetName() string {
	return d.name
}

// GetDescription returns the DAG's description
func (d *DAG) GetDescription() string {
	return d.metadata.Description
}

// GetMaxParallelism returns the configured maximum parallelism
func (d *DAG) GetMaxParallelism() int {
	return d.metadata.MaxParallelism
}

// GetDefaultTimeout returns the default timeout for tasks
func (d *DAG) GetDefaultTimeout() time.Duration {
	return d.metadata.DefaultTimeout
}

// GetDefaultRetries returns the default retry configuration
func (d *DAG) GetDefaultRetries() *tasks.RetryConfig {
	return d.metadata.DefaultRetries
}

// GetGlobalContext returns the global context map
func (d *DAG) GetGlobalContext() map[string]interface{} {
	return d.metadata.GlobalContext
}

// SetGlobalContextValue sets a value in the global context
func (d *DAG) SetGlobalContextValue(key string, value interface{}) {
	if d.metadata.GlobalContext == nil {
		d.metadata.GlobalContext = make(map[string]interface{})
	}
	d.metadata.GlobalContext[key] = value
}

// GetGlobalContextValue retrieves a value from the global context
func (d *DAG) GetGlobalContextValue(key string) (interface{}, bool) {
	if d.metadata.GlobalContext == nil {
		return nil, false
	}
	value, exists := d.metadata.GlobalContext[key]
	return value, exists
}

// PrintDAGStructure provides a human-readable DAG structure for debugging
func (d *DAG) PrintDAGStructure() {
	fmt.Printf("DAG: %s (%s) - %s\n", d.name, d.id, d.metadata.Description)
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Max Parallelism: %d\n", d.metadata.MaxParallelism)
	fmt.Printf("  Default Timeout: %v\n", d.metadata.DefaultTimeout)
	fmt.Printf("  Tasks: %d\n", len(d.tasks))

	fmt.Println("Structure:")
	for id, task := range d.tasks {
		metadata := task.Metadata()
		deps := d.GetDependencies(id)
		downstream := d.GetDownstreamTasks(id)

		fmt.Printf("  %s [%s]\n", id, metadata.TaskType)
		if metadata.Description != "" {
			fmt.Printf("    Description: %s\n", metadata.Description)
		}
		if len(deps) > 0 {
			fmt.Printf("    ← depends on: %v\n", deps)
		}
		if len(downstream) > 0 {
			fmt.Printf("    → triggers: %v\n", downstream)
		}
		if metadata.Timeout > 0 {
			fmt.Printf("    Timeout: %v\n", metadata.Timeout)
		}
		if metadata.RetryConfig != nil {
			fmt.Printf("    Retries: %d\n", metadata.RetryConfig.MaxRetries)
		}
		if len(metadata.Tags) > 0 {
			fmt.Printf("    Tags: %v\n", metadata.Tags)
		}
	}
}

// GetDAGMetrics returns comprehensive metrics about the DAG structure
func (d *DAG) GetDAGMetrics() map[string]interface{} {
	sourceCount := len(d.GetTasksByType(tasks.TaskTypeSource))
	transformCount := len(d.GetTasksByType(tasks.TaskTypeTransform))
	sinkCount := len(d.GetTasksByType(tasks.TaskTypeSink))
	joinCount := len(d.GetTasksByType(tasks.TaskTypeJoin))
	filterCount := len(d.GetTasksByType(tasks.TaskTypeFilter))
	cdcCount := len(d.GetTasksByType(tasks.TaskTypeCDC))
	scdCount := len(d.GetTasksByType(tasks.TaskTypeSCD))
	conditionalCount := len(d.GetTasksByType(tasks.TaskTypeConditional))
	aggregateCount := len(d.GetTasksByType(tasks.TaskTypeAggregate))

	return map[string]interface{}{
		"dag_id":            d.id,
		"dag_name":          d.name,
		"description":       d.metadata.Description,
		"total_tasks":       len(d.tasks),
		"source_tasks":      sourceCount,
		"transform_tasks":   transformCount,
		"sink_tasks":        sinkCount,
		"join_tasks":        joinCount,
		"filter_tasks":      filterCount,
		"cdc_tasks":         cdcCount,
		"scd_tasks":         scdCount,
		"conditional_tasks": conditionalCount,
		"aggregate_tasks":   aggregateCount,
		"max_depth":         d.calculateMaxDepth(),
		"has_cycles":        d.hasCycle(),
		"execution_order":   d.getExecutionOrderSafe(),
	}
}

// ValidateDAGStructure performs comprehensive DAG validation
func (d *DAG) ValidateDAGStructure() []error {
	var errors []error

	// Check for missing dependencies
	for taskID, deps := range d.dependencies {
		for _, dep := range deps {
			if !d.HasTask(dep) {
				errors = append(errors, fmt.Errorf("task %s depends on non-existent task %s", taskID, dep))
			}
		}
	}

	// Check for orphaned tasks (no dependencies and no dependents)
	for taskID := range d.tasks {
		upstream := d.GetUpstreamTasks(taskID)
		downstream := d.GetDownstreamTasks(taskID)

		if len(upstream) == 0 && len(downstream) == 0 && len(d.tasks) > 1 {
			errors = append(errors, fmt.Errorf("task %s appears to be orphaned (no connections)", taskID))
		}
	}

	// Check for cycles using topological sort
	if d.hasCycle() {
		errors = append(errors, fmt.Errorf("DAG contains cycles"))
	}

	// Validate task configurations
	for taskID, task := range d.tasks {
		metadata := task.Metadata()

		// Check for invalid timeout
		if metadata.Timeout < 0 {
			errors = append(errors, fmt.Errorf("task %s has invalid negative timeout", taskID))
		}

		// Check retry configuration
		if metadata.RetryConfig != nil && metadata.RetryConfig.MaxRetries < 0 {
			errors = append(errors, fmt.Errorf("task %s has invalid negative retry count", taskID))
		}
	}

	return errors
}

// GetExecutionOrder returns tasks in topological execution order
func (d *DAG) GetExecutionOrder() ([]string, error) {
	return d.topologicalSort()
}

// getExecutionOrderSafe returns execution order or empty slice if cycles exist
func (d *DAG) getExecutionOrderSafe() []string {
	if order, err := d.GetExecutionOrder(); err == nil {
		return order
	}
	return []string{}
}

func (d *DAG) hasCycle() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for taskID := range d.tasks {
		if !visited[taskID] {
			if d.dfsHasCycle(taskID, visited, recStack) {
				return true
			}
		}
	}
	return false
}

func (d *DAG) dfsHasCycle(taskID string, visited, recStack map[string]bool) bool {
	visited[taskID] = true
	recStack[taskID] = true

	for _, dep := range d.dependencies[taskID] {
		if !visited[dep] {
			if d.dfsHasCycle(dep, visited, recStack) {
				return true
			}
		} else if recStack[dep] {
			return true
		}
	}

	recStack[taskID] = false
	return false
}

// calculateMaxDepth - useful for debugging but not essential
func (d *DAG) calculateMaxDepth() int {
	depths := make(map[string]int)

	var calculateDepth func(taskID string) int
	calculateDepth = func(taskID string) int {
		if depth, exists := depths[taskID]; exists {
			return depth
		}

		maxDepth := 0
		for _, dep := range d.GetDependencies(taskID) {
			depDepth := calculateDepth(dep)
			if depDepth > maxDepth {
				maxDepth = depDepth
			}
		}

		depths[taskID] = maxDepth + 1
		return depths[taskID]
	}

	maxOverall := 0
	for taskID := range d.tasks {
		depth := calculateDepth(taskID)
		if depth > maxOverall {
			maxOverall = depth
		}
	}

	return maxOverall
}

// topologicalSort performs Kahn's algorithm for topological sorting
func (d *DAG) topologicalSort() ([]string, error) {
	// Calculate in-degrees
	inDegree := make(map[string]int)
	for taskID := range d.tasks {
		inDegree[taskID] = 0
	}

	for taskID, deps := range d.dependencies {
		inDegree[taskID] = len(deps)
	}

	// Initialize queue with tasks that have no dependencies
	queue := make([]string, 0)
	for taskID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, taskID)
		}
	}

	// Process queue
	var result []string
	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// Reduce in-degree for dependent tasks
		for taskID, deps := range d.dependencies {
			for _, dep := range deps {
				if dep == current {
					inDegree[taskID]--
					if inDegree[taskID] == 0 {
						queue = append(queue, taskID)
					}
				}
			}
		}
	}

	// Check for cycles
	if len(result) != len(d.tasks) {
		return nil, fmt.Errorf("DAG contains cycles")
	}

	return result, nil
}
