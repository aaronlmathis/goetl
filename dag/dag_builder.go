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

// dag_builder.go - Fluent API for DAG construction
package dag

import (
	"fmt"
	"time"

	"github.com/aaronlmathis/goetl/aggregate"
	"github.com/aaronlmathis/goetl/core"
	"github.com/aaronlmathis/goetl/dag/tasks"
)

// DAGBuilder provides a fluent API for constructing DAGs
type DAGBuilder struct {
	dag *DAG
}

// NewDAG creates a new DAG builder
func NewDAG(id, name string) *DAGBuilder {
	return &DAGBuilder{
		dag: &DAG{
			id:           id,
			name:         name,
			tasks:        make(map[string]tasks.Task),
			dependencies: make(map[string][]string),
			metadata: DAGMetadata{
				MaxParallelism: 4, // Sensible default
				DefaultTimeout: 30 * time.Minute,
			},
		},
	}
}

// AddSourceTask adds a data source task to the DAG
func (db *DAGBuilder) AddSourceTask(id string, source core.DataSource, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewSourceTask(id, source, opts...)
	db.dag.tasks[id] = task
	return db
}

// AddTransformTask adds a transformation task to the DAG
func (db *DAGBuilder) AddTransformTask(id string, transformer core.Transformer, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewTransformTask(id, transformer, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddFilterTask adds a filter task to the DAG
func (db *DAGBuilder) AddFilterTask(id string, filter core.Filter, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewFilterTask(id, filter, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddSinkTask adds a data sink task to the DAG
func (db *DAGBuilder) AddSinkTask(id string, sink core.DataSink, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewSinkTask(id, sink, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddAggregateTask adds an aggregation task to the DAG
func (db *DAGBuilder) AddAggregateTask(id string, aggregator aggregate.Aggregator, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewAggregateTask(id, aggregator, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddJoinTask adds a join operation task to the DAG
func (db *DAGBuilder) AddJoinTask(id string, config tasks.JoinConfig, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewJoinTask(id, config, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddCDCTask adds a change data capture task to the DAG
func (db *DAGBuilder) AddCDCTask(id string, config tasks.CDCConfig, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewCDCTask(id, config, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddSCDTask adds a slowly changing dimension task to the DAG
func (db *DAGBuilder) AddSCDTask(id string, config tasks.SCDConfig, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewSCDTask(id, config, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddConditionalTask adds a conditional execution task to the DAG
func (db *DAGBuilder) AddConditionalTask(id string, condition tasks.ConditionalLogic, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	task := tasks.NewConditionalTask(id, condition, dependencies, opts...)
	db.dag.tasks[id] = task
	db.dag.dependencies[id] = dependencies
	return db
}

// AddLookupTask adds a lookup/enrichment task to the DAG
func (db *DAGBuilder) AddLookupTask(id string, lookup interface{}, dependencies []string, opts ...tasks.TaskOption) *DAGBuilder {
	// For now, treat lookup as a specialized join task
	// In a full implementation, you'd create a dedicated LookupTask type
	return db.AddJoinTask(id, tasks.JoinConfig{
		JoinType: "left",
		Strategy: "hash",
	}, dependencies, opts...)
}

// WithMaxParallelism sets the maximum number of concurrent tasks
func (db *DAGBuilder) WithMaxParallelism(max int) *DAGBuilder {
	db.dag.metadata.MaxParallelism = max
	return db
}

// WithDefaultTimeout sets the default timeout for all tasks
func (db *DAGBuilder) WithDefaultTimeout(timeout time.Duration) *DAGBuilder {
	db.dag.metadata.DefaultTimeout = timeout
	return db
}

// WithGlobalContext sets global context available to all tasks
func (db *DAGBuilder) WithGlobalContext(ctx map[string]interface{}) *DAGBuilder {
	db.dag.metadata.GlobalContext = ctx
	return db
}

// validateDAG checks for cycles and missing dependencies
func (db *DAGBuilder) validateDAG() error {
	// Check for cycles using DFS
	if db.hasCycleDFS() {
		return fmt.Errorf("DAG contains cycles")
	}

	// Check for missing dependencies
	for taskID, deps := range db.dag.dependencies {
		for _, dep := range deps {
			if _, exists := db.dag.tasks[dep]; !exists {
				return fmt.Errorf("task %s depends on non-existent task %s", taskID, dep)
			}
		}
	}

	return nil
}

// hasCycleDFS performs depth-first search to detect cycles
func (db *DAGBuilder) hasCycleDFS() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for taskID := range db.dag.tasks {
		if !visited[taskID] {
			if db.dfsHasCycle(taskID, visited, recStack) {
				return true
			}
		}
	}

	return false
}

func (db *DAGBuilder) dfsHasCycle(taskID string, visited, recStack map[string]bool) bool {
	visited[taskID] = true
	recStack[taskID] = true

	// Check all dependencies
	for _, dep := range db.dag.dependencies[taskID] {
		if !visited[dep] {
			if db.dfsHasCycle(dep, visited, recStack) {
				return true
			}
		} else if recStack[dep] {
			fmt.Printf("CYCLE DETECTED: %s -> %s\n", taskID, dep)
			return true
		}
	}

	recStack[taskID] = false
	return false
}

// Build validates and returns the constructed DAG
func (db *DAGBuilder) Build() (*DAG, error) {
	if err := db.validateDAG(); err != nil {
		return nil, err
	}

	return db.dag, nil
}
