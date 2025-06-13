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
	"time"

	"github.com/aaronlmathis/goetl/dag/tasks"
)

const (
	TriggerAllSuccess  tasks.TriggerRule = "all_success"  // All dependencies succeeded
	TriggerAllComplete tasks.TriggerRule = "all_complete" // All dependencies completed (success or failure)
	TriggerOneFailed   tasks.TriggerRule = "one_failed"   // At least one dependency failed
	TriggerOneSuccess  tasks.TriggerRule = "one_success"  // At least one dependency succeeded
	TriggerNoneFailed  tasks.TriggerRule = "none_failed"  // No dependencies failed
)

// DAG represents a directed acyclic graph of tasks
type DAG struct {
	id           string
	name         string
	tasks        map[string]tasks.Task
	dependencies map[string][]string
	metadata     DAGMetadata
}

// DAGMetadata contains DAG-level configuration
type DAGMetadata struct {
	Description    string
	MaxParallelism int
	DefaultTimeout time.Duration
	DefaultRetries *tasks.RetryConfig
	GlobalContext  map[string]interface{}
}
