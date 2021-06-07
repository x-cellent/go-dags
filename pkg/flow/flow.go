package flow

import (
	"context"
	"errors"
	"fmt"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
	"strings"
)

// AlreadyExists indicates that a task with the given id already exists
var AlreadyExists = errors.New("taskID already exists")

// Workflow consists of a DAG that models the dependencies and
// associated Tasks for each node of the graph.
type Workflow struct {
	// DAG
	graph *simple.DirectedGraph
	// associated Tasks, key is nodeID
	tasks map[int64]*Task
}

// NewWorkflow creates a new workflow
func NewWorkflow() *Workflow {
	return &Workflow{
		graph: simple.NewDirectedGraph(),
		tasks: make(map[int64]*Task),
	}
}

// AddTasks adds the given tasks to this workflow
func (w *Workflow) AddTasks(tasks []*Task) error {
	for _, t := range tasks {
		if err := w.AddTask(t); err != nil {
			return err
		}
	}
	return nil
}

// AddTask adds the given task to this workflow
func (w *Workflow) AddTask(task *Task) error {
	_, ok := w.tasks[task.id]
	if ok {
		return AlreadyExists
	}

	w.tasks[task.id] = task

	taskNode := simple.Node(task.id)
	w.graph.AddNode(taskNode)

	return nil
}

// AddDependency adds one ore more dependencies from the given task to a number of other tasks
func (w *Workflow) AddDependency(task *Task, dependencies ...*Task) error {

	taskNode := w.graph.Node(task.id)
	if taskNode == nil {
		return fmt.Errorf("error adding task dependency for task id %d: node with id %d does not exist", task.id, task.id)
	}
	// pre-check depNodes so that we produce a consistent result or fail otherwise
	var depNodes []graph.Node
	for _, depTask := range dependencies {
		depNode := w.graph.Node(depTask.id)
		if depNode == nil {
			return fmt.Errorf("error adding task dependency from id %d to id %d: node with id %d does not exist", task.id, depTask.id, depTask.id)
		}
		depNodes = append(depNodes, depNode)
	}
	for _, depNode := range depNodes {
		// reverse direction of edge at insert, so that the topological sort returns the execution order
		edge := w.graph.NewEdge(depNode, taskNode)
		w.graph.SetEdge(edge)
	}
	return nil
}

// GetOrderedTasks returns the Tasks in executable order according to their dependencies
func (w *Workflow) GetOrderedTasks() ([]*Task, error) {
	// order topographically and lexically by id
	sortedIDs, err := topo.SortStabilized(w.graph, nil)
	if err != nil {
		return nil, err
	}

	var result []*Task
	for _, node := range sortedIDs {
		result = append(result, w.tasks[node.ID()])
	}
	return result, nil
}

// Reconcile executes the workflow tasks in order and returns nil, if all tasks completed successfully.
// If a FatalError is returned, the workflow failed and cannot be retried.
func (w *Workflow) Reconcile(ctx context.Context) error {
	tasks, err := w.GetOrderedTasks()
	if err != nil {
		return NewFatalError(err)
	}

	for _, task := range tasks {
		if cancelErr := ctx.Err(); cancelErr == nil {
			err := task.reconcileFn(ctx, task)
			// the workflow runs unless some task returns an error
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Visualize returns a string visualizing the sequence of tasks to be executed
func (w *Workflow) Visualize() (string, error) {
	tasks, err := w.GetOrderedTasks()
	if err != nil {
		return "", err
	}

	var result strings.Builder
	for i, t := range tasks {
		if i > 0 {
			result.WriteString(" >> ")
		}
		result.WriteString(t.String())
	}
	return result.String(), nil
}

// Fn is the reconcile function that executes the task's logic to achieve the desired outcome.
// If the task is successful, it returns nil.
// If the task returns a FatalError, it indicates that it failed and cannot be retried.
// If the task returns any other error, it failed but can be retried later.
type Fn func(ctx context.Context, task *Task) error

// FatalError indicates that the execution of the task encountered an error that is fatal and final, i.e. the task cannot be retried.
type FatalError struct {
	err error
}

// NewFatalError creates a new FatalError with the given error.
func NewFatalError(err error) FatalError {
	return FatalError{
		err: err,
	}
}

func (e FatalError) Error() string {
	if e.err == nil {
		return "unknown fatal error"
	}
	return fmt.Sprintf("fatal error: %v", e.err.Error())
}

// Task models a unit of work with dependencies to other tasks
type Task struct {
	id          int64
	desc        string
	deps        []int64
	reconcileFn Fn
}

// NewTask creates a new task specifying the id, description and reconcile function
func NewTask(id int64, desc string, fn Fn) *Task {
	task := &Task{
		id:          id,
		desc:        desc,
		reconcileFn: fn,
		deps:        []int64{},
	}
	return task
}

func (j *Task) String() string {
	return fmt.Sprintf("task %d (%s)", j.id, j.desc)
}
