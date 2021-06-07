package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/x-cellent/go-dags/pkg/flow"
	"log"
	"time"
)

func main() {
	w := flow.NewWorkflow()
	t1 := newTask(1, "create V1", 3)
	t2 := newTask(2, "create V2", 1)
	t3 := newTask(3, "create V3", 1)
	t4 := newTask(4, "create V4", 2)
	t5 := newTask(5, "create V5", 2)

	doOrDie(w.AddTasks([]*flow.Task{t1, t2, t3, t4, t5}))
	doOrDie(w.AddDependency(t1, t2))
	doOrDie(w.AddDependency(t1, t3))
	doOrDie(w.AddDependency(t1, t4))
	doOrDie(w.AddDependency(t1, t5))
	doOrDie(w.AddDependency(t3, t5))
	doOrDie(w.AddDependency(t4, t5))

	v, _ := w.Visualize()
	log.Println(v)

	ctx := context.Background()
	log.Printf("--- reconcile run 1 ---")
	err := w.Reconcile(ctx)
	for i := 2; err != nil; i++ {
		var fatalErr flow.FatalError
		if errors.As(err, &fatalErr) {
			log.Fatalf(fatalErr.Error())
		} else {
			log.Println(err)
		}
		// retry after some time
		time.Sleep(2 * time.Second)
		log.Printf("--- reconcile run %d ---", i)
		err = w.Reconcile(ctx)
	}
}

// newTask creates a new demotask with the specified properties
// simulatedTries is the number ob reconciliation attempts before success, e.g. 1 = immediate success
func newTask(id int64, desc string, simulatedTries int) *flow.Task {
	return flow.NewTask(id, desc, (&demoTask{
		retries: simulatedTries,
	}).do)
}

type demoTask struct {
	// retries until this tasks succeeds to simulate retries
	retries int
	// task completes successfully
	success bool
}

// do is the flow.Fn for out demo tasks
func (t *demoTask) do(ctx context.Context, task *flow.Task) error {
	// "reconcile": check if this task is already done, i.e. if desired state is already present
	if t.success {
		log.Printf("reconcile %s ok\n", task.String())
		return nil
	}

	// no, try to execute the task and reach desired state
	if t.retries--; t.retries > 0 {
		return fmt.Errorf("reconce %s error, %d remain", task.String(), t.retries)
	}

	// ok, we made it, mark task as successful
	log.Printf("reconcile %s success\n", task.String())
	t.success = true
	return nil
}

func doOrDie(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
