package common

import "context"

type pool[I, O any] struct {
	ctx    context.Context
	input  chan I             // Input chan, one per pool, insert task into it.
	output chan O             // Output chan, one per pool, will be filled with job outputs.
	ready  chan *worker[I, O] // Channel for readied workers, one per pool.
	err    chan error         // Chanel to receive error, one per pool.
}

type worker[I, O any] struct {
	ctx      context.Context
	workFunc func(context.Context, I) (O, error) // Function for worker to execute on input.
	p        *pool[I, O]                         // Points back to pool.
	tasks    chan I                              // worker's task, one per worker.
}

func NewPool[I, O any](ctx context.Context, workerCount int, workFunc func(context.Context, I) (O, error)) (input chan I, output chan O, err chan error) {
	// Create new pool.
	p := &pool[I, O]{
		ctx:    ctx,
		input:  make(chan I, workerCount),
		output: make(chan O, workerCount),
		ready:  make(chan *worker[I, O], workerCount),
		err:    make(chan error),
	}
	// Initialize workers and put them on p's ready list.
	for i := 0; i < workerCount; i++ {
		w := &worker[I, O]{
			ctx:      ctx,
			workFunc: workFunc,
			p:        p,
			tasks:    make(chan I),
		}
		p.ready <- w
		go w.work()
	}
	// Start dispatching job to workers.
	go p.dispatch()
	return p.input, p.output, p.err
}

func (p *pool[I, O]) dispatch() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case newTask := <-p.input:
			// New task input, get a free worker from ready chan, and sent task to it.
			w := <-p.ready
			w.tasks <- newTask
		}
	}
}

func (w *worker[I, O]) work() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case newTask := <-w.tasks:
			// Upon new tasks arrive, execute the task and put w on p's ready list.
			result, err := w.workFunc(w.ctx, newTask)
			if err != nil {
				w.p.err <- err
			} else {
				w.p.output <- result
			}
			w.p.ready <- w
		}
	}
}
