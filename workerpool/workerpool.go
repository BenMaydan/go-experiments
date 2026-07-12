package workerpool

import (
	"sync"
	"time"
	"errors"
)

/*
New(maxWorkers int, options ...Option) *WorkerPool — constructs the pool with its channels/config and starts the dispatcher goroutine.
Size() int — returns the maximum number of concurrent workers.


Launched goroutines need to be called with the following things:
	Jobs channel (since an existing goroutine may do more than one job once it is finished)
	A timer (so an idle goroutine, i.e. not working for more than X seconds, shuts down)

*/


type job func()

type WorkerPoolOptions struct {
	NumInitialWorkers uint // 0 does not "warm-up" any workers
	MaxWorkers uint
	IdleTimeout time.Duration
	MaxJobQueueSize uint // 0 means unbounded job queue size
	ErrorHandlingHook func()
}

// Worker pool cannot be value-copied since it has a mutex inside!!
// Always pass by reference
type WorkerPool struct {
	numWorkers uint
	maxWorkers uint
	maxJobQueueSize uint
	wait bool

	// jobs isn't a Queue type because goroutines run in parallel
	// so there is no meaning to have an ordering of tasks
	submitCh chan job
	assignCh chan job
	queueJobs Queue[job]
	wg *sync.WaitGroup
}

// constructs the pool with its channels/config and starts the dispatcher goroutine
func InitWorkerPool(options *WorkerPoolOptions) (*WorkerPool, error) {
	if options.MaxWorkers == 0 {
		return nil, errors.New("cannot create a worker pool with 0 maximum workers")
	}
	if options.NumInitialWorkers > options.MaxWorkers {
		return nil, errors.New("num initial workers cannot be greater than max number of workers")
	}

	pool := &WorkerPool{
		maxWorkers: options.MaxWorkers,
		maxJobQueueSize: options.MaxJobQueueSize,
		running: true,
		// only the dispatcher touches the receiving end
		// do, submit, submitwait, pause can all send to it
		submitCh: make(chan job),
		// dispatcher is the only sender, any worker can be the receiver
		assignCh: make(chan job),
		queueJobs: Queue[job]{},
	}

	pool.wg = &sync.WaitGroup{}
	pool.wg.Add(int(options.NumInitialWorkers))
	pool.numWorkers = options.NumInitialWorkers
	for range options.NumInitialWorkers {
		go worker(func() {}, pool.assignCh, pool.wg)
	}

	// start the dispatcher
	// this is a goroutine that runs the schedule function
	pool.dispatch(options.IdleTimeout)

	return pool, nil
}


// returns the maximum number of concurrent workers
func (wp *WorkerPool) Size() uint {
	return wp.maxWorkers
}

// returns the current number of tasks sitting in the waiting queue
func (wp *WorkerPool) WaitingQueueSize() int {
	return wp.jobs.Size()
}

