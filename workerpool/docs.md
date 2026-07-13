# workerpool

A bounded-concurrency worker pool: submit tasks without blocking, and a fixed
maximum number of goroutines execute them. Idle workers are shut down
gradually; new workers are spawned on demand up to the configured maximum.

## Construction

### `InitWorkerPool(options *WorkerPoolOptions) (*WorkerPool, error)`
Constructs the pool with its internal channels and configuration, then starts
the dispatcher goroutine. Potentially returns an error for invalid options.

## Introspection

### `Size() int`
Returns the maximum number of concurrent workers the pool was configured
with.

### `Stopped() bool`
Reports whether the pool has been stopped (via `Stop`, `StopWait`, or
`StopAbandon`).

### `WaitingQueueSize() int`
Returns the current number of tasks sitting in the waiting queue.

## Submitting work

### `Do(task func()) error`
Submits a task, returning `ErrStopped` instead of panicking if the pool has
already been stopped.

### `Submit(task func())`
Submits a task, panicking if the pool has already been stopped.

### `SubmitWait(task func())`
Submits a task and blocks the caller until that specific task has finished
executing.

## Pausing

### `Pause(ctx context.Context)`
Blocks all workers from picking up new tasks until the given context is
done, and does not return to the caller until every worker has actually
reached the paused/waiting state.

## Stopping

### `StopAbandon()`
Stops the pool and waits only for currently running tasks to finish;
queued-but-not-yet-running tasks are abandoned.

### `Stop()`
Stops the pool and waits for every queued task to run to completion before
returning.

### Stop escalation
If one goroutine has already called `Stop` and, before it
completes, another goroutine calls `StopAbandon`, the in-flight stop is
escalated: the abandon flag is set, and the queue-draining logic checks that
flag continuously so it can bail out and return early instead of finishing
the drain.

## Internal (dispatcher-side)

### `dispatch(idleTimeout time.Duration)`
Core loop that routes tasks to workers, spawns/kills workers based on load,
and manages the waiting queue until told to stop.

### `worker(task func(), workerQueue chan func(), wg *sync.WaitGroup, errHandler func())`
Runs tasks in a loop, pulling the next task from `workerQueue` after each
one, and exits when it receives a `nil` task. Includes an error handler for a user defined way to recover from panicking tasks. The default error handler repropagates the panic.

### `stop(abandon bool)`
Shared internal implementation behind `Stop`/`StopAbandon`: signals
shutdown, records the abandon intent (escalating if a stop is already
in progress), and blocks until all the workers finished executing.

### `processWaitingQueue() bool`
Moves one task between the incoming task queue, the waiting queue, and an
available worker; returns `false` once the pool is stopped.

### `killIdleWorker() bool`
Sends a kill signal to a worker if one is currently idle and ready to
receive it.

### `runQueuedTasks()`
Drains the waiting queue by handing each remaining task to a worker, used
during `StopWait`; exits early if the abandon flag becomes set.
