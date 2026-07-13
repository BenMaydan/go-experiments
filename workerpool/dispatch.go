package workerpool

import (
	"context"
	"sync"
	"time"
)

// submits a task, returning an error instead of panicking if the pool is stopped
func (wp *WorkerPool) Do(task job) (err error) {
	defer func() {
        if r := recover(); r != nil {
            err = &ErrorStopped{}
        }
    }()

	// if this panics (because the pool was stopped), then the deferred function will recover and send an error
	wp.submitCh <- task
	
	return
}

// submits a task, panicking if the pool is stopped
func (wp *WorkerPool) Submit(task job) {
	err := wp.Do(task)
	if err != nil {
		panic(err)
	}
}

// submits a task and blocks until that specific task has finished executing
// When this function returns to the caller, it's guaranteed that the task finished running
func (wp *WorkerPool) SubmitWait(task job) {
	// We achieve this by forcing SubmitWait to wait on a new channel receiving a done signal
	// the task wraps itself into a wrapper task which sends on c when it's completed
	// we are required to use defer since the task might panic (and be recovered)
	c := make(chan struct{})
	wrapperTask := func() {
		defer func() { c <- struct{}{} }()
		task()
	}

	// what happens if stop is called? it closes submitCh
	wp.Submit(wrapperTask)
	<-c
}

// blocks all workers from taking new tasks, and does not return until every worker has confirmed it's actually parked waiting on the context.
func (wp *WorkerPool) Pause(ctx context.Context) {
	// to prevent races between two goroutines calling Pause
	wp.stopLock.Lock()
	defer wp.stopLock.Unlock()

	// if the pool is already stopped then we cannot pause
	if wp.stopped { return }

	waitingCh := make(chan struct{})
	blockingTask := func() {
		waitingCh <- struct{}{}
		select {
		case <-ctx.Done():
		case <-wp.stopSignal:
		}
	}

	for range wp.maxWorkers {
		wp.Submit(blockingTask)
	}

	// block until all workers are currently waiting (sent their state on the channel)
	for range wp.maxWorkers {
		select {
		case <-waitingCh:
		case <-wp.stopSignal:
		}
	}
}

// stops the pool but first runs all queued tasks to completion
func (wp *WorkerPool) Stop() {
	wp.stop(false)
}

// stops the pool, abandoning any not-yet-running queued tasks
func (wp *WorkerPool) StopAbandon() {
	wp.stop(true)
}

// reports whether the pool has been stopped
func (wp *WorkerPool) Stopped() bool {
	wp.stopLock.Lock()
	stopped := wp.stopped
	wp.stopLock.Unlock()
	return stopped
}

// internal shared implementation for Stop/StopAbandon that signals shutdown and waits for all workers to finish
func (wp *WorkerPool) stop(abandon bool) {
	// this allows goroutines to escalate the abandon flag
	if abandon { wp.abandon.Store(true) }

	wp.stopLock.Lock()
	defer wp.stopLock.Unlock()

	// closing the channel only happens once because of this check
	if wp.stopped { return }

	wp.stopped = true
	close(wp.stopSignal)
	close(wp.submitCh)

	// to wait on workers to finish we need to block on the wait group the workers are using
	wp.wg.Wait()
}

/*
runs tasks in a loop until it receives a nil task, then exits

The worker function requires a sync.WaitGroup because:

	the dispatcher — or whoever calls StopAbandon/Stop — needs a way to know "all worker goroutines have actually exited", and channels alone don't give you that for free.
	Here's the gap: sending nil down a worker's channel tells that worker "you should exit,"
	but it doesn't tell you (the caller) when the worker has actually finished exiting.
	The send returns as soon as the worker receives the value — not when the worker goroutine has run its cleanup and returned.
	If Stop/StopAbandon returns right after firing off nil to every worker, you have no guarantee the goroutines are actually gone yet; they might still be mid-teardown.
*/
func worker(task job, workQueue <-chan job, wg *sync.WaitGroup, errHandler func(err any)) {
	defer wg.Done()
	// provides user-assisted way to recover from a panicking worker/task
	defer func() {
		if r := recover(); r != nil {
            errHandler(r)
        }
	}()

	for task != nil {
		task()
		task = <-workQueue
	}
}

// moves one task between the submit channel, assign channel, and an available worker.
// Return: a boolean for whether the pool is still open or not
func (wp *WorkerPool) processWaitingQueue() bool {
	job, err := wp.queueJobs.Peek()
	if err != nil {
		panic("empty queue should not be possible")
	}

	select {
	case wp.assignCh <- job:
		wp.queueJobs.Pop()
	case incomingJob, status := <-wp.submitCh:
		if !status {
			return status
		}
		wp.queueJobs.Add(incomingJob)
	}

	return true
}

// sends a kill signal to a worker if one is currently idle and ready
func (wp *WorkerPool) killIdleWorker() bool {
	select {
	case wp.assignCh <- nil:
		wp.numWorkers--
		return true
	default:
		return false
	}
}

// drains the waiting queue by handing every remaining task to workers before shutdown
func (wp *WorkerPool) runQueuedTasks() {
	for {
		queuedJob, err := wp.queueJobs.Pop()
		if err != nil {
			break
		}

		// to prevent extra side effects from tasks being created
		// two separate goroutines can call Stop and StopAbandon
		// StopAbandon has priority so the abandon flag can theoretically
		// change in between this loop
		if wp.abandon.Load() { return }

		// critically, someone will always be able to receive this job
		// not all the workers can be idle and thus have been killed off
		// 		because that means there wouldn't be any more queued jobs
		// so if there exists a queued job, there must be at least one worker ready to handle it
		wp.assignCh <- queuedJob
	}
}

// the core loop that
//
//	routes tasks to workers
//	spawns/kills workers
//	manages the waiting queue
//
// important distinction: when the idle timeout fires, at most one worker is killed
func (wp *WorkerPool) dispatch(idleTimeoutDuration time.Duration) {
	timer := time.NewTimer(idleTimeoutDuration)
	idle := false

Loop:
	for {
		if wp.queueJobs.Size() != 0 {
			if !wp.processWaitingQueue() {
				break Loop
			}
			continue
		}

		select {
		case <-timer.C:
			if idle {
				wp.killIdleWorker()
			}
			timer.Reset(idleTimeoutDuration)
			idle = true
			continue
		case job, open := <-wp.submitCh:
			// if the submit channel is closed that means the pool was stopped
			if !open {
				break Loop
			}
			idle = false

			select {
			// we first attempt a non blocking handoff to any worker
			case wp.assignCh <- job:
			default:
				// Has to be true: all available workers are busy
				if wp.numWorkers < wp.maxWorkers {
					// if we can spawn a new worker, we do
					wp.wg.Add(1)
					wp.numWorkers++
					go worker(job, wp.assignCh, wp.wg, wp.errHandler)
				} else {
					// It could also be that we are at the maximum number of workers
					// so we are forced to queue up the job -- this holds the invariant
					// that a non empty queue means no more workers can be spawned
					wp.queueJobs.Add(job)
				}
			}
		}
	}

	// when the pool is stopped the wait boolean can be set to true
	// in which case all remaining tasks will be run before the pool is shut down
	if !wp.abandon.Load() {
		wp.runQueuedTasks()
	}

	for range wp.numWorkers {
		wp.assignCh <- nil
	}
}
