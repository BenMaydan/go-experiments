package workerpool

import (
	"sync"
	"time"
	"context"
)

/*
worker(task func(), workerQueue chan func(), wg *sync.WaitGroup) — runs tasks in a loop until it receives a nil task, then exits.
stop(wait bool) — internal shared implementation for Stop/StopWait that signals shutdown and waits for the dispatcher to finish.
processWaitingQueue() bool — moves one task between the incoming queue, waiting queue, and an available worker.
killIdleWorker() bool — sends a kill signal to a worker if one is currently idle and ready.
runQueuedTasks() — drains the waiting queue by handing every remaining task to workers before shutdown.


The worker function requires a sync.WaitGroup because:
	the dispatcher — or whoever calls StopWait/Stop — needs a way to know "all worker goroutines have actually exited", and channels alone don't give you that for free.
	Here's the gap: sending nil down a worker's channel tells that worker "you should exit,"
	but it doesn't tell you (the caller) when the worker has actually finished exiting.
	The send returns as soon as the worker receives the value — not when the worker goroutine has run its cleanup and returned.
	If Stop/StopWait returns right after firing off nil to every worker, you have no guarantee the goroutines are actually gone yet; they might still be mid-teardown.
*/

// submits a task, returning an error instead of panicking if the pool is stopped
func (wp *WorkerPool) Do(task job) error {
	return nil
}

// submits a task, panicking if the pool is stopped
func (wp *WorkerPool) Submit(task job) {
	if !wp.running {
		panic("cannot submit task on stopped pool")
	}
}

// submits a task and blocks until that specific task has finished executing
func (wp *WorkerPool) SubmitWait(task job) {

}

// blocks all workers from taking new tasks until the given context is done
func (wp *WorkerPool) Pause(ctx context.Context) {

}

// stops the pool, abandoning any not-yet-running queued tasks
func (wp *WorkerPool) Stop() {
	wp.stop(false)
}

// stops the pool but first runs all queued tasks to completion
func (wp *WorkerPool) StopWait() {
	wp.stop(true)
}

// reports whether the pool has been stopped
func (wp *WorkerPool) Stopped() bool {
	return wp.stopped
}

/*
runs tasks in a loop until it receives a nil task, then exits

The worker function requires a sync.WaitGroup because:
	the dispatcher — or whoever calls StopWait/Stop — needs a way to know "all worker goroutines have actually exited", and channels alone don't give you that for free.
	Here's the gap: sending nil down a worker's channel tells that worker "you should exit,"
	but it doesn't tell you (the caller) when the worker has actually finished exiting.
	The send returns as soon as the worker receives the value — not when the worker goroutine has run its cleanup and returned.
	If Stop/StopWait returns right after firing off nil to every worker, you have no guarantee the goroutines are actually gone yet; they might still be mid-teardown.
*/
func worker(task job, workQueue <-chan job, wg *sync.WaitGroup) {
	defer wg.Done()
	for task != nil {
		task()
		task = <-workQueue
	}
}

// internal shared implementation for Stop/StopWait that signals shutdown and waits for the dispatcher to finish
func (wp *WorkerPool) stop(wait bool) {
	// the dispatch function breaks out of the loop when the submit channel is closed
	// we need to set wp.wait = wait and close the channel
	// to avoid a race condition, setting wait is required to be first
	// 		this is only since the dispatcher runs on a different goroutine
	wp.stopped = true
	wp.wait = wait
	close(wp.submitCh)
}

// moves one task between the submit channel, assign channel, and an available worker.
// Return: a boolean for whether the pool is still open or not
func (wp *WorkerPool) processWaitingQueue() bool {
	job, err := wp.queueJobs.Peek()
	if err != nil { panic("empty queue should not be possible") }

	select {
	case wp.assignCh <- job:
		wp.queueJobs.Pop()
	case incomingJob, status := <-wp.submitCh:
		if !status { return status }
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
		if err != nil { break }

		// critically, someone will always be able to receive this job
		// not all the workers can be idle and thus have been killed off
		// 		because that means there wouldn't be any more queued jobs
		// so if there exists a queued job, there must be at least one worker ready to handle it
		wp.assignCh <- queuedJob
	}
}

// the core loop that
// 	 routes tasks to workers
//	 spawns/kills workers
// 	 manages the waiting queue
// important distinction: when the idle timeout fires, at most one worker is killed
func (wp *WorkerPool) dispatch(idleTimeoutDuration time.Duration) {
	timer := time.NewTimer(idleTimeoutDuration)
	idle := false

Loop:
	for {
		if wp.queueJobs.Size() != 0 {
			if !wp.processWaitingQueue() { break Loop }
    		continue
		}

		select {
		case <-timer.C:
			if idle { wp.killIdleWorker() }
			timer.Reset(idleTimeoutDuration)
			idle = true
			continue
		case job, open := <-wp.submitCh:
			// if the submit channel is closed that means the pool was stopped
			if !open { break Loop }
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
					go worker(job, wp.assignCh, wp.wg)
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
	if wp.wait {
		wp.runQueuedTasks()
	}

	for range wp.numWorkers {
		wp.assignCh <- nil
	}
}