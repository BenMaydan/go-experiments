package workerpool

import (
	"context"
	"sync"
	"time"
)

// submits a task, returning an error instead of panicking if the pool is stopped
func (wp *WorkerPool) Do(task job) (err error) {
	// wp.stopSignal channel is the source of truth on the pool being stopped
	// to prioritize receiving on stopSignal we need a dual select
	select {
	case wp.submitCh <- task:
	case <-wp.stopSignal:
		err = ErrorStopped{}
	}

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
	// we are required to use defer since the task might internally panic (and be recovered)
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
	if wp.Stopped() {
		return
	}

	ready := &sync.WaitGroup{}
	ready.Add(int(wp.maxWorkers))

	for range wp.maxWorkers {
		wp.Submit(func() {
			ready.Done()
			select {
			case <-ctx.Done():
			case <-wp.stopSignal:
			}
		})
	}

	// block until all workers are currently waiting (sent their state on the channel)
	ready.Wait()
}

// stops the pool but first runs all queued tasks to completion
func (wp *WorkerPool) Stop() {
	wp.stop(false)
}

// stops the pool, abandoning any not-yet-running queued tasks
func (wp *WorkerPool) StopAbandon() {
	wp.stop(true)
}

// reports whether the pool has been stopped.
func (wp *WorkerPool) Stopped() bool {
	wp.stopLock.Lock()
	defer wp.stopLock.Unlock()
	return wp.stopped
}

// internal shared implementation for Stop/StopAbandon that signals shutdown and waits for all workers to finish
func (wp *WorkerPool) stop(abandon bool) {
	// this allows goroutines to escalate the abandon flag
	if abandon {
		wp.escalateAbandon()
	}

	wp.stopLock.Lock()
	if !wp.stopped {
		wp.stopped = true
		// send a request to the dispatcher to stop the pool
		wp.stopRequest <- struct{}{}
	}
	wp.stopLock.Unlock()

	// it's important to make the distinction that this only waits on currently running work to complete
	// not queued tasks. so both Stop and StopAbandon need to wait here.
	<-wp.finishedAllWork
}

// escalateAbandon idempotently signals that queued-but-not-running tasks
// should be dropped. Safe to call from any goroutine, including synchronously
// from within a task the pool itself is currently running — unlike
// StopAbandon, it never blocks.
func (wp *WorkerPool) escalateAbandon() {
	wp.abandonOnce.Do(func() {
		close(wp.abandonSignal)
	})
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
func worker(task job, workQueue <-chan job, wg *sync.WaitGroup) {
	defer wg.Done()

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
	case <-wp.stopRequest:
		return false
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

		// Racing the handoff against abandonSignal rather than checking a
		// flag once and then committing to an uninterruptible send means
		// abandonment can still cancel a job that's already parked here
		// waiting for a worker.
		// I can only minimally prioritize abandoning but there is still a (by design) unfixable race condition
		select {
		case <-wp.abandonSignal:
			return
		default:
			select {
			case wp.assignCh <- queuedJob:
			case <-wp.abandonSignal:
				return
			}
		}
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
	// it is safer to defer this
	// signals to the caller that requested stopping to unblock, it means all workers are done
	defer close(wp.finishedAllWork)

	timer := time.NewTimer(idleTimeoutDuration)
	idle := false
	stopped := false

Loop:
	for {
		if wp.queueJobs.Size() != 0 {
			if !wp.processWaitingQueue() {
				stopped = true
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
		case job := <-wp.submitCh:
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
		case <-wp.stopRequest:
			// pool was requested to be stopped, so exit
			stopped = true
			break Loop
		}
	}

	if stopped {
		close(wp.stopSignal)
	}

	wp.runQueuedTasks()

	for range wp.numWorkers {
		wp.assignCh <- nil
	}
	wp.numWorkers = 0

	// we can be certain no more workers can be added/running so we can wait on the wait group
	// workers call wg.Done once their for loop breaks, which is when they receive a nil task
	wp.wg.Wait()
}
