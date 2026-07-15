package workerpool

import (
	"context"
	"go.uber.org/goleak"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitWithTimeout turns "test hangs forever on deadlock" into
// "test fails after N with a clear message" — principle #5 above.
func waitWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for tasks to complete — possible deadlock")
	}
}

func eventually(t *testing.T, cond func() bool, timeout, tick time.Duration) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if cond() {
            return
        }
        time.Sleep(tick)
    }
    t.Fatal("condition not met within timeout")
}

// TestBasicSubmission verifies every submitted task actually runs exactly once.
func TestBasicSubmission(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}
	defer pool.Stop()

	const numTasks = 100
	var completed int32   // atomic counter: how many tasks actually ran
	var wg sync.WaitGroup // lets the test block until all tasks are done
	wg.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		pool.Submit(func() {
			atomic.AddInt32(&completed, 1)
			wg.Done()
		})
	}

	// Don't just check `completed` immediately — the tasks are running
	// concurrently and may not have finished yet. Wait for the signal.
	waitWithTimeout(t, &wg, 2*time.Second)

	if got := atomic.LoadInt32(&completed); got != numTasks {
		t.Fatalf("expected %d completed tasks, got %d", numTasks, got)
	}
}

// TestInternalWaitGroupRace verifies a single race condition, that submitting a task and immediately calling stop doesn't race on adding a worker and waiting for the worker to finish
func TestInternalWaitGroupRace(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	pool.Submit(func() {})
	pool.Stop()
}

// TestConcurrentStopCalls makes sure if multiple concurrent callers call Stop they both observe the function returning when the work has completed
func TestConcurrentStopCalls(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	// to wait on all callers of Stop to complete
	n := 3
	taskBlockWg := &sync.WaitGroup{}
	taskBlockWg.Add(n)
	wg := &sync.WaitGroup{}
	wg.Add(n)

	completed := &atomic.Bool{}
	task := func() {
		taskBlockWg.Wait()
		time.Sleep(200 * time.Millisecond)
		completed.Store(true)
	}

	concurrentCaller := func(goroutineNumber int) {
		defer wg.Done()
		// now let task wait on all stoppers to be in the same place before continuing
		taskBlockWg.Done()
		pool.Stop()

		if !completed.Load() {
			t.Errorf("Goroutine %v expected work to complete after stopping the pool but it did not.", goroutineNumber)
		}
	}

	pool.Submit(task)
	for i := range n {
		go concurrentCaller(i)
	}

	wg.Wait()
}

// TestConcurrentSubmission: many goroutines calling Submit at once.
// Hint: launch N goroutines, each submitting M tasks, use the same
// atomic-counter + WaitGroup pattern as above to verify N*M tasks ran.
// Self-check: does this test rely on submission ORDER anywhere? It shouldn't.
func TestConcurrentSubmission(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}
	defer pool.Stop()

	n := 42
	m := 69
	wg := &sync.WaitGroup{}

	wg.Add(n * m)

	completed := atomic.Int32{}

	subTask := func() {
		completed.Add(1)
		wg.Done()
	}

	parentTask := func() {
		for range m {
			go pool.Submit(subTask)
		}
	}

	for range n {
		go parentTask()
	}

	waitWithTimeout(t, wg, 2*time.Second)

	if completed.Load() != int32(n*m) {
		t.FailNow()
	}
}

// TestMaxWorkersNeverExceeded: the pool must never run more than
// MaxWorkers tasks concurrently, even under heavy/bursty load.
// Hint: inside each task, atomic.AddInt32(&current, 1), record the max
// seen with a CAS loop (or just a mutex-protected running max), then
// atomic.AddInt32(&current, -1) before returning. Make each task sleep
// briefly (e.g. 10ms) so tasks actually overlap in time.
// Self-check: would this test still pass if MaxWorkers were silently
// ignored and the pool spawned unlimited goroutines? If yes, it's not
// actually testing the bound.
func TestMaxWorkersNeverExceeded(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}
	defer pool.Stop()

	n := 32
	wg := &sync.WaitGroup{}
	completed := atomic.Int32{}
	current := atomic.Int32{}
	maxSeen := 0
	maxSeenLock := sync.Mutex{}

	task := func() {
		maxSeenLock.Lock()
		current.Add(1)
		maxSeen = max(maxSeen, int(current.Load()))
		maxSeenLock.Unlock()

		time.Sleep(10 * time.Millisecond)

		current.Add(-1)
		completed.Add(1)
		wg.Done()
	}

	wg.Add(n)
	for range n {
		pool.Submit(task)
	}
	waitWithTimeout(t, wg, 2*time.Second)

	if maxSeen > int(pool.maxWorkers) {
		t.Fatalf("Max seen goroutines: {%v} is greater than allowable {%v} goroutines", maxSeen, pool.maxWorkers)
	}

	if int(completed.Load()) != n {
		t.Fatalf("Submitted %v goroutines but only %v completed", n, completed.Load())
	}

	if current.Load() != 0 {
		t.Fatalf("%v goroutines should have finished but there is/are still %v running", n, current.Load())
	}
}

// TestSubmitWaitBlocksUntilDone: SubmitWait must not return before the
// task it submitted has actually finished executing.
// Hint: have the task sleep for a short, known duration and set a flag
// (or record a timestamp) right before returning. After SubmitWait
// returns, assert the flag is set / enough time has elapsed.
// Self-check: is there any timing assumption weaker than "the flag must
// be true"? A timing-based assertion (elapsed >= duration) is more prone
// to flakiness than a simple boolean check — prefer the boolean.
func TestSubmitWaitBlocksUntilDone(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}
	defer pool.Stop()

	completed := &atomic.Bool{}
	duration := 50 * time.Millisecond

	task := func() {
		time.Sleep(duration)
		completed.Store(true)
	}

	start := time.Now()
	pool.SubmitWait(task)
	elapsed := time.Since(start)

	// we assert that enough time has passed and the flag is true
	// we don't use a wait group but SubmitWait should wait for us
	if elapsed < duration {
		t.Fatalf("submit wait should have returned after %v time but instead returned after %v time", duration, elapsed)
	}

	if !completed.Load() {
		t.Fatalf("submit wait returned before the task marked the completed flag as true")
	}
}

// TestDoReturnsErrorAfterStop: after Stop(), Do() should return
// *ErrorStopped instead of panicking.
// Hint: Stop() the pool first, then call Do() and inspect the error
// with errors.As.
func TestDoReturnsErrorAfterStop(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("calling Do() after Stop() panicked instead of erroring")
		}
	}()

	pool.Stop()
	err = pool.Do(func() {})

	if !errors.Is(err, ErrorStopped{}) {
		t.Fatalf("calling Do() after Stop() should have returned an ErrorStopped instance")
	}
}

// TestSubmitPanicsAfterStop: after Stop(), Submit() should panic.
// Hint: recover() inside a deferred func, check that a panic occurred
// and that it wraps *ErrorStopped.
func TestSubmitPanicsAfterStop(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  4,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected Submit() to panic after Stop(), but it did not")
		} else {
			_, ok := r.(ErrorStopped)
			if !ok {
				t.Fatalf("calling Do() after Submit() should wrap a panic with an instance of ErrorStopped, instead got %T", r)
			}
		}
	}()

	pool.Stop()
	pool.Submit(func() {})
}

// TestStopRunsQueuedTasks: Stop() must let already-queued tasks finish
// before returning, even ones that haven't started yet.
// Hint: use a small MaxWorkers (e.g. 1) and submit several tasks that
// each take a moment, so some are guaranteed to still be queued when
// Stop() is called from another goroutine. Assert all of them ran.
func TestStopRunsQueuedTasks(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  1,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	completed := atomic.Bool{}

	// we run two tasks, one that waits on the pool to stop (forcing the second to stay queued)
	// and a second task which sets completed to true
	// this way only if completed is true then we know that queued tasks ran after calling Stop()
	blockingTask := func() {
		<-pool.stopSignal
	}
	nonBlockingTask := func() {
		completed.Store(true)
	}
	
	pool.Submit(blockingTask)
	pool.Submit(nonBlockingTask)

	pool.Stop()
	if !completed.Load() {
		t.Fatalf("stopping pool after submitting tasks did not let queued task run to completion")
	}
}

// TestStopAbandonSkipsQueuedTasks: StopAbandon() must NOT run tasks that
// were still sitting in the queue (only currently-running ones finish).
// Hint: same setup as above but call StopAbandon(); assert the completed
// count is less than the submitted count.
// Self-check: this test is inherently about a race between submission
// and shutdown — how do you make it *reliably* reproduce the abandon
// case rather than getting lucky? (Think: block workers deliberately so
// tasks are forced to queue.)
func TestStopAbandonSkipsQueuedTasks(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  1,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	// can I reliably reproduce behavior with only two tasks?
	// one task I want to force to run, and one should somehow remain on the queue
	//		the caveat is the one that is forced to run should run AT LEAST until the caller calls StopAbandon
	shouldComplete := &atomic.Bool{}
	shouldNotComplete := &atomic.Bool{}
	taskToComplete := func() {
		shouldComplete.Store(true)
		// the last thing that happens after stopSignal is closed in the internal stop function is that it waits for all the workers to complete
		// so if this task blocks on receiving from stopSignal (which immediately unblocks when the channel is closed), that forces the second
		// worker to only start once we know that we have started to abandon tasks
		<-pool.stopSignal
	}
	taskToNotComplete := func() {
		shouldNotComplete.Store(true)
	}
	pool.Submit(taskToComplete)
	pool.Submit(taskToNotComplete)

	// replacing StopAbandon with Stop fails the test so clearly waiting on <-pool.stopSignal works
	pool.StopAbandon()
	if !shouldComplete.Load() {
		t.Fatal("the task that should have been unqueued and completed did not complete")
	}
	if shouldNotComplete.Load() {
		t.Fatal("a queued task ran when it shouldn't have")
	}
}

// TestStopEscalation: per docs.md, if Stop() is in progress and another
// goroutine calls StopAbandon(), the abandon flag should escalate and
// the in-flight Stop() should return early rather than draining the
// full queue.
// Hint: you'll need a way to keep Stop()'s drain loop running long
// enough for the second goroutine to call StopAbandon() before it
// finishes — e.g. queue many slow tasks and only Add 1 initial worker.
func TestStopEscalation(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  1,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	// we will have three tasks, all three are submitted after the other
	// the first task runs and calls Stop on the pool
	//		it unfortunately needs to sleep for a minimum amount of time
	//		this is just to make sure the second two tasks are queued
	//		it's not possible to be certain they are queued with channels
	// the second task (is queued) runs and adds one to queuedTasksCompleted
	//		importantly, it calls StopAbandon, which should hopefully prevent task three from ever running if abandon really abandons
	// the third task (is queued) adds one to queuedTasksCompleted

	queuedTasksCompleted := &atomic.Int32{}
	stopCompleted := make(chan struct{})
	
	taskStopper := func() {
		time.Sleep(50 * time.Millisecond)
		// test that there are two queued tasks, otherwise rest of test doesn't make sense
		if pool.queueJobs.Size() != 2 {
			t.Errorf("instead of 2 tasks being queued, only %v are", pool.queueJobs.Size())
		}
		go func() {
			pool.Stop()
			stopCompleted <- struct{}{}
		}()
	}
	taskQueuedOne := func() {
		queuedTasksCompleted.Add(1)
		// setting the flag directly (rather than go pool.StopAbandon()) keeps this
		// synchronous with the rest of taskQueuedOne, so there's no race between this
		// write and the worker looping back to pick up taskQueuedTwo. Calling
		// StopAbandon() itself here would also deadlock: it blocks on
		// <-wp.finishedAllWork, which can't close until this very worker exits.
		pool.abandon.Store(true)
	}
	taskQueuedTwo := func() {
		queuedTasksCompleted.Add(1)
	}

	pool.Submit(taskStopper)
	pool.Submit(taskQueuedOne)
	pool.Submit(taskQueuedTwo)

	// wait until the stopper task completed
	<-stopCompleted

	if queuedTasksCompleted.Load() != 1 {
		t.Fatalf("%v queued tasks completed when only 1 should have completed", queuedTasksCompleted.Load())
	}
}

// TestPauseBlocksNewTasks: after Pause(ctx) returns, the pool guarantees
// all workers are already in the paused state — that's your sync point,
// not a sleep. Submit a task AFTER Pause returns, then prove it doesn't
// run within some bounded window. Cancel ctx, then prove it does run.
// Use a channel the task closes/sends on, and select against time.After
// for both the "shouldn't happen yet" and "should happen now" checks.
func TestPauseBlocksNewTasks(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  1,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}


	completed := &atomic.Int32{}
	cancelCtx, cancel := context.WithCancel(context.Background())
	pool.Pause(cancelCtx)

	ran := make(chan struct{})
	pool.Submit(func() { completed.Add(1); close(ran) })

	// Deterministic-ish: confirm it's parked in the queue, not dispatched.
	eventually(t, func() bool {
		return pool.WaitingQueueSize() == 1
	}, 100*time.Millisecond, time.Millisecond)

	// Now confirm it stays there / hasn't run — this part is still an absence
	// claim, but now backed up by the queue-size fact above rather than being
	// the only evidence.
	select {
	case <-ran:
		t.Fatal("task ran while pool was paused")
	default:
	}

	cancel()
	<-ran

	if completed.Load() != 1 {
		t.Fatal("queued task had context cancelled but did not run")
	}
}

// test no goroutines leak after every test
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestNoGoroutineLeaks: after Stop(), no worker/dispatcher goroutines
// should remain running.
// Then every test that creates a pool must fully Stop() it before
// returning, or this will fail at the end of the whole test binary run.
func TestNoGoroutineLeaks(t *testing.T) {
	pool, err := InitWorkerPool(&WorkerPoolOptions{
		MaxWorkers:  32,
		IdleTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWorkerPool failed: %v", err)
	}

	completed := &atomic.Int32{}

	n := 1000
	for range n {
		pool.Submit(func() {
			time.Sleep(50 * time.Millisecond)
			completed.Add(1)
		})
	}

	pool.Stop()

	if completed.Load() != int32(n) {
		t.Fatalf("%v completed instead of the required %v", completed.Load(), n)
	}
}

// ---------------------------------------------------------------------
// Bonus, in queue_test.go: Queue[T] has its own mutex and is used
// concurrently from Add/Pop/Peek/Size. Worth its own concurrent
// stress test — many goroutines Add-ing and Pop-ing simultaneously
// with -race on, asserting Size() never goes negative and no element
// is ever returned twice.
// ---------------------------------------------------------------------

var _ = context.Background // (remove once you use context in a test above)
