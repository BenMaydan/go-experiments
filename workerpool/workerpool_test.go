package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------
// WORKED EXAMPLE — study this pattern, you'll reuse it for almost
// every other test below.
// ---------------------------------------------------------------------

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

	completed := &atomic.Bool{}
	task := func() {
		time.Sleep(3 * time.Second)
		completed.Store(true)
	}

	pool.Submit(task)

	// to wait on all callers of Stop to complete
	n := 14
	wg := &sync.WaitGroup{}
	wg.Add(n)

	concurrentCaller := func(goroutineNumber int) {
		defer wg.Done()
		pool.Stop()

		if !completed.Load() {
			t.Errorf("Goroutine %v expected work to complete after stopping the pool but it did not.", goroutineNumber)
		}
	}

	for i := range n {
		go concurrentCaller(i)
	}

	wg.Wait()
}

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

// ---------------------------------------------------------------------
// YOUR TURN — stubs below. Each has a hint and a self-check question.
// Delete t.Skip() once implemented.
// ---------------------------------------------------------------------

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

	n := 420
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
		t.Errorf("Max seen goroutines: {%v} is greater than allowable {%v} goroutines", maxSeen, pool.maxWorkers)
	}

	if int(completed.Load()) != n {
		t.Errorf("Submitted %v goroutines but only %v completed", n, completed.Load())
	}

	if current.Load() != 0 {
		t.Errorf("%v goroutines should have finished but there is/are still %v running", n, current.Load())
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
	duration := 420 * time.Millisecond

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
		t.Errorf("submit wait should have returned after %v time but instead returned after %v time", duration, elapsed)
	}

	if !completed.Load() {
		t.Errorf("submit wait returned before the task marked the completed flag as true")
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
			t.Errorf("calling Do() after Stop() panicked instead of erroring")
		}
	}()

	pool.Stop()
	err = pool.Do(func() {})

	if !errors.Is(err, ErrorStopped{}) {
		t.Errorf("calling Do() after Stop() should have returned an ErrorStopped instance")
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
				t.Errorf("calling Do() after Submit() should wrap a panic with an instance of ErrorStopped, instead got %T", r)
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
	t.Skip("TODO")
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
	t.Skip("TODO")
}

// TestStopEscalation: per docs.md, if Stop() is in progress and another
// goroutine calls StopAbandon(), the abandon flag should escalate and
// the in-flight Stop() should return early rather than draining the
// full queue.
// Hint: you'll need a way to keep Stop()'s drain loop running long
// enough for the second goroutine to call StopAbandon() before it
// finishes — e.g. queue many slow tasks and only Add 1 initial worker.
func TestStopEscalation(t *testing.T) {
	t.Skip("TODO")
}

// TestPauseBlocksNewTasks: while Pause(ctx) is in effect, no new tasks
// should start executing; after ctx is cancelled, they should resume.
// Hint: submit a task, assert (with a timeout, not a sleep-and-hope)
// that it has NOT run while paused, then cancel the context and assert
// it now runs.
func TestPauseBlocksNewTasks(t *testing.T) {
	t.Skip("TODO")
}

// TestPanicRecoveryCallsErrHandler: a panicking task should not crash
// the pool or kill the worker permanently — it should invoke the
// configured ErrorHandlingHook and the worker should keep processing
// subsequent tasks.
// Hint: set ErrorHandlingHook to a func that records the panic value
// via a channel or atomic flag instead of the default (which panics).
// Submit a panicking task, then submit a normal task afterward and
// confirm it still runs.
func TestPanicRecoveryCallsErrHandler(t *testing.T) {
	t.Skip("TODO")
}

// TestNoGoroutineLeaks: after Stop(), no worker/dispatcher goroutines
// should remain running.
// Hint: this needs go.uber.org/goleak (`go get go.uber.org/goleak`).
// Pattern:
//
//	func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
//
// Then every test that creates a pool must fully Stop() it before
// returning, or this will fail at the end of the whole test binary run.
func TestNoGoroutineLeaks(t *testing.T) {
	t.Skip("TODO — requires goleak dependency")
}

// ---------------------------------------------------------------------
// Bonus, in queue_test.go: Queue[T] has its own mutex and is used
// concurrently from Add/Pop/Peek/Size. Worth its own concurrent
// stress test — many goroutines Add-ing and Pop-ing simultaneously
// with -race on, asserting Size() never goes negative and no element
// is ever returned twice.
// ---------------------------------------------------------------------

var _ = context.Background // (remove once you use context in a test above)
