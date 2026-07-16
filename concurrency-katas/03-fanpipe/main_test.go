package main

import (
	"context"
	"testing"
	"time"
)


func TestGenerateCancelStopsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := generate(ctx, 1_000_000) // nobody reads this

	v := <-ch // consume exactly one value, so the goroutine is now blocked trying to send the second
	if v != 1 {
		t.Fatalf("got %v, want 1", v)
	}

	cancel()

	// If cancellation didn't work, this blocks forever and -timeout kills the test.
	_, ok := <-ch
	if ok {
		t.Fatal("expected channel to be closed after cancel")
	}
}

func TestWorkerCancelStopsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan int) // unbuffered, we control delivery by hand
	results := worker(ctx, in)

	// This send only returns once worker's select has actually picked
	// "case v, ok := <-in:" — so after this line, we know worker has
	// the value and is now computing isPrime, about to block trying
	// to send the result to a channel nobody's reading.
	in <- 4

	cancel()

	// If worker ignored ctx.Done() on the result-send, this blocks
	// forever and -timeout kills the test.
	_, ok := <-results
	if ok {
		t.Fatal("expected results to be closed after cancel")
	}
}

func TestPipelineCancelTerminates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gen := generate(ctx, 1_000_000_000) // effectively unbounded for this test

	chans := make([]<-chan Result, 4)
	for i := range chans {
		chans[i] = worker(ctx, gen)
	}
	merged := merge(ctx, chans...)

	done := make(chan struct{})
	go func() {
		count := 0
		for range merged {
			count++
			if count == 50 {
				cancel()
			}
		}
		close(done)
	}()

	select {
	case <-done:
		// good — merged closed, loop exited
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not terminate after cancel — goroutine leak or missed ctx.Done()")
	}
}

