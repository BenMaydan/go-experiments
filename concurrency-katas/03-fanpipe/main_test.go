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

	in := make(chan int)
	results := worker(ctx, in)

	in <- 4
	cancel()

	// worker's send-select races ctx.Done() against results<-, both of
	// which become ready the instant we start receiving. Go's select
	// resolves ties uniformly at random, so either outcome is valid:
	//   - ctx.Done() wins: we see closure immediately
	//   - the send wins: we see the in-flight value, then closure next
	select {
	case v, ok := <-results:
		if !ok {
			return // closed immediately, ctx.Done() won
		}
		t.Logf("in-flight value delivered before close: %v", v)
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not respond after cancel")
	}

	select {
	case _, ok := <-results:
		if ok {
			t.Fatal("expected results closed on second receive")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not close results after in-flight value")
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

