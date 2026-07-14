# Three Short Concurrency Projects

Build these as three separate small programs/packages, not one combined project.
Each isolates one mechanism so you can't lean on a pattern you already know.

---

## 1. errgroup-Based Multi-Source Fetcher (highest priority)

Simulate fetching data from several unreliable sources concurrently. If any
one source fails, every other in-flight source should stop working and the
caller gets back the first error. This is the "concurrent calls that must
all succeed together" pattern you'll see constantly in backend code.

**Required to implement:**
- `fetchSource(ctx context.Context, id int, failRate float64) (string, error)` — simulates work with `time.Sleep` in a loop (not one big sleep), checking `ctx.Err()` each iteration; returns an error some percentage of the time
- `fetchAll(ctx context.Context, sourceIDs []int) ([]string, error)` — uses `errgroup.WithContext(ctx)` to launch a goroutine per source via `g.Go(...)`, collects results, returns `g.Wait()`'s error
- `main()` — wraps the call in `context.WithTimeout`, prints whether cancellation actually stopped the other sources early (add a counter/log per source showing how many loop iterations it completed before stopping)

**Done when:** you can show that when one source errors, the others' iteration counts are visibly cut short rather than running to completion.

---

## 2. Context Propagation Through a Call Chain (second priority)

A fake 3-layer service chain where a deadline set at the top has to actually
be honored by the deepest, longest-running function — not just checked once
and ignored. This exposes the most common real-world context bug: checking
`ctx.Done()` on entry but not inside a long loop.

**Required to implement:**
- `handleRequest(ctx context.Context, reqID string) error` — top layer, derives a request-scoped context (`context.WithValue` for the ID, logging-only, never business logic), calls `fetchData`
- `fetchData(ctx context.Context) ([]int, error)` — middle layer, passes the same context down to `computeExpensive`, does no cancellation logic of its own
- `computeExpensive(ctx context.Context) ([]int, error)` — runs a loop of ~1000 iterations with a small per-iteration sleep; checks `ctx.Done()` on **every iteration** and returns `ctx.Err()` immediately if cancelled
- `main()` — sets `context.WithTimeout` shorter than the full uninterrupted runtime, calls `handleRequest`, times how long it actually takes to return

**Done when:** you can comment out the per-iteration check in `computeExpensive`, rerun, and observe the function now takes far longer than the timeout to return — proving the check was load-bearing.

---

## 3. Fan-Out/Fan-In Pipeline (third priority)

A multi-stage pipeline: one generator, N parallel workers, and a fan-in
merge back to a single output channel. You've built fan-out before (your
dispatcher); the new piece is the fan-in and multi-stage wiring.

**Required to implement:**
- `generate(ctx context.Context, n int) <-chan int` — emits numbers `1..n` on a channel, closes it when done or when `ctx` is cancelled
- `worker(ctx context.Context, in <-chan int) <-chan Result` — reads numbers, checks primality via trial division, sends a `Result{N int; IsPrime bool}`; closes its output channel when `in` closes; respects `ctx` cancellation on both receive and send (`select` with `ctx.Done()` on the send, not just the receive)
- `merge[T any](ctx context.Context, chans ...<-chan T) <-chan T` — generic fan-in using a `sync.WaitGroup`, one goroutine per input channel forwarding to a shared output channel, closed exactly once after all inputs are drained
- `main()` — wires generator → N workers (fan-out) → `merge` (fan-in) → reads final results in a loop, with a `context.WithCancel` you trigger early on purpose at least once

**Done when:** you've deliberately closed a channel from two goroutines once to see the panic message firsthand, and confirmed that early `ctx` cancellation doesn't leave any worker goroutine blocked forever on a send.
