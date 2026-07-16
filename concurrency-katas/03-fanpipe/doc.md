# Fan-Out/Fan-In Pipeline — Function Behavior Clarified

This describes *what each function must do*, not *how*. No code, no
implementation hints — just the contract each piece has to satisfy.

---

## `generate(ctx, n) <-chan int`

**Purpose:** the single source of work items for the whole pipeline.

- It hands back a channel immediately, then produces values *asynchronously*
  onto that channel — the caller doesn't block waiting for `generate` to
  finish before it can start reading.
- The values it emits are `1, 2, 3, ..., n` — in order, one at a time.
- Every send onto the channel is a decision point: either the send succeeds,
  or `ctx` gets cancelled while the send is waiting. If cancellation happens
  first, `generate` must stop producing immediately, not finish emitting the
  remaining numbers.
- Regardless of which way it stops (ran out of numbers, or got cancelled),
  it must close its output channel exactly once when it's done. This is the
  signal downstream stages rely on to know "no more input is coming."
- Nobody else is allowed to close this channel. Ownership of closing a
  channel belongs to whoever creates and writes to it — that's `generate`
  here.

**Think about:** what happens to a goroutine sending on an unbuffered
channel if nothing is ever going to read from it again? That's the failure
mode cancellation-awareness is protecting against.

---

## `worker(ctx, in) <-chan Result`

**Purpose:** one participant in the fan-out stage. Many of these run
concurrently, all reading from the *same* `in` channel.

- Each worker independently pulls numbers off `in` — there's no
  coordination between workers about who gets which number; Go's channel
  semantics already guarantee each value goes to exactly one receiver.
- For every number it receives, it determines primality and produces a
  `Result` describing that number and whether it's prime.
- It has to keep doing this until `in` is closed *and* drained — a closed
  channel with nothing left in the buffer is the "no more work" signal, not
  an error.
- Once `in` is exhausted, the worker closes its **own** output channel. Each
  worker owns and closes its own output — this matters a lot for `merge`,
  see below.
- There are two separate places cancellation can bite, and both must be
  handled:
  - **Receiving** from `in`: if `ctx` is cancelled while waiting for the
    next number, stop waiting and exit rather than blocking forever.
  - **Sending** a `Result` to the output channel: if nothing downstream is
    reading anymore (because the pipeline is shutting down) and `ctx` is
    cancelled, the worker must not sit there forever trying to send. It
    should notice the cancellation and exit instead of blocking on the send.
- Either way it exits (input exhausted, or cancelled), it must still close
  its output channel before returning. Downstream stages depend on that
  close happening no matter which exit path was taken.

**Think about:** a worker mid-computation that gets cancelled — does it
finish the primality check and then try to send a result nobody wants, or
does it abandon the send? What should happen to that in-flight value?

---

## `merge[T any](ctx, chans ...<-chan T) <-chan T`

**Purpose:** collapse N separate channels (one per worker) into a single
output channel, interleaving whatever arrives from any of them.

- It immediately returns one output channel to the caller, then
  asynchronously copies everything that arrives on *any* input channel onto
  that single output channel. Order across different input channels is not
  guaranteed — whichever value arrives first goes out first.
- Internally it needs to watch **all** input channels concurrently, not one
  at a time in sequence — otherwise a channel with no data yet would block
  progress on channels that do have data.
- Every one of the input channels will eventually close (because every
  `worker` closes its own output when done). `merge` has to notice each
  individual channel closing and stop reading from *that* channel
  specifically, while continuing to read from the others that are still
  open.
- Only once *all* input channels have closed does `merge` close its own
  output channel — and it must do this exactly once. This is the specific
  hazard the project description is pointing at: if you're not careful
  about coordination, multiple internal goroutines (one per input channel)
  can each try to be the one that closes the shared output channel, and a
  channel being closed twice is a runtime panic, not a warning.
- The generic type parameter `T` just means this function doesn't care
  what's flowing through it — it should work for a fan-in of `int` channels,
  `Result` channels, or anything else, without being rewritten.

**Think about:** how do N goroutines (one watching each input channel) all
signal "I'm done" in a way that lets exactly one piece of code — running
only after *all* of them have signaled — perform the single close? What
primitive exists specifically for "wait until a group of goroutines have
all finished"?

---

## `main()`

**Purpose:** wire the three stages together into one live pipeline and
prove that cancellation propagates all the way through it without leaving
anything stuck.

- The wiring is a straight pipe: `generate`'s output channel is the `in`
  for every worker (all workers share that same source channel — that's
  what makes it fan-*out*). Each worker's output channel then becomes one
  of the inputs to `merge` — that's the fan-*in*.
- `main` reads from `merge`'s single output channel in a loop until it
  closes, printing or collecting results as they arrive.
- It creates a `context.WithCancel` (not `WithTimeout` — you're triggering
  this manually, not letting a clock do it) and calls the cancel function
  itself partway through, on purpose, rather than waiting for the pipeline
  to finish naturally.
- The "done when" criteria ask you to verify two things experimentally,
  not just reason about them:
  1. Deliberately write a version where two goroutines close the same
     channel, run it, and actually read the panic message Go produces —
     so you recognize that exact error if you ever see it by accident.
  2. After wiring cancellation in properly, confirm — by letting the
     program actually finish rather than hang — that no worker goroutine
     is left permanently blocked trying to send a result that will never
     be read.

**Think about:** if you cancel early, `generate` will stop early too. What
does that mean for how many `Result`s `main`'s reading loop should expect
to see, and how does it know when to stop reading rather than waiting
forever?