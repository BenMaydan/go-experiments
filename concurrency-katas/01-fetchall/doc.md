# Behavioral Spec: `fetchSource` and `fetchAll`

Precise contracts for the errgroup-based multi-source fetcher — no
implementation, just what each function does and returns.

---

## `fetchSource(ctx context.Context, id int, failRate float64) (string, error)`

**Call pattern:** called once per source (one call = one goroutine inside
`fetchAll`). It does not yield or return multiple times — it's a normal
blocking function call that returns exactly once, either with a result or
an error.

**Internal behavior while running:**

- It simulates "work" as a fixed number of discrete steps (you choose the
  count, e.g. 5–10 "iterations"), not one big `time.Sleep(2 * time.Second)`.
- Each iteration does a *short* `time.Sleep` (e.g. 100–300ms) followed by a
  check of `ctx.Err()`.
- This is a plain loop with sleep-then-check inside it — not a
  `time.Timer`/`time.Ticker`, and not a `select` blocking on `ctx.Done()`.
  The idea: the sleep is interruptible-in-spirit (checked promptly after
  each short sleep) but the mechanism itself is simple — sleep, then check,
  then decide whether to keep going. Using `select` on `ctx.Done()` instead
  is also valid and arguably more idiomatic, but the sleep-loop version is
  what's being described when the project doc says "checking `ctx.Err()`
  each iteration."
- If `ctx.Err()` is non-nil at the top of an iteration, the function stops
  immediately and returns `("", ctx.Err())`. This is how cancellation from
  a sibling failure actually gets observed.
- If it completes all iterations without cancellation, *then* it decides
  whether to simulate a failure using `failRate` (e.g.
  `rand.Float64() < failRate`). If it "fails," return `("", someError)`.
  Otherwise return the successful string result and `nil`.
- Track how many iterations it got through before returning (increment a
  counter, log it) so `main` can prove early sources got cut short.

**Return contract:**

| Outcome | Return value |
|---|---|
| Success | `(resultString, nil)` |
| Simulated random failure | `("", err)` |
| Cancelled mid-loop | `("", ctx.Err())` |

Exactly one return value pair, exactly once. No channels, no yielding.

---

## `fetchAll(ctx context.Context, sourceIDs []int) ([]string, error)`

**Call pattern:** called once by `main`. It blocks until either all sources
succeed or the group is aborted by the first error — then returns once.

**Internal behavior:**

- Creates a derived context and an `errgroup.Group` via
  `errgroup.WithContext(ctx)`. This derived context (call it `gctx`) is
  what gets passed into each `fetchSource` call — not the original `ctx`.
  That distinction matters: `gctx` is the one that gets cancelled
  automatically the instant any goroutine in the group returns a non-nil
  error.
- Launches one goroutine per source ID via `g.Go(func() error { ... })` —
  that's `len(sourceIDs)` goroutines, no more, no fewer, no worker
  pool/queueing.
- Each of those closures calls `fetchSource(gctx, id, failRate)` exactly
  once, and needs to store the successful string result somewhere that
  survives after the goroutine returns (a pre-sized slice indexed by
  position, written to directly — not appended, since concurrent appends
  to a shared slice are a race).
- After launching all goroutines, it calls `g.Wait()`, which blocks until
  every goroutine has returned, and gives back the *first* non-nil error
  the group encountered (or `nil` if all succeeded).
- Based on `g.Wait()`'s result: if error, return `(nil, err)`; if nil,
  return the populated slice of results and `nil`.

**Return contract:**

| Outcome | Return value |
|---|---|
| All sources succeed | `([]string of results in order, nil)` |
| Any source fails | `(nil, firstError)` |

No partial results are ever returned — the whole point of errgroup is
all-or-nothing.

---

## The mechanism that makes cancellation propagate

`errgroup.WithContext` returns a `gctx` tied to the group. The moment any
`g.Go` closure returns a non-nil error, `gctx` gets cancelled (its
`Done()` channel closes) even though the other goroutines are still
mid-flight. Since you passed `gctx` (not the original `ctx`) into every
`fetchSource` call, those other in-flight calls will see
`ctx.Err() != nil` on their very next iteration check and bail early —
which is exactly the behavior `main` is supposed to demonstrate via the
iteration counters.

---

## Open decisions left to you

- Whether `failRate` is the same for every source or varies per source.
- Whether you want at least one source to be deliberately slow
  ("would-succeed-if-given-time") so the cancellation-cuts-it-short
  behavior is visually obvious in the output.