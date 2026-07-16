# Context Propagation Through a Call Chain — Clarified Spec

Three functions, one call chain: `handleRequest` → `fetchData` → `computeExpensive`.
The same `context.Context` value flows down through all three unchanged
(aside from the one `WithValue` wrap at the top). The point of the exercise
is that a deadline set once at the very top must be *felt* three layers deep,
inside a loop, not just at function entry.

---

## `handleRequest(ctx context.Context, reqID string) error`

**Role:** entry point / top layer. This is what `main` calls directly.

**Inputs:**
- `ctx` — a context that already has a deadline/timeout attached by the caller (`main`). `handleRequest` does not create the timeout itself.
- `reqID` — an arbitrary string identifying this request, used only for logging.

**Behavior:**
- Takes the incoming `ctx` and derives a *new* context from it using `context.WithValue`, attaching `reqID` under some key. This derived context is what gets passed onward — not the original.
- The value attached here exists purely so that log lines deeper in the chain can print which request they belong to. It must never be read to make a decision (no branching on it, no using it to skip work, nothing "business logic" shaped).
- Calls `fetchData`, passing the derived context.
- Returns whatever error `fetchData` returns, unchanged. No wrapping, no swallowing, no retry logic here.

**What it does *not* do:**
- It does not set a timeout — that already exists on the incoming `ctx`.
- It does not check `ctx.Done()` itself. It has no loop and no work of its own to interrupt; its only job is to add the value and delegate.

---

## `fetchData(ctx context.Context) ([]int, error)`

**Role:** middle layer. Purely a pass-through with a return type change.

**Inputs:**
- `ctx` — the context handed down from `handleRequest`, still carrying the request ID value and the original deadline.

**Behavior:**
- Calls `computeExpensive` with the *same* `ctx`, no modification.
- Returns exactly what `computeExpensive` returns — same slice, same error, unchanged.

**What it does *not* do:**
- No cancellation logic of any kind. No `select`, no `ctx.Done()` check, no timeout derivation.
- No transformation of the result.
- This function exists in the exercise specifically to prove that a context (and its deadline) survives passing through a layer that does nothing with it — it's not supposed to have any interesting behavior itself. If you find yourself adding a check here, that's a sign you've misread the intent.

---

## `computeExpensive(ctx context.Context) ([]int, error)`

**Role:** the bottom layer — this is where the deadline is actually supposed to bite.

**Inputs:**
- `ctx` — same context, now several layers removed from where the timeout was originally set.

**Behavior:**
- Runs a loop of roughly 1000 iterations. Each iteration does a small unit of work followed by a short sleep — think of it as simulating expensive computation spread out over time, not one big blocking call.
- On **every single iteration** (not just once before the loop starts), it checks whether `ctx` has been cancelled or its deadline has passed.
- The instant that check is true, the function stops looping immediately and returns `ctx.Err()` as the error, along with a nil/empty result. It does not finish the remaining iterations first.
- If the loop completes all ~1000 iterations without the context ever being cancelled, it returns the fully computed `[]int` result and a nil error.

**What matters most here:**
- The check has to be *inside* the loop body, evaluated fresh on every pass. A single check before the loop starts (or only after the loop finishes) technically "checks the context" but doesn't satisfy the point of the exercise — the whole bug this project is designed to expose is a context that's checked once on entry and then ignored while a long-running loop keeps grinding.
- This is also the function you'll deliberately sabotage later: commenting out the per-iteration check and rerunning is how you prove to yourself that the check was actually doing something, rather than being a no-op that happened to look correct.

---

## How the three fit together

```
main
  └─ sets a timeout on ctx (shorter than ~1000 iterations would take uninterrupted)
     └─ handleRequest   (adds reqID for logging, delegates)
        └─ fetchData    (pure pass-through, no logic)
           └─ computeExpensive   (the only place that loops and the only
                                   place that actually needs to notice
                                   the deadline)
```

The error returned by `computeExpensive` when cancelled should propagate
back up through `fetchData` and `handleRequest` completely unchanged — by
the time it reaches `main`, it should still be recognizable as the same
context error (e.g. comparable with `errors.Is(err, context.DeadlineExceeded)`).