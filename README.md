# Stage — Swift-style reentrant actors for Rust

Stage is a research prototype actor runtime for Rust whose defining property is
**actor reentrancy**, modelled on Swift's actor model:

* **Isolation** — only one continuation belonging to an actor executes at any
  instant; no data races.
* **Reentrancy** — whenever an actor method suspends (`.await`), another queued
  method of the same actor may run.
* **Resumption** — when the suspended operation becomes ready, execution resumes
  from the suspension point with exclusive access to the actor.

You write ordinary async methods on `&mut self`; Stage handles the rest.

```rust
#[stage::actor]
#[derive(Default)]
struct Counter { value: usize }

#[stage::actor]
impl Counter {
    async fn increment(&mut self) {
        self.value += 1;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        self.value += 1;
    }
    async fn get(&mut self) -> usize { self.value }
}

# async fn demo() {
let counter = Counter::spawn();
let task = counter.increment();                 // starts running immediately
tokio::time::sleep(std::time::Duration::from_millis(10)).await;
assert_eq!(counter.get().await, 1);             // reentrancy: observes partial state
task.await;
assert_eq!(counter.get().await, 2);
# }
```

## How reentrancy works

Ordinary Rust `async fn` desugars to a state machine that captures `&mut self`
for the *entire* body, including across `.await`. That directly contradicts the
core requirement: **never retain `&mut Actor` across suspension.**

Stage breaks the borrow into per-poll slices:

1. The executor owns each actor's state in an `ActorCell` (`UnsafeCell<A>`).
2. Immediately before polling a continuation, the executor publishes a
   `*mut A` for that actor into a thread-local "actor scope". It clears it after
   the poll returns.
3. The `#[stage::actor]` macro lowers `async fn increment(&mut self)` into
   `async fn(ctx: ActorContext<'_, Counter>)`, rewriting `self` to `ctx`.
   `ActorContext` is a **zero-sized** handle: each `Deref`/`DerefMut`
   re-derives `&mut A` from the thread-local.

Because the continuation future only captures the zero-sized `ActorContext`
(never a real borrow), it is free to live across `.await`. The actual `&mut A`
exists only for the duration of a single dereference inside a single poll. When
the future returns `Poll::Pending`, no borrow of the actor is held — so another
continuation may enter. That is reentrancy.

## Scheduling policy

The schedulable unit is the **continuation** (one in-flight method invocation),
not the actor. Actors are never scheduled.

* Each `Executor` owns a fixed pool of worker threads, a global injector queue,
  and one work-stealing deque per worker (`crossbeam-deque`). Continuations are
  injected onto the executor and may be **stolen** by any idle worker, so they
  migrate freely between threads.
* **Isolation** is enforced by a single per-actor **token** (`active` flag in
  `CellSched`). A continuation may only run while it holds its actor's token, so
  at most one continuation per actor runs at a time — even across worker
  threads. The mutex guarding the token also establishes the happens-before
  edges required when the token migrates between threads.
* On suspension (`Pending`) or completion, a continuation **releases the token**,
  handing it to the next FIFO-queued continuation of the same actor. Releasing on
  suspension is precisely what enables reentrancy.
* **FIFO** — ready continuations of an actor are promoted in arrival order via
  the per-actor pending queue. Work stealing reorders only *across* actors.
* **Executor affinity** — a continuation is always re-injected onto the executor
  named by its own actor cell, so an actor's work only ever runs on its assigned
  executor. Cross-executor calls are transparent: they are just message sends
  whose response is awaited via a channel.

### Leaf futures and timers

Stage schedules actor continuations itself but delegates *leaf* futures (timers,
I/O — e.g. `tokio::time::sleep`) to a shared background Tokio runtime that every
worker thread `enter()`s. When a leaf becomes ready, its waker re-schedules the
owning continuation onto its Stage executor. This keeps the actor scheduler
small while reusing Tokio's mature reactor.

## Cancellation

`JoinHandle::cancel()` / `abort()` marks the invocation and re-schedules it; the
executor then drops the suspended future (running destructors — e.g. cancelling
a timer) and releases the token. State mutated *before* the cancellation point is
preserved (cooperative, Swift-like semantics). The actor remains fully usable and
later messages execute normally.

## Panic isolation

Each poll runs inside `catch_unwind`. A panicking invocation does not take down
the worker thread or corrupt the executor: its result channel is dropped (the
awaiter observes `Cancelled`), the token is released, and the actor continues to
serve subsequent invocations. State mutated before the panic persists.

## API surface

| Item | Purpose |
|------|---------|
| `#[stage::actor]` on a struct | generates `spawn` / `spawn_on` / `spawn_with` / `spawn_with_on` |
| `#[stage::actor]` on an impl | lowers async `self`-methods and generates the `ActorRef` methods |
| `#[stage::actor_fn]` | turns a free `async fn(ctx: ActorContext<'_, A>, ..)` into a schedulable helper invoked as `name(&actor_ref, ..)` |
| `ActorRef<A>` | cloneable handle to a spawned actor |
| `JoinHandle<R>` | awaitable, cancellable handle to a running invocation |
| `Executor` | a work-stealing executor; `Executor::new()` / `with_threads(n)` |
| `ActorContext<'a, A>` | opaque, internal-only actor handle (exposed only in `actor_fn`) |

## Deviations from the brief (and why)

These are deliberate, documented trade-offs of expressing Swift semantics in
*safe, stable* Rust:

1. **`#[stage::actor]` goes on both the struct and its impl block.** A proc-macro
   attribute can only observe the item it annotates. The struct attribute can't
   see the methods, so the impl-level attribute is what generates the typed
   `ActorRef` methods. The brief shows the attribute only on the struct; one
   extra attribute on the impl is the minimal faithful workaround.
2. **`ActorRef<A>` methods are added via a generated extension trait**
   (`__StageMethods_<Name>`), because an inherent `impl ActorRef<Counter>` is
   illegal (`ActorRef` is a foreign type, E0116). The trait is defined in the
   actor's own module, so it is in scope automatically for `actor.method()`
   calls there. Calling from another module requires importing it.
3. **Method parameters are by value** (e.g. `db: ActorRef<Database>`, not
   `&ActorRef<Database>`). A continuation future must be `'static`, so it cannot
   hold borrows across suspension. `ActorRef` is a cheap `Arc` clone.
4. **`spawn()` requires `Default`.** Use `spawn_with(state)` to supply an initial
   value explicitly.
5. **`ActorContext` safety is partly static, partly an invariant.** Statically
   enforced: it cannot be cloned and cannot be constructed by users
   (`tests/ui/`). Not statically enforced: it is `Send` (continuations migrate
   threads, so everything they capture must be `Send`), so "cannot be sent to
   another thread" is upheld by the runtime invariant (the thread-local is set
   before every poll) rather than the type system.
6. **Test 13's bespoke diagnostic is best-effort.** Stage cannot emit a custom
   message for an *un-annotated* ordinary async function it never sees. What it
   *can* enforce is that the reentrant methods live on `ActorRef`, not the bare
   actor type, so an ordinary `async fn helper(c: &mut Counter)` cannot drive the
   actor and fails to compile (`tests/ui/ordinary_async_fn.rs`), steering the
   user toward `#[stage::actor_fn]`.
## Intra-actor calls

A method may call another async method on `self`:

```rust
async fn compute(&mut self) -> usize {
    self.value = 10;
    let doubled = self.double().await; // continues in the *same* continuation
    self.value = doubled;
    doubled
}
```

`self.double().await` is lowered to a **direct inline call** to the lowered
associated function — it continues executing inside the *same* continuation,
holding the same actor token (it is not a new scheduled message). If the inline
call suspends, the whole continuation suspends, the token is released (so other
queued methods may run reentrantly), and on resume the actor pointer is
re-published. Synchronous helper methods are also callable directly through the
context's `Deref`.

## Tests

The suite in `stage/tests/` covers all 15 success criteria from the brief:

| File | Brief tests |
|------|-------------|
| `basic.rs` | 1 invocation, 2 sequencing, 3 reentrancy, 4 multiple suspended, 5 exclusive, 14 large queue (100k) |
| `cross_actor.rs` | 6 cross-actor, 7 mutual (no deadlock), 9 multiple executors |
| `cancel_panic.rs` | 8 cancellation, 15 panic isolation |
| `work_stealing.rs` | 10 work stealing |
| `actor_fn.rs` | 12 `actor_fn` parity |
| `compile_fail.rs` + `ui/` | 11 `ActorContext` safety, 13 ordinary-async diagnostic |

```sh
cargo test            # run everything
cargo clippy --all-targets
```

## Research question — conclusion

> Can Rust provide an actor model with semantics comparable to Swift
> actors — reentrancy, isolation, cancellation, cross-actor calls, multiple
> executors, executor affinity, and work-stealing scheduling — while preserving
> safety and letting developers write ordinary async methods?

**Largely yes.** Stage demonstrates all of those properties in safe, stable Rust
by combining a small runtime (per-actor token + thread-local actor scope +
work-stealing continuation scheduler) with code generation that lowers
`&mut self` methods into `ActorContext` bodies. Users write ordinary async
methods and never touch message enums, channels, futures, schedulers, or the
context type.

The residual gaps are exactly where Rust's guarantees and Swift's compiler magic
diverge: borrowed parameters across suspension are disallowed (Rust's `'static`
requirement), `ActorContext` must be `Send` for thread migration (so its
non-escape property is a runtime invariant rather than a type-level one), and a
custom diagnostic for functions the macro never sees is impossible. None of these
compromise memory safety; they shape the ergonomics. The prototype is evidence
that Swift-style actor semantics are achievable in idiomatic Rust through runtime
support plus code generation.
