# Stage — Reentrant actors for Rust

Stage is a research prototype actor runtime for Rust whose defining property is
**actor reentrancy**:

* **Isolation** — only one continuation belonging to an actor executes at any
  instant; no data races.
* **Reentrancy** — whenever an actor method suspends (`.await`), another queued
  method of the same actor may run.
* **Resumption** — when the suspended operation becomes ready, execution resumes
  from the suspension point with exclusive access to the actor.

You write ordinary async methods on `&mut self`; Stage handles the rest.

### Inspired by Swift actors

The *semantics* are borrowed from Swift's actor model — isolation plus
reentrancy at suspension points — but the implementation is its own thing. Stage
is a Rust runtime with a Rust execution model: continuations (not actors) are the
schedulable unit, a per-actor token enforces isolation, work stealing balances
load, and a procedural macro lowers `&mut self` methods onto an `ActorContext`
primitive. None of that mirrors Swift's internals; it's what falls out of doing
this safely and idiomatically in Rust.

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
| `#[stage::actor_fn]` | turns a free `async fn(ctx: ActorContext<'_, A>, ..)` into a schedulable helper invoked as `name(&actor_ref, ..)`; may take only `ctx`, and may be generic over the actor type (`fn helper<A: Trait>(ctx: ActorContext<'_, A>)`) for reuse across distinct actors |
| `stage::run_on(&actor, fut)` | run an ordinary future (no macro, no `ActorContext`) as a continuation in an actor's isolation domain — "may run on an actor" decided at the call site |
| `ActorRef<A>` | cloneable handle to a spawned actor |
| `JoinHandle<R>` | awaitable, cancellable handle to a running invocation |
| `Executor` | a work-stealing executor; `Executor::new()` / `with_threads(n)` |
| `ActorContext<'a, A>` | opaque, internal-only actor handle (exposed only in `actor_fn`) |

## Deviations from the brief (and why)

These are deliberate, documented trade-offs of expressing reentrant-actor
semantics in *safe, stable* Rust:

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
   `&ActorRef<Database>`). A continuation runs eagerly, may be detached, and
   executes on the target actor's thread, so its future must be `'static` and
   cannot hold borrows of the caller's frame across suspension. In practice this
   is a non-issue: `ActorRef` is a cheap `Arc` clone, and any large value you'd
   otherwise borrow can be passed as `Arc<T>` — a refcount bump, not a copy, so
   it's zero-copy *sharing* (use `Arc<Mutex<T>>` if you also need to mutate it).
   Note this restricts only invocation *parameters*; borrows of locals and of
   `self` across `.await` inside a method body are fully supported.
4. **`spawn()` requires `Default`.** Use `spawn_with(state)` to supply an initial
   value explicitly.
5. **`ActorContext` is `Send`, but escape is still prevented** — see the
   Soundness section below. Briefly: it must be `Send` because continuations
   migrate between worker threads, but its invariant lifetime stops it escaping
   to a `'static` context at compile time, and a `TypeId`-tagged thread-local
   makes any stray deref panic rather than risk UB.
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
| `actor_fn_generic.rs` | `actor_fn` with no extra args; generic over actor type via a trait bound |
| `compile_fail.rs` + `ui/` | 11 `ActorContext` safety, 13 ordinary-async diagnostic |

```sh
cargo test            # run everything
cargo clippy --all-targets
```

## Soundness

Stage uses `unsafe` in exactly one place: `ActorContext` re-derives `&mut A` from
a thread-local raw pointer. Because that bypasses the borrow checker, the
invariant becomes the heart of the library's soundness. Here is the argument.

**Claim.** No safe use of Stage can produce a data race, a dangling reference, or
a type-confused access through `ActorContext`.

**The mechanism.** Before polling a continuation of actor `A`, the executor
publishes `(TypeId::of::<A>(), *mut A)` into a thread-local and restores the
previous value when the poll returns (an RAII guard, so it holds across panics
too). `ActorContext::<A>::deref{,_mut}` reads the thread-local, checks the
`TypeId`, and casts the pointer.

1. **No two `&mut A` exist at once (isolation).** Each actor cell holds a single
   `active` token. A continuation may be enqueued/polled only while holding it;
   it is released on `Poll::Pending` or completion and handed to the next FIFO
   continuation. So at most one continuation of `A` is ever being polled at any
   instant, across all worker threads. A `deref_mut` only yields `&mut A` during
   a poll, so two live `&mut A` for the same actor cannot coexist. The token is
   guarded by a mutex, which also provides the happens-before edge when the token
   (and thus the actor's state) migrates to another worker thread — so there is
   no data race even though the state is touched from different threads over
   time.

2. **The pointer is always valid when read.** The thread-local is non-`None` only
   for the dynamic extent of a poll, during which the executor holds the cell
   alive (the continuation task holds an `Arc` to it) and the pointer addresses
   live `UnsafeCell<A>` storage. The RAII guard restores the previous value on
   the way out, including on unwind.

3. **The pointer is never read for the wrong type or outside a poll.** Two
   independent guards:
   * *Compile time:* `ActorContext<'a, A>`'s lifetime `'a` is invariant and tied
     to the poll, so safe code cannot move a context into a `'static` context
     (a thread, a global, a detached future). `tests/ui/context_escape_thread.rs`
     confirms the escape attempt fails to compile. It also cannot be cloned or
     constructed by users (`tests/ui/`).
   * *Run time (defense in depth):* every deref checks the thread-local `TypeId`
     against `TypeId::of::<A>()`. If a context were ever observed outside the
     poll of an actor of its type — e.g. an internal scheduling bug — the deref
     **panics** instead of performing an invalid access. Misuse is therefore
     always safe, never UB.

4. **`Send` is required and does not weaken the above.** Continuations migrate
   between worker threads, so the boxed future — and anything it captures,
   including the (zero-sized) `ActorContext` — must be `Send`. Keeping the
   `Fut: Send` bound on the spawn path is also a *feature*: it rejects actor
   bodies that try to hold genuinely non-`Send` state (an `Rc`, etc.) across a
   suspension. The cost is that `Send`-ness alone can't forbid moving a context
   across threads — but points 3a (lifetime) and 3b (`TypeId` panic) cover that.

**Residual assumptions.** The argument rests on the scheduler actually upholding
the single-token invariant and setting the scope before every poll; those are
ordinary (safe) code, exercised by the test suite (isolation, reentrancy,
work-stealing, multi-executor, cancellation, panic). The `TypeId` guard turns any
violation of the scope discipline into a panic rather than UB.

## Benchmarks

Two runnable benchmarks (release mode):

```sh
cargo run --release --example throughput   # scaling across worker counts
cargo run --release --example vs_tokio      # head-to-head against Tokio
```

The workload is pure suspend/resume thrash: continuations that `.await` a leaf
which immediately re-readies them, so each cycle exercises the full reentrancy
path (release the actor token, re-schedule the continuation, steal/repoll). This
is the scheduler's worst case — almost no useful work to amortize overhead — and
exactly the path Stage is built around.

On a 14-core machine, matched independent workload (8192 units × 256 cycles =
~2.1M scheduler ops), atomic-counter completion:

| threads | Stage Mops/s | Tokio Mops/s |
|--------:|-------------:|-------------:|
|       1 |         40.3 |          4.3 |
|       2 |         88.2 |         32.2 |
|       4 |        174.2 |         20.9 |
|       8 |        260.5 |         12.8 |

Stage scales positively to 8 threads; Tokio (which defensively requeues a
self-waking task behind all others) peaks at 2 threads on this pattern. This is a
*scheduling* microbenchmark, not a general async-I/O claim — Tokio's reactor
remains the right tool for real I/O. The point is that Stage's per-continuation
scheduling is competitive and scales.

Getting here took removing three separate global cache lines from the per-cycle
hot path, each of which had caused negative multi-thread scaling:

1. `inject` locked a global `Mutex<Vec<Thread>>` to unpark a worker on every
   schedule → replaced with a lock-free `OnceLock` + parked-worker counter.
2. `release_token` cloned the `Executor` (`Arc<Shared>`) every cycle → clone the
   per-actor cell `Arc` instead (its refcount line is worker-owned).
3. The worker loop called `weak.upgrade()` every iteration (atomic inc/dec on the
   shared refcount) → workers now hold a strong `Arc<Shared>` and shutdown is
   driven by an explicit handle counter.

A re-scheduled continuation is also pushed onto the *current* worker's own deque
(cache-hot, off the contended global queue); idle workers still steal surplus.

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
