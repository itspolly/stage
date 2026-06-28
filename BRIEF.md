# Stage — Swift-Style Reentrant Actors for Rust

## Objective

Design and implement **Stage**, a Rust actor runtime providing semantics similar to Swift actors.

The defining property is **actor reentrancy**:

* Only one continuation executes inside an actor at any instant.
* Whenever an actor method suspends (`.await`), another queued actor method may execute.
* When the suspended operation becomes ready, execution resumes from the suspension point with exclusive access to the actor.

The implementation should feel as close as possible to Swift's actor model while remaining idiomatic Rust.

This project is a **research prototype** exploring whether Swift-style actor semantics can be expressed in Rust while preserving Rust's safety guarantees and ergonomics.

---

# Core Principles

1. **Actor isolation**

   Only one continuation belonging to an actor executes at a time.

2. **Reentrancy**

   Suspension points (`.await`) are the only points where another continuation may enter the actor.

3. **Ordinary Rust syntax**

   Users should write normal Rust methods using `&mut self`.

4. **Type-safe internals**

   Internally, Stage should represent actor execution using an opaque `ActorContext<'_, T>`.

5. **No magic outside Stage**

   Ordinary async Rust should behave exactly as it does today.

---

# Scope

Implement:

* `#[stage::actor]`
* `#[stage::actor_fn]`
* Actor runtime
* Actor executor
* Generated `ActorRef<T>`
* Reentrant scheduling
* Async actor methods
* Async actor helper functions
* Cross-actor calls
* Request/response messaging
* Task cancellation
* Multiple executors
* Work-stealing scheduler
* Comprehensive unit tests

Out of scope (initially):

* Distributed actors
* Persistence
* Networking
* Supervision
* Priority scheduling

Single-process support is sufficient for the prototype.

---

# Desired API

## Actor definition

```rust
#[stage::actor]
struct Counter {
    value: usize,
}

impl Counter {
    async fn increment(&mut self) {
        self.value += 1;

        tokio::time::sleep(Duration::from_secs(1)).await;

        self.value += 1;
    }

    async fn get(&mut self) -> usize {
        self.value
    }
}
```

Usage:

```rust
let counter = Counter::spawn();

let task = counter.increment();

tokio::time::sleep(Duration::from_millis(10)).await;

assert_eq!(counter.get().await, 1);

task.await;

assert_eq!(counter.get().await, 2);
```

Users should **never** manually construct:

* message enums
* channels
* futures
* state machines
* schedulers
* `ActorContext`

---

# Actor Helper Functions

Reusable actor-aware, global helper functions should be supported.

Example:

```rust
#[stage::actor_fn]
async fn increment_by(
    ctx: ActorContext<'_, Counter>,
    amount: usize,
) {
    ctx.value += amount;

    tokio::time::sleep(Duration::from_secs(1)).await;

    ctx.value += amount;
}
```

`ActorContext` is only exposed here because there is no actor method receiver.

It should **not** be required for ordinary actor methods.

---

# Internal Representation

Actor methods should be lowered internally into an implementation using `ActorContext`.

Conceptually:

User code:

```rust
async fn increment(&mut self)
```

becomes something equivalent to

```rust
async fn __stage_increment(
    ctx: ActorContext<'_, Counter>,
)
```

This transformation is an implementation detail.

---

# ActorContext

Stage should expose an opaque type:

```rust
pub struct ActorContext<'a, A> {
    // private
}
```

Properties:

* exclusive mutable access to actor state while polling
* cannot be constructed by users
* cannot be cloned
* cannot be stored
* cannot outlive a single poll
* cannot accidentally be captured by ordinary async futures

`ActorContext` is the primitive abstraction used internally by Stage.

---

# Semantic Requirements

## Actor isolation

Only one continuation belonging to an actor may execute simultaneously.

No data races.

---

## Reentrancy

Given

```rust
async fn increment(&mut self) {
    self.value += 1;

    sleep(...).await;

    self.value += 1;
}
```

another actor method may execute after the suspension begins but before execution resumes.

This is the defining property of Stage.

---

## No mutable borrow across suspension

The runtime must **never** retain `&mut Actor` across suspension.

Instead:

* acquire actor access while polling
* release actor access when returning `Poll::Pending`
* reacquire actor access on the next poll

---

## FIFO scheduling

Ready continuations execute in FIFO order.

When a continuation becomes ready via its waker, it is appended to the ready queue.

---

## Await transparency

Users write ordinary async methods.

No explicit yield/resume APIs should exist.

---

## Cross-actor calls

Actors should naturally invoke methods on other actors.

Example:

```rust
#[stage::actor]
struct Database;

#[stage::actor]
struct Service;

impl Service {
    async fn query(&mut self, db: &ActorRef<Database>) {
        db.fetch().await;
    }
}
```

Requirements:

* current actor releases execution while awaiting
* unrelated actor work continues
* deadlocks are avoided where possible

---

## Cancellation

Actor invocations are cancellable.

Cancellation must:

* preserve actor consistency
* preserve isolation
* clean up suspended continuations
* have deterministic semantics

---

## Multiple executors

Support multiple executors.

Example:

```rust
let io = Executor::new();
let ui = Executor::new();

let db = Database::spawn_on(&io);
let window = Window::spawn_on(&ui);
```

Requirements:

* actors execute only on their assigned executor
* cross-executor actor calls work transparently
* actor isolation is preserved

---

## Work stealing

Executors should balance work using work stealing.

Requirements:

* actor isolation is never violated
* continuations are the schedulable unit
* continuations may migrate between worker threads within an executor
* executor affinity is preserved
* scheduling policy is documented

Actors themselves are **not** scheduled.

Continuations are.

---

## Ordinary async functions

Ordinary async functions remain ordinary Rust.

Example:

```rust
async fn helper(counter: &mut Counter) {
    ...
}
```

These functions **must not** participate in actor lowering.

If an ordinary async function would retain actor state across suspension, Stage should emit a compile-time diagnostic.

Example:

```text
error:

ordinary async function captures actor state across suspension

help:

move this function into the actor implementation

or annotate it with

    #[stage::actor_fn]
```

---

# Suggested Architecture

```text
stage/

    Executor
    Worker
    Scheduler

    ActorExecutor
    ActorCell<T>
    ActorRef<T>
    ActorContext<T>

    ActorFuture<T>

stage-macros/

    #[stage::actor]
    #[stage::actor_fn]
```

Suggested polling interface:

```rust
trait ActorFuture<A> {
    type Output;

    fn poll(
        self: Pin<&mut Self>,
        actor: &mut A,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output>;
}
```

The actor is supplied during polling rather than stored inside the future.

---

# Success Criteria

## Test 1 — Simple invocation

```rust
increment()
```

Completes successfully.

---

## Test 2 — Sequential execution

```rust
increment()
increment()
```

Expected:

```text
value == 4
```

---

## Test 3 — Reentrancy

```
increment()

...suspends...

get()
```

Expected:

```
get() == 1
```

This is the defining behavioural test.

---

## Test 4 — Multiple suspended continuations

Queue:

```
increment()
increment()
increment()
```

All suspend.

After waking:

```
value == 6
```

No races.

---

## Test 5 — Exclusive execution

Spawn many concurrent tasks mutating actor state.

Verify:

* no simultaneous mutable access
* deterministic final value

---

## Test 6 — Cross-actor call

Actor A awaits Actor B.

Verify:

* execution succeeds
* unrelated actor work continues
* no unnecessary blocking

---

## Test 7 — Mutual actor calls

Two actors await one another.

Verify:

* no deadlock
* both operations complete
* actor isolation preserved

---

## Test 8 — Cancellation

Cancel an actor invocation.

Verify:

* actor remains usable
* no leaked continuations
* consistent state
* later messages execute correctly

---

## Test 9 — Multiple executors

Spawn actors on different executors.

Verify:

* executor affinity
* transparent cross-executor calls
* actor isolation

---

## Test 10 — Work stealing

Create many actors with many suspended continuations.

Verify:

* idle workers steal continuations
* throughput improves
* no actor executes multiple continuations simultaneously
* actor state remains correct

---

## Test 11 — ActorContext safety

Verify `ActorContext`:

* cannot be cloned
* cannot be stored
* cannot escape
* cannot be retained across suspension outside Stage
* cannot be moved into an ordinary async future

Invalid usage should fail to compile.

---

## Test 12 — actor_fn

Verify that a `#[stage::actor_fn]` exhibits identical scheduling and reentrancy semantics to an actor method.

---

## Test 13 — Compile-time diagnostics

Attempt:

```rust
async fn helper(counter: &mut Counter) {
    tokio::time::sleep(...).await;
}
```

Expect a compile-time diagnostic explaining why actor state cannot be retained across suspension and suggesting `#[stage::actor_fn]`.

---

## Test 14 — Large queue

Queue 100,000 actor invocations.

Verify:

* completion
* FIFO ordering
* stable memory usage
* no leaks

---

## Test 15 — Panic isolation

A panic inside one actor invocation must not corrupt executor state.

Subsequent actor invocations should either continue correctly or fail in a documented and deterministic manner.

---

# Stretch Goals

* Return values
* Async streams
* Actor timers
* Metrics
* Tracing
* Deterministic testing utilities
* Distributed actors
* Persistence

---

# Research Question

Can Rust provide an actor programming model with semantics comparable to Swift actors—including reentrancy, isolation, cancellation, cross-actor calls, multiple executors, executor affinity, and work-stealing scheduling—while preserving Rust's safety guarantees and allowing developers to write ordinary async methods?

A successful prototype should demonstrate that Swift-style actor semantics are achievable in Rust through a combination of runtime support and code generation while remaining ergonomic, performant, memory safe, and idiomatic.
