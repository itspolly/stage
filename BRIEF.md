# Project Brief: Swift-Style Reentrant Actors for Rust

## Objective

Design and implement a prototype Rust actor runtime providing semantics similar to Swift actors.

The defining property is **actor reentrancy**:

* Only one continuation executes inside an actor at any instant.
* Whenever an actor method suspends (`.await`), another queued actor method may execute.
* When the suspended operation becomes ready, execution resumes from the suspension point with exclusive access to the actor.

The implementation should feel as close as possible to Swift's actor model while remaining idiomatic Rust.

The project is primarily a **research prototype** exploring whether Swift-style actor semantics can be expressed in Rust with excellent ergonomics and competitive performance.

---

# Scope

Implement:

* `#[actor]` procedural macro (or another equally ergonomic code-generation approach if necessary)
* Actor runtime
* Actor executor
* Generated actor handle (`ActorRef<T>` or similar)
* Reentrant scheduling
* Async actor methods
* Request/response messaging
* Cross-actor calls
* Task cancellation
* Multiple executors
* Work-stealing scheduler for load balancing between executors
* Comprehensive unit tests demonstrating semantics

Do **not** initially attempt:

* Distributed actors
* Persistence
* Networking
* Supervision
* Priority scheduling

The prototype only needs to support a single process.

---

# Desired API

```rust
#[actor]
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

#[tokio::main]
async fn main() {
    let counter = Counter::spawn();

    let task = counter.increment();

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(counter.get().await, 1);

    task.await;

    assert_eq!(counter.get().await, 2);
}
```

Users should write ordinary async Rust.

They should **not** manually define:

* message enums
* channels
* custom futures
* state machines
* schedulers

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

another actor method may execute after the suspension begins but before it resumes.

This is the defining behaviour of the runtime.

---

## No mutable borrow across suspension

The implementation must **not** retain `&mut Actor` inside a suspended future.

Instead:

* acquire mutable access while polling
* release mutable access when returning `Poll::Pending`
* reacquire mutable access on the next poll

This property enables actor reentrancy.

---

## FIFO scheduling

Ready continuations execute in FIFO order.

When a continuation becomes ready via its waker, it is appended to the ready queue.

---

## Await transparency

Users should write ordinary async functions.

No explicit yield, resume, requeue or scheduling APIs should be required.

---

## Cross-actor calls

Actors should be able to invoke async methods on other actors naturally.

Example:

```rust
#[actor]
struct Database;

#[actor]
struct Service;

impl Service {
    async fn request(&mut self, db: &ActorRef<Database>) {
        db.query().await;
    }
}
```

While awaiting another actor:

* execution of the current actor is released
* unrelated actor work may continue
* unnecessary deadlocks should be avoided

---

## Cancellation

Actor method invocations should be cancellable.

Cancellation must:

* never leave actor state inconsistent
* never violate actor isolation
* correctly clean up suspended continuations
* have well-defined behaviour while suspended

Cancellation semantics must be documented and tested.

---

## Multiple executors

The runtime should support multiple independent executors.

Example:

```rust
let io = Executor::new();
let ui = Executor::new();

let database = Database::spawn_on(&io);
let window = Window::spawn_on(&ui);
```

Requirements:

* actors execute only on the executor they were spawned onto
* cross-executor actor calls function transparently
* actor isolation is preserved

---

## Work stealing

Executors should balance work using work stealing.

Requirements:

* work stealing must never violate actor isolation
* at most one continuation belonging to an actor executes at once
* continuations may migrate between worker threads within an executor if safe
* executor affinity must be preserved
* scheduling policy should be documented

The schedulable unit should be **actor continuations**, not actors themselves.

---

# Suggested Architecture

```text
actor-runtime/

    Executor
    Worker
    ActorExecutor
    ActorRef<T>
    ActorCell<T>
    ActorFuture<T>
    Scheduler

actor-macros/

    #[actor]
```

Suggested polling trait:

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

The actor is supplied during polling rather than captured by the future.

---

# Success Criteria

The following tests must all pass.

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

```text
increment()

...suspends...

get()
```

Expected:

```text
get() == 1
```

This is the defining behavioural test.

---

## Test 4 — Multiple suspended continuations

Queue

```text
increment()
increment()
increment()
```

All suspend.

When resumed:

```text
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

```
Actor A
    ↓ await
Actor B
```

Verify:

* execution succeeds
* unrelated actor work continues
* no unnecessary blocking

---

## Test 7 — Mutual actor calls

Two actors call one another while suspending.

Verify:

* no deadlock
* both operations complete
* actor isolation preserved

---

## Test 8 — Cancellation

Cancel an actor invocation before completion.

Verify:

* actor remains usable
* no leaked continuations
* no inconsistent state
* subsequent messages execute correctly

---

## Test 9 — Multiple executors

Spawn actors onto different executors.

Verify:

* actors execute only on their assigned executor
* cross-executor calls function correctly
* actor isolation remains intact

---

## Test 10 — Work stealing

Create many actors with many suspended continuations.

Verify:

* idle workers successfully steal work
* throughput improves compared to a single worker
* no actor executes multiple continuations simultaneously
* actor state remains correct

---

## Test 11 — Large queue

Queue 100,000 actor invocations.

Verify:

* completion
* FIFO ordering
* no leaks
* stable memory usage

---

## Test 12 — Panic isolation

A panic inside one actor invocation must not corrupt executor state.

Subsequent actor invocations should either continue correctly (if supported) or fail in a documented and deterministic manner.

---

# Stretch Goals

* Return values
* Async streams
* Actor timers
* Supervision
* Priority queues
* Bounded mailboxes
* Metrics and tracing
* Deterministic testing utilities
* Distributed actors
* Persistence

---

# Research Question

Can Rust provide an actor programming model with semantics comparable to Swift actors—including reentrancy, isolation, cancellation, cross-actor calls, multiple executors, executor affinity, and work-stealing scheduling—while preserving Rust's safety guarantees and allowing developers to write ordinary async methods without manually constructing state machines?

A successful prototype should demonstrate that Swift-style actor semantics are achievable in Rust through a combination of runtime support and code generation while remaining ergonomic, performant, and memory safe.

