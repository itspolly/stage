//! Tests 1–5 and 14 from the brief: invocation, sequencing, reentrancy,
//! multiple suspended continuations, exclusive execution, and a large queue.

use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Counter {
    value: usize,
}

#[stage::actor]
impl Counter {
    async fn increment(&mut self) {
        self.value += 1;
        sleep(Duration::from_millis(100)).await;
        self.value += 1;
    }

    async fn bump(&mut self) {
        self.value += 1;
    }

    async fn get(&mut self) -> usize {
        self.value
    }
}

// Test 1 — Simple invocation.
#[tokio::test]
async fn simple_invocation() {
    let counter = Counter::spawn();
    counter.increment().await;
    assert_eq!(counter.get().await, 2);
}

// Test 2 — Sequential execution: two increments => 4.
#[tokio::test]
async fn sequential_execution() {
    let counter = Counter::spawn();
    let t1 = counter.increment();
    let t2 = counter.increment();
    t1.await;
    t2.await;
    assert_eq!(counter.get().await, 4);
}

// Test 3 — Reentrancy: get() observes the partial state while increment() is
// suspended. This is the defining behavioural test.
#[tokio::test]
async fn reentrancy() {
    let counter = Counter::spawn();
    let task = counter.increment();
    sleep(Duration::from_millis(20)).await;
    assert_eq!(counter.get().await, 1);
    task.await;
    assert_eq!(counter.get().await, 2);
}

// Test 4 — Multiple suspended continuations: three increments => 6.
#[tokio::test]
async fn multiple_suspended_continuations() {
    let counter = Counter::spawn();
    let a = counter.increment();
    let b = counter.increment();
    let c = counter.increment();
    a.await;
    b.await;
    c.await;
    assert_eq!(counter.get().await, 6);
}

// Test 5 — Exclusive execution: many concurrent mutators, deterministic result.
#[tokio::test]
async fn exclusive_execution() {
    let counter = Counter::spawn();
    let mut tasks = Vec::new();
    for _ in 0..1000 {
        tasks.push(counter.bump());
    }
    for t in tasks {
        t.await;
    }
    assert_eq!(counter.get().await, 1000);
}

// Test 14 — Large queue: 100k invocations complete with the correct total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_queue() {
    let counter = Counter::spawn();
    const N: usize = 100_000;
    let mut tasks = Vec::with_capacity(N);
    for _ in 0..N {
        tasks.push(counter.bump());
    }
    for t in tasks {
        t.await;
    }
    assert_eq!(counter.get().await, N);
}
