//! Tests 6, 7, 9: cross-actor calls, mutual calls (no deadlock), and multiple
//! executors with transparent cross-executor calls.

use stage::{ActorRef, Executor};
use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Database {
    hits: usize,
}

#[stage::actor]
impl Database {
    async fn fetch(&mut self) -> usize {
        self.hits += 1;
        sleep(Duration::from_millis(20)).await;
        self.hits
    }
}

#[stage::actor]
#[derive(Default)]
struct Service {
    last: usize,
    // background work counter, to prove unrelated work proceeds.
    background: usize,
}

#[stage::actor]
impl Service {
    async fn query(&mut self, db: ActorRef<Database>) -> usize {
        let r = db.fetch().await;
        self.last = r;
        r
    }

    async fn tick(&mut self) {
        self.background += 1;
    }

    async fn background(&mut self) -> usize {
        self.background
    }
}

// Test 6 — Cross-actor call: a Service awaits a Database, and unrelated Service
// work continues while the cross-actor call is suspended.
#[tokio::test]
async fn cross_actor_call() {
    let db = Database::spawn();
    let svc = Service::spawn();

    let q = svc.query(db.clone());
    // While `query` is suspended inside `db.fetch().await`, the Service actor is
    // free, so an unrelated method runs reentrantly.
    sleep(Duration::from_millis(5)).await;
    svc.tick().await;
    assert_eq!(svc.background().await, 1);

    assert_eq!(q.await, 1);
}

// Two actors that call into each other.
#[stage::actor]
#[derive(Default)]
struct A {
    val: usize,
}

#[stage::actor]
impl A {
    async fn start(&mut self, b: ActorRef<B>, me: ActorRef<A>) -> usize {
        self.val = 10;
        let r = b.work(me).await; // B will call back into A while we're suspended
        self.val + r
    }

    async fn helper(&mut self) -> usize {
        self.val
    }
}

#[stage::actor]
#[derive(Default)]
struct B {
    val: usize,
}

#[stage::actor]
impl B {
    async fn work(&mut self, a: ActorRef<A>) -> usize {
        self.val = 5;
        // A.start is suspended here, so A is free and helper() runs: reentrancy
        // across the mutual call avoids deadlock.
        let r = a.helper().await;
        self.val + r
    }
}

// Test 7 — Mutual actor calls: both complete, no deadlock.
#[tokio::test]
async fn mutual_calls() {
    let a = A::spawn();
    let b = B::spawn();
    let result = a.start(b.clone(), a.clone()).await;
    // start: val=10; work: val=5, helper()->10, returns 15; start returns 25.
    assert_eq!(result, 25);
}

// Test 9 — Multiple executors: actors are pinned to their own executor, and
// cross-executor calls are transparent.
#[tokio::test]
async fn multiple_executors() {
    let io = Executor::new();
    let ui = Executor::new();

    let db = Database::spawn_on(&io);
    let svc = Service::spawn_on(&ui);

    // svc (on ui) calls db (on io) transparently.
    let r = svc.query(db.clone()).await;
    assert_eq!(r, 1);
    assert_eq!(svc.query(db.clone()).await, 2);
}
