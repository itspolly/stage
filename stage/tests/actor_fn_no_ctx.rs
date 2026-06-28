//! `#[stage::actor_fn]` with no declared `ActorContext`. The helper runs on an
//! actor (its token + reentrancy) but never reads actor state, and is generic
//! over the actor type, so it can be invoked on any actor.

use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

#[stage::actor]
#[derive(Default)]
struct Worker {
    value: usize,
}
#[stage::actor]
impl Worker {
    async fn bump(&mut self) {
        self.value += 1;
    }
    async fn get(&mut self) -> usize {
        self.value
    }
}

#[stage::actor]
#[derive(Default)]
struct Other {
    value: usize,
}
#[stage::actor]
impl Other {
    async fn get(&mut self) -> usize {
        self.value
    }
}

// Exactly the requested shape: no ctx, no params.
#[stage::actor_fn]
async fn work() {
    sleep(Duration::from_millis(10)).await;
}

// No ctx, with args and a return value.
#[stage::actor_fn]
async fn compute(a: usize, b: usize) -> usize {
    sleep(Duration::from_millis(5)).await;
    a + b
}

#[tokio::test]
async fn runs_on_any_actor_type() {
    let w = Worker::spawn();
    let o = Other::spawn();

    // Same helper, two distinct actor types — it's generic over the actor.
    work(&w).await;
    work(&o).await;
    assert_eq!(compute(&w, 20, 22).await, 42);
    assert_eq!(compute(&o, 1, 2).await, 3);
}

// It genuinely runs inside the actor's isolation domain: reentrant at its
// suspension points (this would deadlock if it held the token across `.await`).
#[stage::actor_fn]
async fn park(rx: oneshot::Receiver<()>) {
    rx.await.unwrap();
}

#[tokio::test]
async fn no_ctx_helper_is_reentrant() {
    let w = Worker::spawn();
    let (tx, rx) = oneshot::channel();

    let task = park(&w, rx);
    timeout(Duration::from_secs(2), w.bump())
        .await
        .expect("actor blocked: no-ctx helper held the token across .await");
    assert_eq!(w.get().await, 1);

    tx.send(()).unwrap();
    task.await;
}
