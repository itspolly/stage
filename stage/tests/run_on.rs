//! `stage::run_on`: schedule an ordinary future (no macro, no `ActorContext`)
//! into an actor's isolation domain, decided at the call site.

use std::time::Duration;
use tokio::time::sleep;

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

// A completely ordinary async fn: no attribute, no ActorContext, no knowledge of
// actors at all.
async fn double_after(x: usize) -> usize {
    sleep(Duration::from_millis(20)).await;
    x * 2
}

#[tokio::test]
async fn runs_on_actor_or_inline() {
    let w = Worker::spawn();

    // Same function, two ways: on the actor, and plain.
    assert_eq!(stage::run_on(&w, double_after(21)).await, 42);
    assert_eq!(double_after(21).await, 42);
}

#[tokio::test]
async fn run_on_is_reentrant() {
    let w = Worker::spawn();

    // A future that runs on the actor and suspends (doesn't touch actor state).
    let task = stage::run_on(&w, double_after(10));
    // While it is suspended, the actor's own methods still run — it participated
    // in the actor's scheduling rather than blocking it.
    sleep(Duration::from_millis(5)).await;
    w.bump().await;
    assert_eq!(w.get().await, 1);

    assert_eq!(task.await, 20);
}

#[tokio::test]
async fn run_on_accepts_inline_async_block() {
    let w = Worker::spawn();
    let r = stage::run_on(&w, async {
        sleep(Duration::from_millis(5)).await;
        7 * 6
    })
    .await;
    assert_eq!(r, 42);
}
