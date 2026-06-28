//! Test 12: a `#[stage::actor_fn]` helper exhibits the same scheduling and
//! reentrancy semantics as an actor method.

use stage::ActorContext;
use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Counter {
    value: usize,
}

#[stage::actor]
impl Counter {
    async fn get(&mut self) -> usize {
        self.value
    }
}

// A reusable, actor-aware helper. `ActorContext` is exposed here only because
// there is no `self` receiver.
#[stage::actor_fn]
async fn increment_by(ctx: ActorContext<'_, Counter>, amount: usize) {
    ctx.value += amount;
    sleep(Duration::from_millis(100)).await;
    ctx.value += amount;
}

#[tokio::test]
async fn actor_fn_reentrancy() {
    let counter = Counter::spawn();

    let task = increment_by(&counter, 5);
    sleep(Duration::from_millis(20)).await;

    // Reentrancy: while increment_by is suspended, get() observes the partial
    // state — identical to an actor method.
    assert_eq!(counter.get().await, 5);

    task.await;
    assert_eq!(counter.get().await, 10);
}

#[tokio::test]
async fn actor_fn_sequential() {
    let counter = Counter::spawn();
    let a = increment_by(&counter, 1);
    let b = increment_by(&counter, 1);
    a.await;
    b.await;
    assert_eq!(counter.get().await, 4);
}
