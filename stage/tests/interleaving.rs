//! Proves the interleaving model rigorously: every real suspension point is an
//! interleave point, and it composes recursively across nested actor_fn calls.
//! Each test would *deadlock* (caught by a timeout) if a token were wrongly held
//! across an `.await`.

use stage::{ActorContext, ActorRef};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[stage::actor]
#[derive(Default)]
struct X {
    v: usize,
}
#[stage::actor]
impl X {
    async fn bump(&mut self) {
        self.v += 1;
    }
    async fn get(&mut self) -> usize {
        self.v
    }
}

#[stage::actor]
#[derive(Default)]
struct Y {
    v: usize,
}
#[stage::actor]
impl Y {
    async fn bump(&mut self) {
        self.v += 1;
    }
    async fn get(&mut self) -> usize {
        self.v
    }
}

// An actor_fn that parks on a channel, then mutates Y's state.
#[stage::actor_fn]
async fn y_wait(ctx: ActorContext<'_, Y>, rx: oneshot::Receiver<()>) {
    rx.await.unwrap(); // suspension point — must release Y's token
    ctx.v += 1;
}

// An actor_fn on X that calls the actor_fn on Y and awaits it.
#[stage::actor_fn]
async fn x_calls_y(ctx: ActorContext<'_, X>, y: ActorRef<Y>, rx: oneshot::Receiver<()>) {
    ctx.v += 1;
    y_wait(&y, rx).await; // suspension point — must release X's token
    ctx.v += 1;
}

// A single actor_fn's suspension is an interleave point for its actor.
#[stage::actor_fn]
async fn park(ctx: ActorContext<'_, X>, rx: oneshot::Receiver<()>) -> usize {
    rx.await.unwrap();
    ctx.v
}

#[tokio::test]
async fn actor_fn_suspension_is_interleave_point() {
    let x = X::spawn();
    let (tx, rx) = oneshot::channel();

    let task = park(&x, rx); // parks; would hold the token forever if not reentrant
    timeout(Duration::from_secs(2), x.bump())
        .await
        .expect("X blocked: actor_fn did not release its token at .await");
    assert_eq!(x.get().await, 1);

    tx.send(()).unwrap();
    assert_eq!(task.await, 1);
}

// Recursive: while a nested chain (actor_fn on X awaiting actor_fn on Y) is
// suspended, BOTH X and Y keep interleaving their own queued work.
#[tokio::test]
async fn nested_actor_fns_interleave_recursively() {
    let x = X::spawn();
    let y = Y::spawn();
    let (tx, rx) = oneshot::channel();

    // x_calls_y: X.v -> 1, then awaits y_wait on Y (which parks on rx).
    let task = x_calls_y(&x, y.clone(), rx);

    // X interleaves while x_calls_y is suspended awaiting Y.
    timeout(Duration::from_secs(2), x.bump())
        .await
        .expect("X blocked: outer actor_fn held X's token across .await");
    assert_eq!(x.get().await, 2); // 1 (from x_calls_y) + 1 (bump)

    // Y interleaves while y_wait is parked on the channel.
    timeout(Duration::from_secs(2), y.bump())
        .await
        .expect("Y blocked: inner actor_fn held Y's token across .await");
    assert_eq!(y.get().await, 1); // y_wait's += 1 hasn't happened yet

    // Release the chain; both resume.
    tx.send(()).unwrap();
    task.await;
    assert_eq!(x.get().await, 3); // x_calls_y adds its final += 1
    assert_eq!(y.get().await, 2); // y_wait adds its += 1
}
