// Test 13 — Ordinary async functions do not participate in actor lowering, so an
// ordinary `async fn helper(counter: &mut Counter)` cannot drive the actor: the
// async, reentrant methods live on `ActorRef<Counter>`, not on the bare actor
// type. This steers users toward `#[stage::actor_fn]` (or an actor method).
//
// NOTE: The brief's Test 13 asks for a bespoke diagnostic ("ordinary async
// function captures actor state across suspension ... annotate with
// #[stage::actor_fn]"). Emitting a *custom* message for a function Stage never
// sees is not possible without compiler support; this is the closest achievable
// enforcement and is documented as a known limitation in the README.

use std::time::Duration;

#[stage::actor]
#[derive(Default)]
struct Counter {
    value: usize,
}

#[stage::actor]
impl Counter {
    async fn increment(&mut self) {
        self.value += 1;
    }
}

async fn helper(counter: &mut Counter) {
    tokio::time::sleep(Duration::from_millis(1)).await;
    // `increment` is a reentrant actor method on ActorRef<Counter>, not on
    // Counter. This fails to compile.
    counter.increment().await;
}

fn main() {}
