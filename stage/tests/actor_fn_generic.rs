//! `#[stage::actor_fn]` helpers that take only `ctx` (no extra args) and that are
//! generic over the actor type — reusable from multiple distinct actors.

use stage::ActorContext;
use std::time::Duration;
use tokio::time::sleep;

// --- No extra arguments (only ctx) -----------------------------------------

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

#[stage::actor_fn]
async fn tick(ctx: ActorContext<'_, Counter>) {
    ctx.value += 1;
}

#[stage::actor_fn]
async fn read(ctx: ActorContext<'_, Counter>) -> usize {
    ctx.value
}

#[tokio::test]
async fn actor_fn_with_no_extra_args() {
    let c = Counter::spawn();
    tick(&c).await;
    tick(&c).await;
    assert_eq!(read(&c).await, 2);
}

// --- Generic over the actor type, via a shared trait ------------------------

trait Tally: Send + 'static {
    fn bump(&mut self);
    fn total(&self) -> usize;
}

#[stage::actor]
#[derive(Default)]
struct Apples {
    n: usize,
}
#[stage::actor]
impl Apples {
    async fn total(&mut self) -> usize {
        self.n
    }
}
impl Tally for Apples {
    fn bump(&mut self) {
        self.n += 1;
    }
    fn total(&self) -> usize {
        self.n
    }
}

#[stage::actor]
#[derive(Default)]
struct Oranges {
    n: usize,
}
#[stage::actor]
impl Oranges {
    async fn total(&mut self) -> usize {
        self.n
    }
}
impl Tally for Oranges {
    fn bump(&mut self) {
        self.n += 1;
    }
    fn total(&self) -> usize {
        self.n
    }
}

// One helper, usable from ANY actor that is `Tally`. It accesses actor state
// through the trait bound, suspends, and accesses it again — full reentrant
// continuation semantics, independent of the concrete actor type.
#[stage::actor_fn]
async fn bump_twice<T: Tally>(ctx: ActorContext<'_, T>) -> usize {
    ctx.bump();
    sleep(Duration::from_millis(50)).await;
    ctx.bump();
    ctx.total()
}

#[tokio::test]
async fn generic_helper_runs_on_distinct_actors() {
    let apples = Apples::spawn();
    let oranges = Oranges::spawn();

    // Same free function, two distinct actor types.
    assert_eq!(bump_twice(&apples).await, 2);
    assert_eq!(bump_twice(&oranges).await, 2);

    assert_eq!(apples.total().await, 2);
    assert_eq!(oranges.total().await, 2);
}

#[tokio::test]
async fn generic_helper_is_reentrant() {
    let apples = Apples::spawn();

    let task = bump_twice(&apples); // bumps to 1, suspends in sleep
    sleep(Duration::from_millis(10)).await;
    // Reentrancy: an actor method runs while the generic helper is suspended.
    assert_eq!(apples.total().await, 1);

    assert_eq!(task.await, 2);
    assert_eq!(apples.total().await, 2);
}
