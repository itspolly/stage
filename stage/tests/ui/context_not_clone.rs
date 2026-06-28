// Test 11 — ActorContext cannot be cloned. (The actor type itself is not Clone
// either, so deref-based cloning cannot mask the missing impl.)

use stage::ActorContext;

#[stage::actor]
#[derive(Default)]
struct Counter {
    value: usize,
}

#[stage::actor]
impl Counter {
    async fn noop(&mut self) {}
}

#[stage::actor_fn]
async fn helper(ctx: ActorContext<'_, Counter>) {
    let _escaped = ctx.clone();
}

fn main() {}
