// Test 11 — ActorContext cannot escape its poll. Although it is `Send` (so
// continuations can migrate between worker threads), its invariant lifetime is
// tied to the poll, so it cannot be moved into a `'static` context such as a
// spawned thread. This is caught at compile time.

use stage::ActorContext;

#[stage::actor]
#[derive(Default)]
struct Guarded {
    n: usize,
}

#[stage::actor]
impl Guarded {
    async fn noop(&mut self) {}
}

#[stage::actor_fn]
async fn escape(ctx: ActorContext<'_, Guarded>) {
    std::thread::spawn(move || {
        let _escaped = ctx.n;
    });
}

fn main() {}
