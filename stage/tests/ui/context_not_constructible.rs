// Test 11 — ActorContext cannot be constructed by users: its constructor is
// crate-private.

fn main() {
    let _ctx = stage::ActorContext::<u8>::new();
}
