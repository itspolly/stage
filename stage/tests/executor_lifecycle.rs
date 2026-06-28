//! Executor lifecycle: dropping all handles to an executor shuts its workers
//! down cleanly (no hang, no leak), and creating/destroying many executors is
//! fine. Workers hold a strong reference internally but exit on the shutdown
//! signal raised when the last user-facing handle drops.

use stage::Executor;

#[stage::actor]
#[derive(Default)]
struct Counter {
    value: usize,
}

#[stage::actor]
impl Counter {
    async fn bump(&mut self) {
        self.value += 1;
    }
    async fn get(&mut self) -> usize {
        self.value
    }
}

#[tokio::test]
async fn create_and_drop_many_executors() {
    for _ in 0..50 {
        let ex = Executor::with_threads(4);
        let c = Counter::spawn_on(&ex);
        for _ in 0..100 {
            c.bump().await;
        }
        assert_eq!(c.get().await, 100);
        // `c` and `ex` drop here; the executor's workers should exit cleanly.
    }
}

#[tokio::test]
async fn actor_keeps_executor_alive() {
    // An actor cell holds an executor handle, so the actor keeps working even
    // after the local `Executor` binding is dropped.
    let c = {
        let ex = Executor::with_threads(2);
        Counter::spawn_on(&ex)
        // `ex` dropped here, but the actor's cell still holds a handle.
    };
    for _ in 0..100 {
        c.bump().await;
    }
    assert_eq!(c.get().await, 100);
}
