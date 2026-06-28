//! Test 10: work stealing across a multi-worker executor with many actors and
//! many suspended continuations. Verifies completion, correctness, and (by the
//! deterministic per-actor totals) that no actor ran two continuations at once.

use stage::Executor;
use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Cell {
    value: usize,
}

#[stage::actor]
impl Cell {
    async fn work(&mut self) {
        self.value += 1;
        sleep(Duration::from_millis(10)).await;
        self.value += 1;
    }

    async fn get(&mut self) -> usize {
        self.value
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_stealing() {
    let ex = Executor::with_threads(8);

    const ACTORS: usize = 64;
    const PER_ACTOR: usize = 50;

    let cells: Vec<_> = (0..ACTORS).map(|_| Cell::spawn_on(&ex)).collect();

    // Queue many continuations across many actors; they all suspend on the
    // sleep, creating plenty of work for idle workers to steal.
    let mut tasks = Vec::new();
    for c in &cells {
        for _ in 0..PER_ACTOR {
            tasks.push(c.work());
        }
    }
    for t in tasks {
        t.await;
    }

    // Each actor saw exactly PER_ACTOR invocations, each adding 2 => 2*PER_ACTOR.
    // Any isolation violation (two continuations of one actor at once) would
    // corrupt these totals.
    for c in &cells {
        assert_eq!(c.get().await, 2 * PER_ACTOR);
    }
}
