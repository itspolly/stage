//! Tests 8 and 15: cancellation and panic isolation.

use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Worker {
    value: usize,
}

#[stage::actor]
impl Worker {
    async fn slow(&mut self) {
        self.value += 1;
        sleep(Duration::from_secs(10)).await;
        self.value += 1; // never reached if cancelled during the sleep
    }

    async fn boom(&mut self) {
        self.value += 1;
        panic!("intentional panic inside actor invocation");
    }

    async fn inc(&mut self) {
        self.value += 1;
    }

    async fn get(&mut self) -> usize {
        self.value
    }
}

// Test 8 — Cancellation: cancel a suspended invocation; actor stays usable, no
// leaked continuation, consistent state, later messages run correctly.
#[tokio::test]
async fn cancellation() {
    let w = Worker::spawn();

    let task = w.slow();
    sleep(Duration::from_millis(30)).await; // let it apply the first += and suspend
    assert_eq!(w.get().await, 1);

    task.cancel();
    sleep(Duration::from_millis(30)).await;

    // The second += never happened; state is consistent at 1.
    assert_eq!(w.get().await, 1);

    // Later messages still execute correctly.
    w.inc().await;
    assert_eq!(w.get().await, 2);
}

// Test 15 — Panic isolation: a panicking invocation does not corrupt the
// executor; subsequent invocations continue correctly.
#[tokio::test]
async fn panic_isolation() {
    let w = Worker::spawn();

    let task = w.boom();
    // Awaiting via try_join observes the failure without unwinding the test.
    let outcome = task.try_join().await;
    assert!(outcome.is_err(), "panicked invocation yields no value");

    // State mutated before the panic persists; the actor is still usable.
    assert_eq!(w.get().await, 1);
    w.inc().await;
    assert_eq!(w.get().await, 2);

    // Many subsequent invocations still work — the executor is healthy.
    for _ in 0..100 {
        w.inc().await;
    }
    assert_eq!(w.get().await, 102);
}
