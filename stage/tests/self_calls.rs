//! Intra-actor self-calls: `self.method().await` continues executing inside the
//! *same* continuation (a direct inline call, not a new scheduled message).

use std::time::Duration;
use tokio::time::sleep;

#[stage::actor]
#[derive(Default)]
struct Calc {
    value: usize,
    log: Vec<&'static str>,
}

#[stage::actor]
impl Calc {
    async fn compute(&mut self) -> usize {
        self.log.push("compute:start");
        self.value = 10;
        // Inline self-call: runs within this continuation, holding the token.
        let doubled = self.double().await;
        self.value = doubled;
        self.log.push("compute:end");
        doubled
    }

    async fn double(&mut self) -> usize {
        self.log.push("double:start");
        sleep(Duration::from_millis(20)).await; // suspends the whole continuation
        self.log.push("double:end");
        self.value * 2
    }

    // A sync helper is reachable through the context's Deref.
    fn sync_bump(&mut self) {
        self.value += 1;
    }

    async fn via_sync_helper(&mut self) -> usize {
        self.sync_bump();
        self.value
    }

    async fn get(&mut self) -> usize {
        self.value
    }

    async fn log_len(&mut self) -> usize {
        self.log.len()
    }
}

#[tokio::test]
async fn self_call_runs_inline() {
    let calc = Calc::spawn();
    // compute sets 10, calls double() inline -> 20, stores 20.
    assert_eq!(calc.compute().await, 20);
    assert_eq!(calc.get().await, 20);
    // Ordering proves it ran inline within one continuation, in order.
    assert_eq!(calc.log_len().await, 4); // start, double:start, double:end, end
}

#[tokio::test]
async fn sync_helper_through_context() {
    let calc = Calc::spawn();
    assert_eq!(calc.via_sync_helper().await, 1);
    assert_eq!(calc.via_sync_helper().await, 2);
}

// While a self-call is suspended, the actor token is released, so another queued
// method runs reentrantly and observes the partial state.
#[tokio::test]
async fn reentrancy_across_self_call() {
    let calc = Calc::spawn();
    let task = calc.compute(); // sets value=10, suspends inside double()
    sleep(Duration::from_millis(5)).await;
    assert_eq!(calc.get().await, 10); // reentrant read of partial state
    assert_eq!(task.await, 20);
    assert_eq!(calc.get().await, 20);
}
