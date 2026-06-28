//! Reentrancy + work-stealing throughput benchmark.
//!
//! Each invocation repeatedly suspends and resumes via a cheap self-waking
//! `Yield` leaf. Every suspension releases the actor's token (exercising the
//! reentrancy path) and re-schedules the continuation onto the executor, where
//! any idle worker may steal it (exercising work stealing). With many actors,
//! continuations stream through the scheduler and spread across worker threads.
//!
//! We hold the *total* work fixed and vary the worker-thread count, reporting
//! scheduler throughput (suspend/resume cycles per second) and the speedup over
//! a single worker. Run with:
//!
//! ```sh
//! cargo run --release --example throughput
//! ```

use stage::Executor;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

/// A leaf future that yields exactly once, waking itself immediately so the
/// continuation is re-scheduled. This is the unit of scheduler work.
struct Yield {
    yielded: bool,
}

impl Yield {
    fn once() -> Self {
        Yield { yielded: false }
    }
}

impl Future for Yield {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[stage::actor]
#[derive(Default)]
struct Churn {
    acc: u64,
}

#[stage::actor]
impl Churn {
    /// Suspend/resume `cycles` times, doing a little work each time so the
    /// optimizer cannot elide the loop.
    async fn churn(&mut self, cycles: u64) -> u64 {
        for i in 0..cycles {
            Yield::once().await;
            self.acc = self.acc.wrapping_add(black_box(i));
        }
        self.acc
    }
}

fn run(threads: usize, actors: usize, msgs: usize, cycles: u64) -> (f64, std::time::Duration) {
    let ex = Executor::with_threads(threads);
    // A current-thread runtime just to await the JoinHandles; all actor work
    // runs on the Stage executor's worker threads.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let refs: Vec<_> = (0..actors).map(|_| Churn::spawn_on(&ex)).collect();

        let start = Instant::now();
        let mut handles = Vec::with_capacity(actors * msgs);
        for r in &refs {
            for _ in 0..msgs {
                handles.push(r.churn(cycles));
            }
        }
        for h in handles {
            black_box(h.await);
        }
        let elapsed = start.elapsed();

        let ops = (actors * msgs) as u64 * cycles;
        let throughput = ops as f64 / elapsed.as_secs_f64();
        (throughput, elapsed)
    })
}

fn main() {
    const ACTORS: usize = 256;
    const MSGS: usize = 32;
    const CYCLES: u64 = 256;

    let total_ops = (ACTORS * MSGS) as u64 * CYCLES;
    println!(
        "workload: {ACTORS} actors x {MSGS} msgs x {CYCLES} suspend/resume cycles \
         = {total_ops} scheduler ops\n"
    );
    println!(
        "{:>8}  {:>14}  {:>16}  {:>9}",
        "threads", "time (ms)", "ops/sec", "speedup"
    );

    // Warm up so the shared reactor + first executor are initialized.
    let _ = run(1, 16, 4, 16);

    let mut baseline = None;
    for &threads in &[1usize, 2, 4, 8] {
        let (throughput, elapsed) = run(threads, ACTORS, MSGS, CYCLES);
        let base = *baseline.get_or_insert(throughput);
        println!(
            "{:>8}  {:>14.1}  {:>16.0}  {:>8.2}x",
            threads,
            elapsed.as_secs_f64() * 1000.0,
            throughput,
            throughput / base
        );
    }
}
