//! Matched head-to-head: Stage vs Tokio on identical self-wake suspend/resume
//! work. Independent units (Stage: one message per actor; Tokio: one task) so
//! neither side pays extra serialization. Completion is observed via an atomic
//! counter, so this measures the scheduler, not handle-await plumbing.
//!
//! Caveat: this is a *scheduling* microbenchmark (a continuation that suspends
//! and immediately re-readies itself), which is exactly the reentrancy path
//! Stage is built around. It is not a claim about general async I/O throughput,
//! where Tokio's reactor is the mature, battle-tested choice. The point is to
//! show Stage's per-continuation scheduling is competitive and scales.
//!
//! ```sh
//! cargo run --release --example vs_tokio
//! ```

// Invocations run eagerly; dropping the JoinHandle detaches (work still runs).
#![allow(clippy::let_underscore_future)]

use stage::Executor;
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

struct Yield {
    yielded: bool,
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

static DONE: AtomicUsize = AtomicUsize::new(0);

#[stage::actor]
#[derive(Default)]
struct Unit {
    acc: u64,
}
#[stage::actor]
impl Unit {
    async fn go(&mut self, cycles: u64) {
        for i in 0..cycles {
            Yield { yielded: false }.await;
            self.acc = self.acc.wrapping_add(black_box(i));
        }
        black_box(self.acc);
        DONE.fetch_add(1, Ordering::Relaxed);
    }
}

fn stage_run(threads: usize, units: usize, cycles: u64) -> f64 {
    DONE.store(0, Ordering::Relaxed);
    let ex = Executor::with_threads(threads);
    let refs: Vec<_> = (0..units).map(|_| Unit::spawn_on(&ex)).collect();
    let start = Instant::now();
    for r in &refs {
        let _ = r.go(cycles);
    }
    while DONE.load(Ordering::Relaxed) < units {
        std::hint::spin_loop();
    }
    start.elapsed().as_secs_f64()
}

fn tokio_run(threads: usize, units: usize, cycles: u64) -> f64 {
    let done = Arc::new(AtomicUsize::new(0));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let start = Instant::now();
        for _ in 0..units {
            let done = done.clone();
            tokio::spawn(async move {
                let mut acc = 0u64;
                for i in 0..cycles {
                    Yield { yielded: false }.await;
                    acc = acc.wrapping_add(black_box(i));
                }
                black_box(acc);
                done.fetch_add(1, Ordering::Relaxed);
            });
        }
        while done.load(Ordering::Relaxed) < units {
            tokio::task::yield_now().await;
        }
        start.elapsed().as_secs_f64()
    })
}

fn main() {
    const UNITS: usize = 8192;
    const CYCLES: u64 = 256;
    let total = UNITS as u64 * CYCLES;
    let _ = stage_run(1, 64, 16);
    let _ = tokio_run(1, 64, 16);
    println!("matched self-wake: {UNITS} units x {CYCLES} cycles = {total} ops\n");
    println!("{:>8}  {:>12}  {:>12}  {:>10}  {:>10}", "threads", "stage ms", "tokio ms", "stage Mop/s", "tokio Mop/s");
    for &t in &[1usize, 2, 4, 8] {
        let s = stage_run(t, UNITS, CYCLES);
        let k = tokio_run(t, UNITS, CYCLES);
        println!(
            "{:>8}  {:>12.1}  {:>12.1}  {:>10.1}  {:>10.1}",
            t,
            s * 1000.0,
            k * 1000.0,
            total as f64 / s / 1e6,
            total as f64 / k / 1e6,
        );
    }
}
