//! The Stage executor: a work-stealing scheduler whose schedulable unit is the
//! *continuation*, plus the per-actor token logic that enforces isolation and
//! reentrancy.
//!
//! ## Scheduling policy
//!
//! * Each [`Executor`] owns a fixed pool of worker threads, a global injector
//!   queue, and one work-stealing deque per worker (via `crossbeam-deque`).
//! * The schedulable unit is a [`ContinuationTask`] — one in-flight actor
//!   method invocation. Continuations are pushed onto the injector and may be
//!   stolen by any idle worker, so they migrate between threads freely.
//! * Actors themselves are *never* scheduled. Instead each actor cell holds a
//!   single `active` token (see [`crate::cell::CellSched`]). A continuation may
//!   only run while it holds its actor's token, which guarantees that at most
//!   one continuation per actor executes at any instant (isolation).
//! * When a continuation suspends (`Poll::Pending`) or finishes, it releases
//!   the token, handing it to the next FIFO-queued continuation of the same
//!   actor. Releasing on suspension is exactly what enables reentrancy: another
//!   queued method runs in the gap.
//! * Executor affinity is preserved because a continuation is always re-injected
//!   onto the executor named by its own actor cell.
//!
//! ## Leaf futures / timers
//!
//! Stage schedules actor continuations itself, but delegates leaf futures
//! (timers, I/O — e.g. `tokio::time::sleep`) to a shared background Tokio
//! runtime that every worker thread *enters*. When such a leaf becomes ready it
//! invokes the continuation's waker, which re-schedules the continuation onto
//! its Stage executor.

use crate::cell::AnyCell;
use crate::context::ActorScope;
use crossbeam_deque::{Injector, Steal, Stealer, Worker as Deque};
use parking_lot::Mutex;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::Duration;

/// One in-flight actor method invocation.
pub struct ContinuationTask {
    pub(crate) cell: Arc<dyn AnyCell>,
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    /// Set while the task sits in *some* queue, to coalesce redundant wakes.
    queued: AtomicBool,
    pub(crate) cancelled: AtomicBool,
}

impl ContinuationTask {
    pub fn new(
        cell: Arc<dyn AnyCell>,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Arc<Self> {
        Arc::new(ContinuationTask {
            cell,
            future: Mutex::new(Some(future)),
            queued: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
        })
    }
}

impl Wake for ContinuationTask {
    fn wake(self: Arc<Self>) {
        schedule(self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        schedule(self.clone());
    }
}

/// Make a continuation runnable, respecting its actor's token.
///
/// If the actor is idle, claim the token and inject the task onto its executor.
/// Otherwise park it in the actor's FIFO pending queue; it will be promoted when
/// the current holder releases the token.
pub fn schedule(task: Arc<ContinuationTask>) {
    // Coalesce duplicate wakes: if already queued, do nothing.
    if task.queued.swap(true, Ordering::AcqRel) {
        return;
    }
    let cell = task.cell.clone();
    let mut sched = cell.sched().lock();
    if sched.active {
        sched.pending.push_back(task);
    } else {
        sched.active = true;
        drop(sched);
        cell.executor().inject(task);
    }
}

/// Release the actor token held by a just-finished or just-suspended
/// continuation, handing it to the next FIFO continuation if any.
fn release_token(cell: &Arc<dyn AnyCell>) {
    let mut sched = cell.sched().lock();
    if let Some(next) = sched.pending.pop_front() {
        // Token stays held; the next continuation inherits it.
        drop(sched);
        let executor = next.cell.executor().clone();
        executor.inject(next);
    } else {
        sched.active = false;
    }
}

/// Poll a continuation once. Called only by worker threads, and only when the
/// task holds its actor's token.
pub fn run_task(task: Arc<ContinuationTask>) {
    // Allow future wakes to re-queue this task while we poll it.
    task.queued.store(false, Ordering::Release);

    let mut guard = task.future.lock();

    // Future already taken (a stray wake after completion): release the token
    // this stray acquisition implicitly took, then bail.
    if guard.is_none() {
        drop(guard);
        release_token(&task.cell);
        return;
    }

    // Cancellation: drop the future (running destructors of suspended locals,
    // e.g. cancelling a sleep), then release the token. State mutated before
    // the cancellation point is preserved.
    if task.cancelled.load(Ordering::Acquire) {
        *guard = None;
        drop(guard);
        release_token(&task.cell);
        return;
    }

    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let ptr = task.cell.state_ptr();

    let poll = {
        // Publish the actor pointer for the duration of this poll so that any
        // `ActorContext` deref resolves to it. Restored on drop / unwind.
        let _scope = ActorScope::enter(ptr);
        let future = guard.as_mut().unwrap();
        catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx)))
    };

    match poll {
        Ok(Poll::Ready(())) => {
            *guard = None;
            drop(guard);
            release_token(&task.cell);
        }
        Ok(Poll::Pending) => {
            // Suspended: keep the future, release the token so other
            // continuations of this actor may run (reentrancy). The waker will
            // re-schedule us when the leaf becomes ready.
            drop(guard);
            release_token(&task.cell);
        }
        Err(_panic) => {
            // Panic isolation: the worker survives. The invocation's result
            // channel is dropped (its awaiter observes a cancellation), and the
            // actor remains usable for subsequent invocations. State mutated
            // before the panic persists.
            *guard = None;
            drop(guard);
            release_token(&task.cell);
        }
    }
}

/// A handle to a Stage executor (cheaply cloneable; refcounted).
pub struct Executor(Arc<Shared>);

struct Shared {
    injector: Injector<Arc<ContinuationTask>>,
    stealers: Vec<Stealer<Arc<ContinuationTask>>>,
    threads: Mutex<Vec<Thread>>,
    next: AtomicUsize,
    shutdown: AtomicBool,
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        Executor(self.0.clone())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Executor::new()
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for t in self.threads.lock().iter() {
            t.unpark();
        }
    }
}

impl Executor {
    /// Create an executor with one worker thread per available core.
    pub fn new() -> Self {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Executor::with_threads(n)
    }

    /// Create an executor with `n` worker threads.
    pub fn with_threads(n: usize) -> Self {
        let n = n.max(1);
        let deques: Vec<Deque<Arc<ContinuationTask>>> =
            (0..n).map(|_| Deque::new_fifo()).collect();
        let stealers = deques.iter().map(|d| d.stealer()).collect();

        let shared = Arc::new(Shared {
            injector: Injector::new(),
            stealers,
            threads: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });

        let mut handles = Vec::with_capacity(n);
        for (i, deque) in deques.into_iter().enumerate() {
            let weak = Arc::downgrade(&shared);
            let handle = std::thread::Builder::new()
                .name(format!("stage-worker-{i}"))
                .spawn(move || worker_loop(weak, deque, i))
                .expect("stage: failed to spawn worker thread");
            handles.push(handle.thread().clone());
        }
        *shared.threads.lock() = handles;

        Executor(shared)
    }

    /// Push a runnable continuation onto this executor and nudge a worker.
    pub(crate) fn inject(&self, task: Arc<ContinuationTask>) {
        self.0.injector.push(task);
        let n = self.0.stealers.len();
        if n > 0 {
            let i = self.0.next.fetch_add(1, Ordering::Relaxed) % n;
            if let Some(t) = self.0.threads.lock().get(i) {
                t.unpark();
            }
        }
    }
}

fn worker_loop(weak: Weak<Shared>, local: Deque<Arc<ContinuationTask>>, idx: usize) {
    // Enter the shared reactor so leaf futures (timers/I/O) polled on this
    // thread register with it.
    let _enter = reactor().enter();
    loop {
        let shared = match weak.upgrade() {
            Some(s) => s,
            None => break, // executor dropped
        };
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }
        match find_task(&local, &shared, idx) {
            Some(task) => {
                drop(shared);
                run_task(task);
            }
            None => {
                drop(shared);
                // Park with a short timeout as a safety net against missed
                // unparks; `inject` unparks us when work arrives.
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

fn find_task(
    local: &Deque<Arc<ContinuationTask>>,
    shared: &Shared,
    idx: usize,
) -> Option<Arc<ContinuationTask>> {
    if let Some(t) = local.pop() {
        return Some(t);
    }
    // Steal a batch from the global injector.
    loop {
        match shared.injector.steal_batch_and_pop(local) {
            Steal::Success(t) => return Some(t),
            Steal::Retry => continue,
            Steal::Empty => break,
        }
    }
    // Steal from siblings.
    for (i, stealer) in shared.stealers.iter().enumerate() {
        if i == idx {
            continue;
        }
        loop {
            match stealer.steal_batch_and_pop(local) {
                Steal::Success(t) => return Some(t),
                Steal::Retry => continue,
                Steal::Empty => break,
            }
        }
    }
    None
}

/// Shared background Tokio runtime used purely as a reactor (timers + I/O).
fn reactor() -> &'static tokio::runtime::Runtime {
    static R: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    R.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("stage-reactor")
            .build()
            .expect("stage: failed to build reactor runtime")
    })
}
