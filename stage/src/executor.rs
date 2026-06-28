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
use std::cell::Cell;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::Duration;

type TaskDeque = Deque<Arc<ContinuationTask>>;

// Identifies the executor and local deque of the worker running on this thread,
// if any. Lets `inject` push a re-scheduled continuation onto the *current*
// worker's own queue (cache-hot, no global-queue contention) when the schedule
// originates from inside that executor's worker. The raw pointer is only ever
// dereferenced on its owning thread, where the deque outlives every use.
thread_local! {
    static CURRENT_WORKER: Cell<Option<(usize, *const TaskDeque)>> = const { Cell::new(None) };
}

static NEXT_EXECUTOR_ID: AtomicUsize = AtomicUsize::new(1);

/// Clears the `CURRENT_WORKER` thread-local when a worker loop exits (including
/// via unwind), so a dangling deque pointer can never be observed.
struct ClearWorkerOnDrop;
impl Drop for ClearWorkerOnDrop {
    fn drop(&mut self) {
        CURRENT_WORKER.with(|c| c.set(None));
    }
}

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
        // Token stays held; the next continuation inherits it. Clone the cell
        // Arc (whose refcount line is owned by this worker) rather than the
        // Executor (whose refcount is shared by all workers) to avoid global
        // cache-line contention on the hot re-schedule path.
        drop(sched);
        let next_cell = next.cell.clone();
        next_cell.executor().inject(next);
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
    let type_id = task.cell.actor_type_id();

    let poll = {
        // Publish the actor (type tag + pointer) for the duration of this poll so
        // any `ActorContext` deref resolves to it. Restored on drop / unwind.
        let _scope = ActorScope::enter(type_id, ptr);
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
    id: usize,
    injector: Injector<Arc<ContinuationTask>>,
    stealers: Vec<Stealer<Arc<ContinuationTask>>>,
    /// Worker thread handles for unparking. Written once, after spawn; read
    /// lock-free on the hot inject path.
    threads: OnceLock<Vec<Thread>>,
    /// Number of workers currently parked, so inject can skip the unpark syscall
    /// when everyone is already busy.
    parked: AtomicUsize,
    /// Number of live user-facing `Executor` handles (Executor clones, including
    /// those held inside actor cells). When it hits zero we signal shutdown.
    /// Workers hold their own strong `Arc<Shared>` so they do NOT touch this on
    /// the hot path — avoiding global refcount contention every loop iteration.
    handles: AtomicUsize,
    next: AtomicUsize,
    shutdown: AtomicBool,
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        self.0.handles.fetch_add(1, Ordering::Relaxed);
        Executor(self.0.clone())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Executor::new()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        // Last user-facing handle gone: tell the workers to exit and wake them.
        // They hold their own strong Arc<Shared>, so Shared is freed once they
        // observe the flag and drop those refs.
        if self.0.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.shutdown.store(true, Ordering::Release);
            if let Some(threads) = self.0.threads.get() {
                for t in threads {
                    t.unpark();
                }
            }
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
            id: NEXT_EXECUTOR_ID.fetch_add(1, Ordering::Relaxed),
            injector: Injector::new(),
            stealers,
            threads: OnceLock::new(),
            parked: AtomicUsize::new(0),
            handles: AtomicUsize::new(1),
            next: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });

        let mut handles = Vec::with_capacity(n);
        let id = shared.id;
        for (i, deque) in deques.into_iter().enumerate() {
            // Each worker owns a strong Arc<Shared> for its lifetime, so the hot
            // loop never touches the shared refcount.
            let shared = shared.clone();
            let handle = std::thread::Builder::new()
                .name(format!("stage-worker-{i}"))
                .spawn(move || worker_loop(shared, deque, i, id))
                .expect("stage: failed to spawn worker thread");
            handles.push(handle.thread().clone());
        }
        let _ = shared.threads.set(handles);

        Executor(shared)
    }

    /// Push a runnable continuation onto this executor and nudge a parked worker.
    pub(crate) fn inject(&self, task: Arc<ContinuationTask>) {
        // Fast path: if we're running on a worker of *this* executor, push onto
        // its own local deque. Re-scheduled continuations (the common case) then
        // stay cache-hot and off the contended global queue; idle workers can
        // still steal them.
        let local = CURRENT_WORKER.with(|c| c.get());
        if let Some((id, deque)) = local
            && id == self.0.id
        {
            // SAFETY: `deque` points to the local `Deque` owned by this very
            // thread's worker loop, which is live for the duration of the
            // loop; we only ever push to it from its owning thread.
            unsafe { (*deque).push(task) };
            // Do NOT unpark here. This worker is actively running and will
            // process the task itself; idle workers steal surplus via their
            // periodic re-check. Unparking on every local re-schedule causes
            // an unpark-syscall storm whenever any worker is parked (e.g. the
            // staggered endgame), which is what made the thrash workload
            // scale negatively.
            return;
        }

        // External / cross-thread wake (e.g. a timer firing, a cross-executor
        // call, or initial dispatch). Push to the shared queue and wake a parked
        // worker so latency stays low; these are far rarer than re-schedules.
        self.0.injector.push(task);
        if self.0.parked.load(Ordering::Acquire) > 0 {
            self.unpark_one();
        }
    }

    fn unpark_one(&self) {
        if let Some(threads) = self.0.threads.get() {
            let n = threads.len();
            if n > 0 {
                let i = self.0.next.fetch_add(1, Ordering::Relaxed) % n;
                threads[i].unpark();
            }
        }
    }
}

fn worker_loop(shared: Arc<Shared>, local: TaskDeque, idx: usize, id: usize) {
    // Enter the shared reactor so leaf futures (timers/I/O) polled on this
    // thread register with it.
    let _enter = reactor().enter();
    // Publish this worker's executor id + local deque so `inject` can re-schedule
    // continuations locally. Cleared on exit.
    CURRENT_WORKER.with(|c| c.set(Some((id, &local as *const TaskDeque))));
    let _clear = ClearWorkerOnDrop;
    while !shared.shutdown.load(Ordering::Acquire) {
        match find_task(&local, &shared, idx) {
            Some(task) => run_task(task),
            None => {
                // Mark parked, then re-check for work to close the race with a
                // concurrent inject that saw us as not-yet-parked.
                shared.parked.fetch_add(1, Ordering::Release);
                match find_task(&local, &shared, idx) {
                    Some(task) => {
                        shared.parked.fetch_sub(1, Ordering::Release);
                        run_task(task);
                    }
                    None => {
                        // Short timeout as a safety net against missed unparks.
                        std::thread::park_timeout(Duration::from_millis(1));
                        shared.parked.fetch_sub(1, Ordering::Release);
                    }
                }
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
