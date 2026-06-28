//! [`ActorCell`]: owns an actor's state plus its per-actor scheduling data.

use crate::executor::{ContinuationTask, Executor};
use core::cell::UnsafeCell;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Per-actor scheduling state, protected by the cell's mutex.
///
/// `active` is a single token: when set, exactly one continuation of this actor
/// is either enqueued on its executor or currently being polled. The token is
/// passed from one continuation to the next as they suspend or complete, which
/// is what enforces actor isolation while still allowing continuations to
/// migrate freely between worker threads.
#[derive(Default)]
pub struct CellSched {
    pub active: bool,
    /// Ready-but-blocked continuations, in FIFO order.
    pub pending: VecDeque<Arc<ContinuationTask>>,
}

/// Owns one actor instance and the machinery to schedule its continuations on a
/// particular executor.
pub struct ActorCell<A> {
    state: UnsafeCell<A>,
    sched: Mutex<CellSched>,
    executor: Executor,
}

// SAFETY: access to `state` is serialized by the `active` token in `sched`; at
// most one thread ever holds `&mut A` at a time, and the mutex establishes the
// happens-before edges needed when the token migrates between threads. We
// require `A: Send` because the state itself moves (logically) across threads.
unsafe impl<A: Send> Send for ActorCell<A> {}
unsafe impl<A: Send> Sync for ActorCell<A> {}

impl<A> ActorCell<A> {
    pub fn new(state: A, executor: Executor) -> Self {
        ActorCell {
            state: UnsafeCell::new(state),
            sched: Mutex::new(CellSched::default()),
            executor,
        }
    }
}

/// Type-erased view of an [`ActorCell`], so continuations can reference their
/// owning cell without carrying the actor type around.
pub trait AnyCell: Send + Sync {
    fn state_ptr(&self) -> *mut ();
    fn actor_type_id(&self) -> core::any::TypeId;
    fn sched(&self) -> &Mutex<CellSched>;
    fn executor(&self) -> &Executor;
}

impl<A: Send + 'static> AnyCell for ActorCell<A> {
    fn state_ptr(&self) -> *mut () {
        self.state.get() as *mut ()
    }
    fn actor_type_id(&self) -> core::any::TypeId {
        core::any::TypeId::of::<A>()
    }
    fn sched(&self) -> &Mutex<CellSched> {
        &self.sched
    }
    fn executor(&self) -> &Executor {
        &self.executor
    }
}
