//! # Stage — Reentrant actors for Rust
//!
//! Stage is a research prototype actor runtime whose defining property is
//! **actor reentrancy** (semantics inspired by Swift actors, implementation its
//! own Rust execution model):
//!
//! * Only one continuation belonging to an actor executes at any instant
//!   (isolation).
//! * Whenever an actor method suspends (`.await`), another queued method of the
//!   same actor may run.
//! * When the suspended operation becomes ready, execution resumes from the
//!   suspension point with exclusive access to the actor.
//!
//! Users write ordinary async methods taking `&mut self`:
//!
//! ```ignore
//! #[stage::actor]
//! #[derive(Default)]
//! struct Counter { value: usize }
//!
//! #[stage::actor]
//! impl Counter {
//!     async fn increment(&mut self) {
//!         self.value += 1;
//!         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//!         self.value += 1;
//!     }
//!     async fn get(&mut self) -> usize { self.value }
//! }
//!
//! let counter = Counter::spawn();
//! let task = counter.increment();
//! // ... while `increment` is suspended, `get` can run ...
//! ```
//!
//! See `BRIEF.md` and the integration tests for the full behavioural contract.
//!
//! ## Design notes & deviations from the brief
//!
//! * `#[stage::actor]` is applied to **both** the struct and its `impl` block.
//!   A proc-macro attribute can only see the item it is attached to, so the
//!   impl-level attribute is what lets Stage generate the typed [`ActorRef`]
//!   methods. The struct-level attribute generates `spawn`/`spawn_on`.
//! * `spawn()` requires the actor to implement [`Default`]; use
//!   `spawn_with(state)` to provide an initial value explicitly.
//! * Method parameters are taken **by value** (e.g. `db: ActorRef<Database>`,
//!   not `&ActorRef<Database>`). A continuation future must be `'static`, so it
//!   cannot hold borrows across suspension. `ActorRef` is cheap to clone.

mod cell;
mod context;
mod executor;
mod handle;

use std::sync::{Arc, OnceLock};

pub use context::ActorContext;
pub use executor::Executor;
pub use handle::{AbortHandle, Cancelled, JoinHandle};

/// A cloneable reference to a spawned actor. Generated method implementations
/// are attached to `ActorRef<YourActor>` by `#[stage::actor]`.
pub struct ActorRef<A> {
    cell: Arc<cell::ActorCell<A>>,
}

impl<A> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        ActorRef {
            cell: self.cell.clone(),
        }
    }
}

impl<A: Send + 'static> ActorRef<A> {
    /// Spawn an actor with an explicit initial state on the given executor.
    #[doc(hidden)]
    pub fn __spawn_with(executor: &Executor, state: A) -> Self {
        ActorRef {
            cell: Arc::new(cell::ActorCell::new(state, executor.clone())),
        }
    }

    /// Spawn an actor with `Default` state on the given executor.
    #[doc(hidden)]
    pub fn __spawn_default(executor: &Executor) -> Self
    where
        A: Default,
    {
        Self::__spawn_with(executor, A::default())
    }

    #[doc(hidden)]
    pub fn __cell(&self) -> Arc<cell::ActorCell<A>> {
        self.cell.clone()
    }
}

/// The process-wide default executor, created on first use.
pub fn default_executor() -> Executor {
    static DEFAULT: OnceLock<Executor> = OnceLock::new();
    DEFAULT.get_or_init(Executor::new).clone()
}

pub use stage_macros::{actor, actor_fn};

/// Internal glue called by generated code. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use crate::cell::{ActorCell, AnyCell};
    pub use crate::context::ActorContext;
    use crate::executor::{schedule, ContinuationTask};
    use crate::handle::{AbortHandle, JoinHandle};
    use std::future::Future;
    use std::sync::Arc;

    /// Construct an [`ActorContext`]; called immediately before awaiting a
    /// lowered actor body, while the executor has published the actor pointer.
    pub fn __ctx<'a, A>() -> ActorContext<'a, A> {
        ActorContext::new()
    }

    /// Wrap a lowered actor body into a continuation, schedule it eagerly, and
    /// return a [`JoinHandle`] for its result.
    pub fn spawn_method<A, Fut, R>(cell: Arc<ActorCell<A>>, fut: Fut) -> JoinHandle<R>
    where
        A: Send + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let wrapped = async move {
            let r = fut.await;
            let _ = tx.send(r);
        };
        let task = ContinuationTask::new(cell as Arc<dyn AnyCell>, Box::pin(wrapped));
        let abort = AbortHandle::new(Arc::downgrade(&task));
        schedule(task);
        JoinHandle::new(rx, abort)
    }
}
