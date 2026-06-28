//! The [`ActorContext`] primitive and the thread-local "actor scope".
//!
//! The central trick that makes Swift-style reentrancy work in Rust is that an
//! actor method never *stores* `&mut Actor` across a suspension point. Instead,
//! the executor publishes a pointer to the actor's state into a thread-local
//! immediately before polling a continuation, and clears it afterwards. An
//! [`ActorContext`] is a zero-sized handle that re-derives `&mut Actor` from
//! that thread-local on every field access.
//!
//! Because the handle holds no real borrow, it can live across `.await` points
//! (that is exactly what actor methods need) while the *actual* borrow only
//! exists for the duration of a single dereference inside a single poll.

use core::cell::Cell;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use core::ptr;

thread_local! {
    /// Pointer to the actor state currently being polled on this thread, or
    /// null when no actor continuation is executing.
    static CURRENT: Cell<*mut ()> = const { Cell::new(ptr::null_mut()) };
}

/// RAII guard that publishes an actor pointer for the duration of one poll.
///
/// On drop it restores the previous value, which keeps things correct even if a
/// poll unwinds through a panic.
pub(crate) struct ActorScope {
    prev: *mut (),
}

impl ActorScope {
    pub(crate) fn enter(ptr: *mut ()) -> Self {
        let prev = CURRENT.with(|c| {
            let prev = c.get();
            c.set(ptr);
            prev
        });
        ActorScope { prev }
    }
}

impl Drop for ActorScope {
    fn drop(&mut self) {
        CURRENT.with(|c| c.set(self.prev));
    }
}

/// Opaque handle granting exclusive access to an actor's state while a
/// continuation is being polled.
///
/// `ActorContext` is the primitive abstraction Stage uses internally. It is
/// exposed to users only inside `#[stage::actor_fn]` helpers, where there is no
/// `self` receiver to stand in for the actor.
///
/// Properties:
///
/// * It dereferences to the actor, giving exclusive mutable access *while
///   polling*.
/// * It cannot be constructed by users (the constructor is crate-private).
/// * It cannot be cloned (no `Clone` impl).
/// * Its lifetime parameter is invariant, so it cannot be smuggled out to a
///   longer lifetime.
///
/// The lifetime `'a` ties a context to a single poll at the type level; the
/// runtime additionally guarantees, via per-actor scheduling, that at most one
/// continuation of a given actor is ever polled at a time, so the re-derived
/// `&mut Actor` is never aliased.
pub struct ActorContext<'a, A> {
    // Invariant in `'a` so `ActorContext<'long>` cannot coerce to a shorter or
    // longer lifetime behind the user's back.
    _inv: PhantomData<fn(&'a ()) -> &'a ()>,
    _actor: PhantomData<A>,
}

impl<'a, A> ActorContext<'a, A> {
    /// Construct a context. Crate-private: only Stage's generated glue calls
    /// this, always immediately before awaiting a lowered actor body.
    pub(crate) fn new() -> Self {
        ActorContext {
            _inv: PhantomData,
            _actor: PhantomData,
        }
    }

    #[inline]
    fn actor_ptr() -> *mut A {
        let p = CURRENT.with(|c| c.get());
        debug_assert!(
            !p.is_null(),
            "stage: ActorContext dereferenced outside of an actor poll; \
             this usually means a context escaped its continuation"
        );
        p as *mut A
    }
}

impl<'a, A> Deref for ActorContext<'a, A> {
    type Target = A;

    #[inline]
    fn deref(&self) -> &A {
        // SAFETY: the runtime publishes a valid `*mut A` for the actor whose
        // continuation is currently being polled, and guarantees that exactly
        // one continuation of that actor runs at a time. The returned reference
        // is bounded by `&self`, which cannot outlive the poll in practice.
        unsafe { &*Self::actor_ptr() }
    }
}

impl<'a, A> DerefMut for ActorContext<'a, A> {
    #[inline]
    fn deref_mut(&mut self) -> &mut A {
        // SAFETY: see `Deref`. Exclusive access is upheld by the single-active-
        // continuation-per-actor scheduling invariant.
        unsafe { &mut *Self::actor_ptr() }
    }
}
