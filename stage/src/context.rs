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
use core::any::TypeId;
use core::ops::{Deref, DerefMut};

thread_local! {
    /// The actor currently being polled on this thread: its `TypeId` and a
    /// pointer to its state. `None` when no actor continuation is executing.
    ///
    /// The `TypeId` lets a dereference verify it is reading the actor it was
    /// created for. A stray context (escaped to another thread, or used after
    /// its poll) therefore *panics* on deref rather than risking a type-confused
    /// or dangling access — misuse is always safe, never UB.
    static CURRENT: Cell<Option<(TypeId, *mut ())>> = const { Cell::new(None) };
}

/// RAII guard that publishes the current actor for the duration of one poll.
///
/// On drop it restores the previous value, which keeps nested polls and
/// panic-unwinds correct.
pub(crate) struct ActorScope {
    prev: Option<(TypeId, *mut ())>,
}

impl ActorScope {
    pub(crate) fn enter(type_id: TypeId, ptr: *mut ()) -> Self {
        let prev = CURRENT.with(|c| c.replace(Some((type_id, ptr))));
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
}

impl<'a, A: 'static> ActorContext<'a, A> {
    /// Resolve the pointer to `A`'s state, verifying we are inside the poll of an
    /// actor of type `A`. Panics (never UB) if the context is used outside its
    /// poll or on the wrong actor.
    #[inline]
    fn actor_ptr(&self) -> *mut A {
        match CURRENT.with(|c| c.get()) {
            Some((tid, p)) if tid == TypeId::of::<A>() => p as *mut A,
            _ => panic!(
                "stage: ActorContext used outside of its actor poll \
                 (it must not escape its continuation or move to another thread)"
            ),
        }
    }
}

impl<'a, A: 'static> Deref for ActorContext<'a, A> {
    type Target = A;

    #[inline]
    fn deref(&self) -> &A {
        // SAFETY: `actor_ptr` verifies (via the thread-local TypeId tag) that we
        // are inside the poll of an actor of type `A`, where the runtime has
        // published a valid `*mut A`. The single-active-continuation-per-actor
        // invariant guarantees no aliasing. The returned reference is bounded by
        // `&self`, which cannot outlive the poll in practice.
        unsafe { &*self.actor_ptr() }
    }
}

impl<'a, A: 'static> DerefMut for ActorContext<'a, A> {
    #[inline]
    fn deref_mut(&mut self) -> &mut A {
        // SAFETY: see `Deref`; exclusive access is upheld by the single-active-
        // continuation-per-actor scheduling invariant.
        unsafe { &mut *self.actor_ptr() }
    }
}
