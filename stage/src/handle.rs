//! [`JoinHandle`] and [`AbortHandle`]: awaiting and cancelling invocations.

use crate::executor::{schedule, ContinuationTask};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A cloneable handle that can request cancellation of an in-flight invocation.
#[derive(Clone)]
pub struct AbortHandle {
    task: Weak<ContinuationTask>,
}

impl AbortHandle {
    pub(crate) fn new(task: Weak<ContinuationTask>) -> Self {
        AbortHandle { task }
    }

    /// Request cancellation. If the invocation is still alive it is marked
    /// cancelled and re-scheduled so the executor can drop its suspended state
    /// promptly. A no-op if the invocation already finished.
    pub fn abort(&self) {
        if let Some(task) = self.task.upgrade() {
            task.cancelled.store(true, Ordering::Release);
            schedule(task);
        }
    }
}

/// Awaitable handle to a running actor invocation.
///
/// Invocations start executing eagerly (at call time), so a `JoinHandle`
/// represents work that is already in flight. Awaiting it yields the method's
/// return value. Dropping it detaches (the invocation keeps running).
///
/// If the invocation was cancelled or panicked, awaiting the handle panics; use
/// [`JoinHandle::try_join`] to observe that case without panicking.
pub struct JoinHandle<R> {
    rx: oneshot::Receiver<R>,
    abort: AbortHandle,
}

/// Error returned by [`JoinHandle::try_join`] when an invocation did not produce
/// a value (it was cancelled or panicked).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stage: actor invocation was cancelled or panicked")
    }
}

impl std::error::Error for Cancelled {}

impl<R> JoinHandle<R> {
    pub(crate) fn new(rx: oneshot::Receiver<R>, abort: AbortHandle) -> Self {
        JoinHandle { rx, abort }
    }

    /// Request cancellation without consuming the handle.
    pub fn abort(&self) {
        self.abort.abort();
    }

    /// Cancel the invocation, consuming the handle.
    pub fn cancel(self) {
        self.abort.abort();
    }

    /// Obtain a standalone abort handle.
    pub fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }

    /// Await the result, returning `Err(Cancelled)` instead of panicking if the
    /// invocation was cancelled or panicked.
    pub async fn try_join(self) -> Result<R, Cancelled> {
        self.rx.await.map_err(|_| Cancelled)
    }
}

impl<R> Future for JoinHandle<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => {
                panic!("stage: actor invocation was cancelled or panicked")
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
