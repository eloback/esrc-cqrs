use std::future::Future;

use esrc::error;

/// A handler that drives an event projector subscription.
///
/// Each registered projector is run as an independent task. The handler
/// encapsulates the subscription logic (durable or transient) for the
/// underlying store backend.
///
/// The generic parameter `S` is the event store type (e.g., `NatsStore`).
pub trait ProjectorHandler<S>: Send + 'static {
    /// The unique durable name for this projector.
    ///
    /// Used as the durable consumer name so that the projector resumes from
    /// its last position across restarts.
    fn name(&self) -> &'static str;

    /// Run the projector against the given store, consuming events indefinitely.
    ///
    /// This future is expected to run until an error occurs or the process
    /// shuts down. It drives the underlying [`esrc::project::Project`]
    /// implementation.
    fn run<'a>(&'a self, store: &'a S) -> impl Future<Output = error::Result<()>> + Send + 'a;
}
