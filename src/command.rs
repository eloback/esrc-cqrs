use std::future::Future;

use esrc::error;

/// A handler for a single command type.
///
/// Implementors receive a raw byte payload (the serialized command), perform
/// all aggregate loading, command processing, and event writing, then return
/// an optional byte payload as a reply to the caller.
///
/// The generic parameter `S` is the event store type (e.g., `NatsStore`).
pub trait CommandHandler<S>: Send + Sync + 'static {
    /// The unique name for this command handler.
    ///
    /// This is used to route incoming command messages to the correct handler.
    /// The convention is `<AggregateName>.<CommandName>`.
    fn name(&self) -> &'static str;

    /// Handle a raw incoming command payload, returning a reply payload.
    ///
    /// The handler is responsible for deserializing the command, loading the
    /// aggregate, processing the command, writing the event, and serializing
    /// any reply. Returning an `Err` will cause an error reply to be sent.
    fn handle<'a>(
        &'a self,
        store: &'a mut S,
        payload: &'a [u8],
    ) -> impl Future<Output = error::Result<Vec<u8>>> + Send + 'a;
}
