use std::future::Future;

use esrc::error;
use serde::{de::DeserializeOwned, Serialize};

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

/// A handler for a typed command dispatched under a single NATS service name.
///
/// Unlike [`CommandHandler`], which works on raw byte payloads, this trait
/// receives an already-deserialized command value `C`. The user controls the
/// reply shape entirely; the returned `Vec<u8>` is sent back verbatim.
///
/// The generic parameter `S` is the event store type (e.g., `NatsStore`).
/// The generic parameter `C` is the command enum type; it must implement
/// `Serialize` and `DeserializeOwned` so the dispatcher can decode incoming
/// NATS payloads before forwarding them here.
///
/// # Usage
///
/// For registering a single handler that covers an entire API group or vertical
/// slice, implement this trait and wrap the value with [`crate::nats::command::ServiceCommandHandler`].
/// All commands are received under the same NATS endpoint named by `name()`.
pub trait NatsServiceCommandHandler<S, C>: Send + Sync + 'static
where
    C: Serialize + DeserializeOwned + Send + Sync,
{
    /// The unique NATS service/endpoint name for this handler.
    ///
    /// All commands of type `C` are routed to the NATS endpoint with this name.
    fn name(&self) -> &'static str;

    /// Handle a deserialized command, returning a serialized reply payload.
    ///
    /// The reply bytes are sent back to the caller verbatim. Use
    /// [`crate::nats::command::ServiceCommandReply`] as a convenience reply
    /// envelope, or return any other serializable structure.
    fn handle<'a>(
        &'a self,
        store: &'a mut S,
        command: C,
    ) -> impl Future<Output = error::Result<Vec<u8>>> + Send + 'a;
}
