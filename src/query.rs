use std::future::Future;

use esrc::error;

/// A handler for a single query type.
///
/// Implementors receive a raw byte payload (the serialized query request),
/// perform all necessary data retrieval (e.g., loading a read model or
/// replaying aggregate state), and return a serialized response payload.
///
/// The generic parameter `S` is the event store type (e.g., `NatsStore`).
///
/// Queries are read-only by convention: a `QueryHandler` should never write
/// events or mutate aggregate state. The store reference is therefore shared
/// (`&S`) rather than exclusive (`&mut S`).
///
/// # Usage
///
/// For the common case of loading a single aggregate and projecting its state,
/// use [`crate::nats::AggregateQueryHandler`] rather than implementing this
/// trait directly.
///
/// For custom queries (e.g., cross-aggregate reads or external data sources),
/// implement this trait directly and register the handler with
/// [`crate::CqrsRegistry::register_query`].
pub trait QueryHandler<S>: Send + Sync + 'static {
    /// The unique name for this query handler.
    ///
    /// This is used to route incoming query messages to the correct handler.
    /// The convention is `<AggregateName>.<QueryName>` or `<ReadModel>.<QueryName>`.
    fn name(&self) -> &'static str;

    /// Handle a raw incoming query payload, returning a reply payload.
    ///
    /// The handler is responsible for deserializing the query, loading the
    /// required data, and serializing the response. Returning an `Err` will
    /// cause an error reply to be sent to the caller.
    fn handle<'a>(
        &'a self,
        store: &'a S,
        payload: &'a [u8],
    ) -> impl Future<Output = error::Result<Vec<u8>>> + Send + 'a;
}
