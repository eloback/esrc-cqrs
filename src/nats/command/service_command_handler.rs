use serde::de::DeserializeOwned;

use esrc::error::{self, Error};

use crate::command::NatsServiceCommandHandler;

use serde::{Deserialize, Serialize};

/// A convenience reply envelope for service command handlers.
///
/// This is an opt-in type; users may return any serializable bytes from their
/// [`NatsServiceCommandHandler::handle`] implementation. When used, `R` should
/// match the type that the caller expects to deserialize on the other side.
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceCommandReply<R> {
    /// Whether the command succeeded.
    pub success: bool,
    /// The response data, present when `success` is true.
    pub data: Option<R>,
    /// The structured CQRS error, present only when `success` is false.
    pub error: Option<crate::Error>,
}

impl ServiceCommandReply<()> {
    /// Construct a successful reply with no data payload.
    pub fn ok() -> Self {
        Self {
            success: true,
            data: Some(()),
            error: None,
        }
    }
}

impl<R> ServiceCommandReply<R> {
    /// Construct a successful reply carrying the given data.
    pub fn ok_with(data: R) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// Construct a failure reply carrying the given CQRS error.
    pub fn err(e: crate::Error) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(e),
        }
    }
}

/// Adapter that wraps a [`NatsServiceCommandHandler`] into a [`crate::command::CommandHandler`].
///
/// All variants of `C` are received under a single NATS endpoint named by
/// [`NatsServiceCommandHandler::name`]. The raw payload is deserialized as `C`
/// and forwarded to the inner handler. The reply bytes are returned verbatim.
pub struct ServiceCommandHandler<H, C> {
    handler: H,
    name: &'static str,
    _marker: std::marker::PhantomData<C>,
}

impl<H, C> ServiceCommandHandler<H, C>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new adapter wrapping the given handler.
    ///
    /// The endpoint name is taken from [`NatsServiceCommandHandler::name`] so
    /// there is a single registration call.
    pub fn new<S>(handler: H) -> Self
    where
        H: NatsServiceCommandHandler<S, C>,
    {
        let name = handler.name();
        Self {
            handler,
            name,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, H, C> crate::command::CommandHandler<S> for ServiceCommandHandler<H, C>
where
    S: Send + Sync + 'static,
    H: NatsServiceCommandHandler<S, C> + Send + Sync + 'static,
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    async fn handle<'a>(&'a self, store: &'a mut S, payload: &'a [u8]) -> error::Result<Vec<u8>> {
        let command: C = serde_json::from_slice(payload).map_err(|e| Error::Format(e.into()))?;
        self.handler.handle(store, command).await
    }
}
