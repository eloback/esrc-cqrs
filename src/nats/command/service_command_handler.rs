use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};

use esrc::error::{self, Error};

use crate::command::{CommandHandler, NatsServiceCommandHandler};

/// An adapter that bridges a [`NatsServiceCommandHandler`] into the generic
/// [`CommandHandler`] interface expected by the registry and dispatcher.
///
/// `ServiceCommandHandler<H, C>` holds a user-defined handler `H` that
/// implements `NatsServiceCommandHandler<S, C>` and exposes it as a single
/// NATS endpoint. All variants of `C` are received under that one endpoint;
/// the adapter deserializes the payload and delegates to the user handler.
///
/// `H` is the user handler type.
/// `C` is the typed command enum or struct; it must be JSON-serializable.
pub struct ServiceCommandHandler<H, C> {
    handler: H,
    _phantom: PhantomData<C>,
}

impl<H, C> ServiceCommandHandler<H, C> {
    /// Create a new adapter wrapping the given handler.
    ///
    /// The endpoint name is taken from `handler.name()` so there is a single
    /// registration call and the handler controls its own routing key.
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<S, H, C> CommandHandler<S> for ServiceCommandHandler<H, C>
where
    S: Send + Sync + 'static,
    H: NatsServiceCommandHandler<S, C> + Send + Sync + 'static,
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.handler.name()
    }

    async fn handle<'a>(
        &'a self,
        store: &'a mut S,
        payload: &'a [u8],
    ) -> error::Result<Vec<u8>> {
        let command: C =
            serde_json::from_slice(payload).map_err(|e| Error::Format(e.into()))?;
        self.handler.handle(store, command).await
    }
}
