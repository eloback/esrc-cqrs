use std::sync::Arc;

use async_nats::service::ServiceExt;
use futures::StreamExt;
use tracing::instrument;

use esrc::error::{self, Error};
use super::aggregate_command_handler::CommandReply;

use crate::registry::ErasedCommandHandler;

/// Subject prefix used for all command messages.
///
/// Full subject pattern: `<service_name>.<handler_name>`
/// where `handler_name` is the value returned by `CommandHandler::name()`.
pub const CMD_SERVICE_VERSION: &str = "0.1.0";

/// NATS command dispatcher.
///
/// Registers all command handlers as endpoints on a single NATS service,
/// using core NATS request/reply. Each handler name becomes one endpoint
/// within the service group named `<prefix>-cqrs`.
///
/// A mutable clone of the store is passed into each handler invocation so
/// that publishing events works correctly (since `Publish` requires `&mut self`).
pub struct NatsCommandDispatcher {
    /// The NATS client used to create the service.
    client: async_nats::Client,
    /// The service group name (e.g. `"myapp-cqrs"`).
    service_name: String,
}

impl NatsCommandDispatcher {
    /// Create a new dispatcher using the given NATS client and service name.
    pub fn new(client: async_nats::Client, service_name: impl Into<String>) -> Self {
        Self {
            client,
            service_name: service_name.into(),
        }
    }

    /// Start the command dispatcher and listen for incoming commands.
    ///
    /// This method creates one NATS service endpoint per registered command
    /// handler. Each endpoint is named after the handler's `name()`. The
    /// dispatcher runs until an error occurs or the NATS connection is closed.
    ///
    /// A fresh clone of `store` is passed into each request so that handlers
    /// can use `&mut store` for publishing without contention.
    #[instrument(skip_all, level = "debug")]
    pub async fn run<S>(
        &self,
        store: S,
        handlers: &[Arc<dyn ErasedCommandHandler<S>>],
    ) -> error::Result<()>
    where
        S: Clone + Send + Sync + 'static,
    {
        let service = self
            .client
            .service_builder()
            .description("esrc-cqrs command dispatcher")
            .start(&self.service_name, CMD_SERVICE_VERSION)
            .await
            .map_err(|e| Error::Internal(e.into()))?;

        let group = service.group(&self.service_name);

        // Build one endpoint per handler and spawn a task for each.
        let mut tasks = tokio::task::JoinSet::new();

        for handler in handlers {
            let handler = Arc::clone(handler);
            let mut store = store.clone();

            let mut endpoint = group
                .endpoint(handler.name())
                .await
                .map_err(|e| Error::Internal(e.into()))?;

            tasks.spawn(async move {
                while let Some(request) = endpoint.next().await {
                    let payload = request.message.payload.as_ref();
                    match handler.handle_erased(&mut store, payload).await {
                        Ok(reply) => {
                            let _ = request.respond(Ok(reply.into())).await;
                        },
                        Err(e) => {
                            // Encode the failure as a CommandReply so callers always
                            // deserialize the same shape regardless of outcome.
                            let failure = CommandReply {
                                id: uuid::Uuid::nil(),
                                success: false,
                                error: Some(crate::error::Error::Internal(format!("{e}"))),
                            };
                            let body = serde_json::to_vec(&failure).unwrap_or_default();
                            let _ = request.respond(Ok(body.into())).await;
                        },
                    }
                }
                error::Result::Ok(())
            });
        }

        // Wait for all endpoint tasks; return the first error encountered.
        while let Some(result) = tasks.join_next().await {
            result.map_err(|e| Error::Internal(e.into()))??;
        }

        Ok(())
    }
}

/// Build the full NATS subject for a command endpoint.
///
/// Pattern: `<service_name>.<handler_name>`
pub fn command_subject(service_name: &str, handler_name: &str) -> String {
    format!("{service_name}.{handler_name}")
}
