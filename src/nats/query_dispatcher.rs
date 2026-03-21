use std::sync::Arc;

use async_nats::service::ServiceExt;
use futures::StreamExt;
use tracing::instrument;

use esrc::error::{self, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::registry::ErasedQueryHandler;

/// A standard query envelope sent over NATS.
///
/// The query payload wraps only the aggregate ID because the handler already
/// knows which aggregate type and query to execute. If a query requires
/// additional parameters they can be placed alongside `id` in a custom
/// request type by implementing [`QueryHandler`] directly.
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryEnvelope {
    /// The ID of the aggregate instance to query.
    pub id: Uuid,
}

/// A standard reply envelope returned after processing a query.
///
/// On success the inner `data` field contains the serialized response value
/// (a JSON object). On failure `success` is false and `error` is set.
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryReply {
    /// Whether the query succeeded.
    pub success: bool,
    /// The query result serialized as a JSON value, present when `success` is true.
    pub data: Option<serde_json::Value>,
    /// The structured CQRS error, present only when `success` is false.
    pub error: Option<crate::Error>,
}

/// Version string for the NATS query service group.
pub const QUERY_SERVICE_VERSION: &str = "0.1.0";

/// NATS query dispatcher.
///
/// Registers all query handlers as endpoints on a single NATS service,
/// using core NATS request/reply. Each handler name becomes one endpoint
/// within the service group named `<service_name>`.
///
/// The store reference is shared (`&S`) across all query handlers because
/// queries are read-only by convention.
///
/// # Subject Pattern
///
/// Subjects follow the pattern `<service_name>.<handler_name>`, where
/// `handler_name` is the value returned by [`crate::query::QueryHandler::name`].
/// Use [`query_subject`] to build the subject string for a given handler.
///
/// # Reply Shape
///
/// Each endpoint returns a serialized [`crate::nats::QueryReply`]. On success,
/// `success` is `true` and `data` contains the handler's response as a JSON
/// value. On failure, `success` is `false` and `error` is set to a
/// [`crate::Error`] describing the problem.
pub struct NatsQueryDispatcher {
    /// The NATS client used to create the service.
    client: async_nats::Client,
    /// The service group name (e.g. `"myapp-query"`).
    service_name: String,
}

impl NatsQueryDispatcher {
    /// Create a new dispatcher using the given NATS client and service name.
    pub fn new(client: async_nats::Client, service_name: impl Into<String>) -> Self {
        Self {
            client,
            service_name: service_name.into(),
        }
    }

    /// Start the query dispatcher and listen for incoming queries.
    ///
    /// This method creates one NATS service endpoint per registered query
    /// handler. Each endpoint is named after the handler's `name()`. The
    /// dispatcher runs until an error occurs or the NATS connection is closed.
    ///
    /// The store is shared across all endpoint tasks via [`Arc`] because
    /// query handlers only require a shared reference.
    #[instrument(skip_all, level = "debug")]
    pub async fn run<S>(
        &self,
        store: S,
        handlers: &[Arc<dyn ErasedQueryHandler<S>>],
    ) -> error::Result<()>
    where
        S: Clone + Send + Sync + 'static,
    {
        let store = Arc::new(store);

        let service = self
            .client
            .service_builder()
            .description("esrc-cqrs query dispatcher")
            .start(&self.service_name, QUERY_SERVICE_VERSION)
            .await
            .map_err(|e| Error::Internal(e.into()))?;

        let group = service.group(&self.service_name);

        // Build one endpoint per handler and spawn a task for each.
        let mut tasks = tokio::task::JoinSet::new();

        for handler in handlers {
            let handler = Arc::clone(handler);
            let store = Arc::clone(&store);

            let mut endpoint = group
                .endpoint(handler.name())
                .await
                .map_err(|e| Error::Internal(e.into()))?;

            tasks.spawn(async move {
                while let Some(request) = endpoint.next().await {
                    let payload = request.message.payload.as_ref();
                    match handler.handle_erased(&*store, payload).await {
                        Ok(reply) => {
                            let _ = request.respond(Ok(reply.into())).await;
                        },
                        Err(e) => {
                            let failure = QueryReply {
                                success: false,
                                data: None,
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

/// Build the full NATS subject for a query endpoint.
///
/// Pattern: `<service_name>.<handler_name>`
pub fn query_subject(service_name: &str, handler_name: &str) -> String {
    format!("{service_name}.{handler_name}")
}
