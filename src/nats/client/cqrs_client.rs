use async_nats::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{self, Error};

use crate::nats::command::aggregate_command_handler::CommandEnvelope;
use crate::nats::command_dispatcher::{command_subject, CommandReply};
use crate::nats::query_dispatcher::{query_subject, QueryEnvelope, QueryReply};

/// High-level CQRS client that removes boilerplate from command and query dispatch.
///
/// `CqrsClient` wraps an `async_nats::Client` and handles envelope construction,
/// serialization, subject building, and reply deserialization internally. Callers
/// only supply the service name, handler name, aggregate ID, and payload.
///
/// # Command dispatch
///
/// ```rust,ignore
/// let reply = client
///     .send_command("my-service", "MyAggregate", aggregate_id, my_command)
///     .await?;
/// ```
///
/// # Query dispatch
///
/// ```rust,ignore
/// let state: MyState = client
///     .send_query("my-service", "MyAggregate.GetState", aggregate_id)
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct CqrsClient {
    inner: Client,
}

impl CqrsClient {
    /// Create a new `CqrsClient` wrapping the given NATS client.
    pub fn new(client: Client) -> Self {
        Self { inner: client }
    }

    /// Return a reference to the underlying `async_nats::Client`.
    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// Send a command to a handler and return the raw [`CommandReply`].
    ///
    /// The envelope is constructed and serialized internally. The subject is
    /// built from `service_name` and `handler_name` using [`command_subject`].
    ///
    /// # Errors
    ///
    /// Returns an [`esrc::error::Error::Internal`] if the NATS request fails or
    /// the reply cannot be deserialized. A successful return does not imply
    /// `reply.success == true`; the caller should inspect [`CommandReply::success`].
    pub async fn send_command<C>(
        &self,
        service_name: &str,
        handler_name: &str,
        id: Uuid,
        command: C,
    ) -> error::Result<CommandReply>
    where
        C: Serialize,
    {
        let envelope = CommandEnvelope { id, command };
        let payload = serde_json::to_vec(&envelope).map_err(|e| Error::Format(e.to_string()))?;
        let subject = command_subject(service_name, handler_name);

        let msg = self
            .inner
            .request(subject, payload.into())
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        serde_json::from_slice::<CommandReply>(&msg.payload)
            .map_err(|e| Error::Format(e.to_string()))
    }

    /// Send a command and return `Ok(reply.id)` on success, or convert the
    /// [`CommandReply`] error into an [`esrc::error::Error`] on failure.
    ///
    /// This is a convenience wrapper around [`send_command`] for callers that
    /// want to propagate command failures as `Result::Err` rather than
    /// inspecting the reply manually.
    ///
    /// [`send_command`]: CqrsClient::send_command
    pub async fn dispatch_command<C>(
        &self,
        service_name: &str,
        handler_name: &str,
        id: Uuid,
        command: C,
    ) -> error::Result<Uuid>
    where
        C: Serialize,
    {
        let reply = self
            .send_command(service_name, handler_name, id, command)
            .await?;
        if reply.success {
            Ok(reply.id)
        } else {
            let err = reply.error.unwrap_or(Error::Internal(
                "command failed but no error present".into(),
            ));
            return Err(err);
        }
    }

    /// Send a query to a handler and return the raw [`QueryReply`].
    ///
    /// The envelope is constructed and serialized internally. The subject is
    /// built from `service_name` and `handler_name` using [`query_subject`].
    ///
    /// # Errors
    ///
    /// Returns an [`esrc::error::Error::Internal`] if the NATS request fails or
    /// the reply cannot be deserialized. A successful return does not imply
    /// `reply.success == true`; the caller should inspect [`QueryReply::success`].
    pub async fn send_query(
        &self,
        service_name: &str,
        handler_name: &str,
        id: Uuid,
    ) -> error::Result<QueryReply>
where {
        let envelope = QueryEnvelope { id };
        let payload = serde_json::to_vec(&envelope).map_err(|e| Error::Format(e.to_string()))?;
        let subject = query_subject(service_name, handler_name);

        let msg = self
            .inner
            .request(subject, payload.into())
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        serde_json::from_slice::<QueryReply>(&msg.payload).map_err(|e| Error::Format(e.to_string()))
    }

    /// Send a query and deserialize the result directly into `T`.
    ///
    /// This is a convenience wrapper around [`send_query`] for callers that
    /// want a typed result rather than a raw [`QueryReply`]. Returns
    /// `Err(Error::Internal(...))` when `reply.success` is false.
    ///
    /// # Errors
    ///
    /// Returns an error if the NATS request fails, the reply cannot be
    /// deserialized, or the `data` field is absent even though `success` is true.
    ///
    /// [`send_query`]: CqrsClient::send_query
    pub async fn dispatch_query<T>(
        &self,
        service_name: &str,
        handler_name: &str,
        id: Uuid,
    ) -> error::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let reply = self.send_query(service_name, handler_name, id).await?;
        if !reply.success {
            let err = reply
                .error
                .unwrap_or(Error::Internal("query failed but no error present".into()));
            return Err(err);
        }
        let data = reply
            .data
            .ok_or_else(|| Error::Internal("query succeeded but returned no data".into()))?;
        serde_json::from_value::<T>(data).map_err(|e| Error::Format(e.to_string()))
    }
}

impl From<Client> for CqrsClient {
    fn from(client: Client) -> Self {
        Self::new(client)
    }
}
