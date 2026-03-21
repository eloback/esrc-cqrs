use std::marker::PhantomData;

use esrc::Envelope;
use esrc::error::{self, Error};
use esrc::event::replay::ReplayOne;
use esrc::nats::NatsStore;
use esrc::view::View;
use futures::StreamExt;
use serde::Serialize;
use uuid::Uuid;

use crate::nats::query_dispatcher::{QueryEnvelope, QueryReply};
use crate::query::QueryHandler;

/// A [`QueryHandler`] that replays an event stream on each request to build a
/// [`View`] and return a projected read model as the query response.
///
/// On every incoming query, `LiveViewQuery` replays the full event history for
/// the requested aggregate ID, folds all events into a fresh `V` instance
/// starting from `V::default()`, applies the projection function, and returns
/// the serialized result inside a [`QueryReply`].
///
/// This is suitable for views where replaying on demand is acceptable (e.g.,
/// small streams or low-throughput queries). For higher-throughput scenarios,
/// prefer [`MemoryViewQuery`] which keeps an in-memory projection updated by a
/// running projector.
///
/// `V` is the [`View`] type to build from replayed events.
/// `R` is the read-model type returned to the caller; it must implement
/// [`serde::Serialize`].
pub struct LiveViewQuery<V, R> {
    /// The unique handler name used to route queries to this handler.
    handler_name: &'static str,
    /// Projects a built view into the serializable response type.
    projection: fn(&V) -> R,
    _phantom: PhantomData<(V, R)>,
}

impl<V, R> LiveViewQuery<V, R>
where
    V: View,
    R: Serialize,
{
    /// Create a new handler with the given routing name and projection function.
    ///
    /// `handler_name` is used to route incoming query messages to this handler.
    /// `projection` converts the built `V` into the serializable response `R`.
    pub fn new(handler_name: &'static str, projection: fn(&V) -> R) -> Self {
        Self {
            handler_name,
            projection,
            _phantom: PhantomData,
        }
    }
}

impl<V, R> QueryHandler<NatsStore> for LiveViewQuery<V, R>
where
    V: View + Send + Sync + 'static,
    V::Event: esrc::version::DeserializeVersion + Send,
    R: Serialize + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.handler_name
    }

    async fn handle<'a>(
        &'a self,
        store: &'a NatsStore,
        payload: &'a [u8],
    ) -> error::Result<Vec<u8>> {
        let envelope: QueryEnvelope = serde_json::from_slice(payload)
            .map_err(|e| esrc::error::Error::Format(e.into()))?;

        // Replay the full event history for the requested aggregate ID, starting from sequence 0.
        let mut stream = store
            .replay_one::<V::Event>(envelope.id, esrc::event::Sequence::new())
            .await?;

        let mut view = V::default();
        while let Some(result) = stream.next().await {
            let nats_envelope = result?;
            let event = nats_envelope
                .deserialize::<V::Event>()
                .map_err(|e| Error::Format(format!("{e}").into()))?;
            view = view.apply(&event);
        }

        let data = serde_json::to_value((self.projection)(&view))
            .map_err(|e| esrc::error::Error::Format(e.into()))?;

        let reply = QueryReply {
            success: true,
            data: Some(data),
            error: None,
        };
        serde_json::to_vec(&reply).map_err(|e| esrc::error::Error::Format(e.into()))
    }
}
