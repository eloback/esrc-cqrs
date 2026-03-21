use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use esrc::envelope::Envelope;
use esrc::error;
use esrc::nats::NatsStore;
use esrc::project::{Context, Project};
use esrc::view::View;
use serde::Serialize;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::nats::query_dispatcher::{QueryEnvelope, QueryReply};
use crate::query::QueryHandler;

/// An in-memory projection that keeps a [`View`] per aggregate ID.
///
/// `MemoryView<V>` implements [`Project`] so it can be registered as a
/// projector. It is also the shared backing store for [`MemoryViewQuery`].
///
/// Multiple `MemoryViewQuery` instances can share the same `MemoryView` handle
/// because the internal map is wrapped in an `Arc<RwLock<...>>`.
///
/// `V` must implement [`View`] and [`Clone`] so that a snapshot can be taken
/// for the projection function without holding the write lock.
#[derive(Clone)]
pub struct MemoryView<V> {
    views: Arc<RwLock<HashMap<Uuid, V>>>,
}

impl<V> Default for MemoryView<V> {
    fn default() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<V> MemoryView<V> {
    /// Create a new, empty `MemoryView`.
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("memory view projection error")]
struct MemoryViewError;

impl<V> Project for MemoryView<V>
where
    V: View + Clone + Send + Sync + 'static,
    V::Event: esrc::version::DeserializeVersion + Send,
{
    type EventGroup = V::Event;
    type Error = std::convert::Infallible;

    async fn project<'de, E>(
        &mut self,
        context: Context<'de, E, Self::EventGroup>,
    ) -> Result<(), Self::Error>
    where
        E: Envelope + Sync,
    {
        let id = Context::id(&context);
        let event = Context::into_inner(context);

        let mut map = self.views.write().await;
        let view = map.entry(id).or_insert_with(V::default);
        // Apply the event in-place by temporarily swapping the value out.
        let current = std::mem::replace(view, V::default());
        *view = current.apply(&event);

        Ok(())
    }
}

/// A [`QueryHandler`] that reads from a [`MemoryView`] to answer queries.
///
/// On every incoming query, `MemoryViewQuery` looks up the current `V` for the
/// requested aggregate ID in the shared in-memory map, applies the projection
/// function, and returns the serialized result inside a [`QueryReply`].
///
/// If the aggregate ID has never been seen by the projector, `V::default()` is
/// used, which matches the semantics of an aggregate with no events applied.
///
/// `V` is the [`View`] type held in memory.
/// `R` is the read-model type returned to the caller; it must implement
/// [`serde::Serialize`].
pub struct MemoryViewQuery<V, R> {
    /// The unique handler name used to route queries to this handler.
    handler_name: &'static str,
    /// The shared in-memory view store.
    memory_view: MemoryView<V>,
    /// Projects a snapshot of the view into the serializable response type.
    projection: fn(&V) -> R,
    _phantom: PhantomData<R>,
}

impl<V, R> MemoryViewQuery<V, R>
where
    V: View + Clone,
    R: Serialize,
{
    /// Create a new handler with the given routing name, shared memory view, and projection function.
    ///
    /// `handler_name` is used to route incoming query messages to this handler.
    /// `memory_view` is the shared `MemoryView` instance that must also be registered
    /// as a projector so it receives events.
    /// `projection` converts a view snapshot into the serializable response `R`.
    pub fn new(
        handler_name: &'static str,
        memory_view: MemoryView<V>,
        projection: fn(&V) -> R,
    ) -> Self {
        Self {
            handler_name,
            memory_view,
            projection,
            _phantom: PhantomData,
        }
    }
}

impl<V, R> QueryHandler<NatsStore> for MemoryViewQuery<V, R>
where
    V: View + Clone + Send + Sync + 'static,
    V::Event: esrc::version::DeserializeVersion + Send,
    R: Serialize + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.handler_name
    }

    async fn handle<'a>(
        &'a self,
        _store: &'a NatsStore,
        payload: &'a [u8],
    ) -> error::Result<Vec<u8>> {
        let envelope: QueryEnvelope = serde_json::from_slice(payload)
            .map_err(|e| esrc::error::Error::Format(e.into()))?;

        // Take a snapshot under a read lock to avoid holding the lock during serialization.
        let snapshot: V = {
            let map = self.memory_view.views.read().await;
            map.get(&envelope.id).cloned().unwrap_or_default()
        };

        let data = serde_json::to_value((self.projection)(&snapshot))
            .map_err(|e| esrc::error::Error::Format(e.into()))?;

        let reply = QueryReply {
            success: true,
            data: Some(data),
            error: None,
        };
        serde_json::to_vec(&reply).map_err(|e| esrc::error::Error::Format(e.into()))
    }
}
