use tracing::instrument;

use esrc::error;
use esrc::nats::NatsStore;

use crate::projector::ProjectorHandler;

/// NATS projector runner.
///
/// Wraps a [`ProjectorHandler`] and drives it against a [`NatsStore`].
/// The runner simply delegates to the handler's `run` method, which internally
/// uses the durable JetStream consumer subscribe path already present in the
/// `NatsStore` via `durable_observe`.
///
/// Each runner should be started in its own Tokio task so that projectors
/// run concurrently and independently.
pub struct NatsProjectorRunner<H> {
    handler: H,
}

impl<H> NatsProjectorRunner<H>
where
    H: ProjectorHandler<NatsStore> + Send + 'static,
{
    /// Create a new runner wrapping the given handler.
    pub fn new(handler: H) -> Self {
        Self { handler }
    }

    /// Run the projector against the given store.
    ///
    /// This drives the handler until it returns an error. The caller is
    /// responsible for respawning or handling the error as appropriate.
    #[instrument(skip_all, level = "debug")]
    pub async fn run(&self, store: &NatsStore) -> error::Result<()> {
        self.handler.run(store).await
    }
}
