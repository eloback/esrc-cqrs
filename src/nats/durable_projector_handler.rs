use esrc::error;
use esrc::nats::NatsStore;
use esrc::project::Project;
use tracing::instrument;

use crate::projector::ProjectorHandler;

/// A [`ProjectorHandler`] backed by a NATS JetStream durable consumer.
///
/// Uses `NatsStore::durable_observe` to subscribe to events with a named
/// durable consumer, ensuring the projector resumes from its last position
/// across process restarts.
///
/// `P` is the [`Project`] implementation to drive.
pub struct DurableProjectorHandler<P> {
    /// The durable consumer name; also used as the projector's routing name.
    durable_name: &'static str,
    projector: P,
}

impl<P> DurableProjectorHandler<P>
where
    P: Project + Clone + Send + Sync + 'static,
{
    /// Create a new handler with the given durable name and projector.
    ///
    /// The `durable_name` becomes the NATS durable consumer name and must be
    /// unique across all projectors in the application.
    pub fn new(durable_name: &'static str, projector: P) -> Self {
        Self {
            durable_name,
            projector,
        }
    }
}

impl<P> ProjectorHandler<NatsStore> for DurableProjectorHandler<P>
where
    P: Project + Clone + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.durable_name
    }

    #[instrument(skip_all, level = "debug")]
    async fn run<'a>(&'a self, store: &'a NatsStore) -> error::Result<()> {
        store
            .durable_observe(self.projector.clone(), self.durable_name)
            .await
    }
}
