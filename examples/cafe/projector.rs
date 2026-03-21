use esrc::envelope::Envelope;
use esrc::error;
use esrc::project::{Context, Project};

use crate::domain::OrderEvent;

/// Projects Order events to stdout, tracking order activity.
#[derive(Debug, Default, Clone)]
pub struct OrderProjector;

impl Project for OrderProjector {
    type EventGroup = OrderEvent;
    type Error = std::convert::Infallible;

    async fn project<'de, E>(
        &mut self,
        context: Context<'de, E, Self::EventGroup>,
    ) -> Result<(), Self::Error>
    where
        E: Envelope + Sync,
    {
        let id = Context::id(&context);
        match *context {
            OrderEvent::OrderPlaced { ref item, quantity } => {
                println!("[projector] Order {id} placed: {quantity}x {item}");
            },
            OrderEvent::OrderCompleted => {
                println!("[projector] Order {id} completed");
            },
        }
        Ok(())
    }
}
