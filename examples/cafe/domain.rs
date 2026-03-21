use esrc::aggregate::Aggregate;
use esrc::aggregate::Root;
use esrc::version::{DeserializeVersion, SerializeVersion};
use esrc::view::View;
use esrc::Event;
use serde::{Deserialize, Serialize};

/// The status of a cafe order.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum OrderStatus {
    #[default]
    Pending,
    Completed,
}

/// The cafe Order aggregate.
#[derive(Debug, Default)]
pub struct Order {
    pub status: OrderStatus,
    pub item: Option<String>,
    pub quantity: u32,
}

/// Commands that can be applied to the Order aggregate.
#[derive(Debug, Deserialize, Serialize)]
pub enum OrderCommand {
    /// Place a new order for an item.
    PlaceOrder { item: String, quantity: u32 },
    /// Complete an existing order.
    CompleteOrder,
}

/// Events emitted by the Order aggregate.
#[derive(Debug, Clone, Serialize, Deserialize, Event, SerializeVersion, DeserializeVersion)]
pub enum OrderEvent {
    /// An order was placed for an item.
    OrderPlaced { item: String, quantity: u32 },
    /// An order was completed.
    OrderCompleted,
}

/// Errors that can occur when processing Order commands.
#[derive(Debug, Serialize, Deserialize, thiserror::Error)]
pub enum OrderError {
    #[error("order is already completed")]
    AlreadyCompleted,
    #[error("order has not been placed yet")]
    NotPlaced,
}

/// A read-model snapshot of an Order aggregate, returned by queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderState {
    /// The current status of the order.
    pub status: String,
    /// The item that was ordered, if any.
    pub item: Option<String>,
    /// The quantity ordered.
    pub quantity: u32,
}

impl OrderState {
    /// Project an [`Order`] into an [`OrderState`] read-model.
    pub fn from_order(order: &Order) -> Self {
        Self {
            status: format!("{:?}", order.status),
            item: order.item.clone(),
            quantity: order.quantity,
        }
    }
}

impl Aggregate for Order {
    type Command = OrderCommand;
    type Event = OrderEvent;
    type Error = OrderError;

    fn process(&self, command: Self::Command) -> Result<Self::Event, Self::Error> {
        match command {
            OrderCommand::PlaceOrder { item, quantity } => {
                Ok(OrderEvent::OrderPlaced { item, quantity })
            },
            OrderCommand::CompleteOrder => {
                if self.status == OrderStatus::Completed {
                    Err(OrderError::AlreadyCompleted)
                } else if self.item.is_none() {
                    Err(OrderError::NotPlaced)
                } else {
                    Ok(OrderEvent::OrderCompleted)
                }
            },
        }
    }

    fn apply(self, event: &Self::Event) -> Self {
        match event {
            OrderEvent::OrderPlaced { item, quantity } => Order {
                status: OrderStatus::Pending,
                item: Some(item.clone()),
                quantity: *quantity,
            },
            OrderEvent::OrderCompleted => Order {
                status: OrderStatus::Completed,
                ..self
            },
        }
    }
}

impl View for Order {
    type Event = OrderEvent;

    fn apply(self, event: &Self::Event) -> Self {
        match event {
            OrderEvent::OrderPlaced { item, quantity } => Order {
                status: OrderStatus::Pending,
                item: Some(item.clone()),
                quantity: *quantity,
            },
            OrderEvent::OrderCompleted => Order {
                status: OrderStatus::Completed,
                ..self
            },
        }
    }
}
