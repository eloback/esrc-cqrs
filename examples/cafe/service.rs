use esrc::error::{self, Error};
use esrc::event::publish::PublishExt;
use esrc::event::replay::ReplayOneExt;
use esrc::nats::NatsStore;
use esrc_cqrs::error::from_esrc_error;
use esrc_cqrs::nats::command::service_command_handler::ServiceCommandReply;
use esrc_cqrs::nats::command::EsrcCommandHandler;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{Order, OrderCommand};

/// A unified command enum for the cafe service handler.
///
/// This enum is dispatched through a single NATS endpoint and routed
/// internally to the appropriate domain logic.
#[derive(Debug, Serialize, Deserialize)]
pub enum CafeCommands {
    /// Place a new order for an item.
    PlaceOrder {
        id: Uuid,
        item: String,
        quantity: u32,
    },
    /// Complete an existing order by ID.
    CompleteOrder { id: Uuid },
}

/// Service command handler for all cafe operations.
///
/// Implements `EsrcCommandHandler` so it can be registered via
/// `ServiceCommandHandler::new(CafeServiceHandler)` and receive all
/// `CafeCommands` variants under a single NATS endpoint.
#[derive(Clone)]
pub struct CafeServiceHandler;

impl EsrcCommandHandler<NatsStore, CafeCommands> for CafeServiceHandler {
    async fn handle<'a>(
        &'a self,
        store: &'a mut NatsStore,
        command: CafeCommands,
    ) -> error::Result<Vec<u8>> {
        let reply: ServiceCommandReply<Uuid> = match command {
            CafeCommands::PlaceOrder { id, item, quantity } => {
                let root = store.read::<Order>(id).await?;
                let result = store
                    .try_write(root, OrderCommand::PlaceOrder { item, quantity }, None)
                    .await;
                match result {
                    Ok(written) => {
                        ServiceCommandReply::ok_with(esrc::aggregate::Root::id(&written))
                    },
                    Err(e) => ServiceCommandReply::err(from_esrc_error(e)),
                }
            },
            CafeCommands::CompleteOrder { id } => {
                let root = store.read::<Order>(id).await?;
                let result = store
                    .try_write(root, OrderCommand::CompleteOrder, None)
                    .await;
                match result {
                    Ok(written) => {
                        ServiceCommandReply::ok_with(esrc::aggregate::Root::id(&written))
                    },
                    Err(e) => ServiceCommandReply::err(from_esrc_error(e)),
                }
            },
        };

        serde_json::to_vec(&reply).map_err(|e| Error::Format(e.into()))
    }
}
