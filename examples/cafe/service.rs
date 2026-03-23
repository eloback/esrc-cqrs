use esrc::error::{self, Error};
use esrc::event::publish::PublishExt;
use esrc::event::replay::ReplayOneExt;
use esrc::nats::NatsStore;
use esrc_cqrs::command::NatsServiceCommandHandler;
use esrc_cqrs::nats::command::service_command_handler::ServiceCommandReply;
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
    PlaceOrder { id: Uuid, item: String, quantity: u32 },
    /// Complete an existing order by ID.
    CompleteOrder { id: Uuid },
}

/// Service command handler for all cafe operations.
///
/// Implements `NatsServiceCommandHandler` so it can be registered via
/// `ServiceCommandHandler::new(CafeServiceHandler)` and receive all
/// `CafeCommands` variants under a single NATS endpoint.
#[derive(Clone)]
pub struct CafeServiceHandler;

impl NatsServiceCommandHandler<NatsStore, CafeCommands> for CafeServiceHandler {
    fn name(&self) -> &'static str {
        "CafeService"
    }

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
                    Ok(written) => ServiceCommandReply::ok_with(esrc::aggregate::Root::id(&written)),
                    Err(e) => ServiceCommandReply::err(esrc_error_to_cqrs(e)),
                }
            },
            CafeCommands::CompleteOrder { id } => {
                let root = store.read::<Order>(id).await?;
                let result = store
                    .try_write(root, OrderCommand::CompleteOrder, None)
                    .await;
                match result {
                    Ok(written) => ServiceCommandReply::ok_with(esrc::aggregate::Root::id(&written)),
                    Err(e) => ServiceCommandReply::err(esrc_error_to_cqrs(e)),
                }
            },
        };

        serde_json::to_vec(&reply).map_err(|e| Error::Format(e.into()))
    }
}

/// Convert an [`esrc::error::Error`] into a [`esrc_cqrs::Error`] for transport.
fn esrc_error_to_cqrs(err: esrc::error::Error) -> esrc_cqrs::Error {
    match err {
        esrc::error::Error::Internal(e) => esrc_cqrs::Error::Internal(e.to_string()),
        esrc::error::Error::External(e) => {
            esrc_cqrs::Error::External(serde_json::Value::String(e.to_string()))
        },
        esrc::error::Error::Format(e) => esrc_cqrs::Error::Format(e.to_string()),
        esrc::error::Error::Invalid => esrc_cqrs::Error::Invalid,
        esrc::error::Error::Conflict => esrc_cqrs::Error::Conflict,
    }
}
